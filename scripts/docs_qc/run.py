#!/usr/bin/env python3
"""Hard-fail quality checks for Cortex docs."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

CITATION_RE = re.compile(r"\[src:\s*([^\]]+)\]")
HEADING_RE = re.compile(r"^(#{2,6})\s+(.*)$")
BULLET_RE = re.compile(r"^\s*(?:[-*+]\s+|\d+\.\s+)")
WORD_RE = re.compile(r"[A-Za-z0-9_'-]+")
REF_RE = re.compile(r"^([^:\s]+):(?:L)?(\d+)(?:-(?:L)?(\d+))?$", re.IGNORECASE)


@dataclass
class Section:
    title: str
    start: int
    end: int


@dataclass
class CheckResult:
    name: str
    status: str
    message: str


class DocAnalyzer:
    def __init__(self, repo_root: Path, docs_root: Path, policy: dict):
        self.repo_root = repo_root
        self.docs_root = docs_root
        self.policy = policy
        self._line_cache: Dict[Path, int] = {}

    def analyze_doc(self, doc_path: Path, core_cfg: dict | None) -> dict:
        rel = doc_path.relative_to(self.docs_root)
        text = doc_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        checks: List[CheckResult] = []

        in_code = False
        content_lines = 0
        bullet_lines = 0
        word_count = 0
        citations: List[Tuple[str, int]] = []
        citation_errors: List[str] = []
        headings: List[Section] = []

        # First pass: metrics + citations + heading boundaries.
        for idx, line in enumerate(lines, start=1):
            stripped = line.strip()

            if stripped.startswith("```"):
                in_code = not in_code
                continue

            if in_code:
                continue

            m_heading = HEADING_RE.match(line)
            if m_heading:
                headings.append(Section(m_heading.group(2).strip(), idx, len(lines)))
                continue

            if not stripped:
                continue

            content_lines += 1
            if BULLET_RE.match(line):
                bullet_lines += 1
            word_count += len(WORD_RE.findall(line))

            for match in CITATION_RE.finditer(line):
                raw = match.group(1)
                refs = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
                for ref in refs:
                    rm = REF_RE.match(ref)
                    if not rm:
                        citation_errors.append(f"invalid citation format '{ref}' on {rel}:{idx}")
                        continue
                    path = rm.group(1)
                    start = int(rm.group(2))
                    end = int(rm.group(3) or start)
                    if end < start:
                        start, end = end, start
                    abs_path = (self.repo_root / path).resolve()
                    if not abs_path.exists():
                        citation_errors.append(f"missing citation file '{path}' on {rel}:{idx}")
                        continue
                    max_line = self._line_count(abs_path)
                    if start < 1 or end > max_line:
                        citation_errors.append(
                            f"invalid citation range {start}-{end} for '{path}' (max {max_line}) on {rel}:{idx}"
                        )
                        continue
                    citations.extend((path, line_no) for line_no in range(start, end + 1))

        # Complete section ranges.
        for i in range(len(headings)):
            if i < len(headings) - 1:
                headings[i].end = headings[i + 1].start - 1
            else:
                headings[i].end = len(lines)

        citation_count = len(citations)
        bullet_ratio = (bullet_lines / content_lines) if content_lines else 0.0

        # Common checks.
        if citation_errors:
            checks.append(CheckResult("citation_format", "fail", "; ".join(citation_errors[:12])))
        else:
            checks.append(CheckResult("citation_format", "pass", "all citations resolve to path:Lstart[-end]"))

        if citation_count == 0:
            checks.append(CheckResult("citation_presence", "fail", "no [src: ...] citations found"))
        else:
            checks.append(CheckResult("citation_presence", "pass", f"found {citation_count} citations"))

        max_bullet_ratio = (
            core_cfg["max_bullet_ratio"] if core_cfg else self.policy["global"]["max_bullet_ratio_non_core"]
        )
        if bullet_ratio <= max_bullet_ratio:
            checks.append(
                CheckResult(
                    "bullet_density",
                    "pass",
                    f"bullet ratio {bullet_ratio:.3f} <= {max_bullet_ratio:.3f}",
                )
            )
        else:
            checks.append(
                CheckResult(
                    "bullet_density",
                    "fail",
                    f"bullet ratio {bullet_ratio:.3f} exceeds {max_bullet_ratio:.3f}",
                )
            )

        # Core-doc checks.
        if core_cfg:
            min_words = int(core_cfg["min_words"])
            max_words = int(core_cfg["max_words"])
            if min_words <= word_count <= max_words:
                checks.append(
                    CheckResult(
                        "word_count",
                        "pass",
                        f"word count {word_count} within [{min_words}, {max_words}]",
                    )
                )
            else:
                checks.append(
                    CheckResult(
                        "word_count",
                        "fail",
                        f"word count {word_count} outside [{min_words}, {max_words}]",
                    )
                )

            required_sections = list(core_cfg.get("required_sections", []))
            heading_titles = {h.title for h in headings}
            missing = [section for section in required_sections if section not in heading_titles]
            if missing:
                checks.append(
                    CheckResult(
                        "required_sections",
                        "fail",
                        "missing sections: " + ", ".join(missing),
                    )
                )
            else:
                checks.append(CheckResult("required_sections", "pass", "all required sections present"))

            citation_target = math.ceil(word_count / 250) * int(self.policy["global"]["min_citations_per_250_words"])
            if citation_count >= citation_target:
                checks.append(
                    CheckResult(
                        "citation_density",
                        "pass",
                        f"citations {citation_count} >= required {citation_target}",
                    )
                )
            else:
                checks.append(
                    CheckResult(
                        "citation_density",
                        "fail",
                        f"citations {citation_count} < required {citation_target}",
                    )
                )

            # Per-section prose and citation coverage.
            min_prose_words = int(self.policy["global"]["min_prose_words_per_section"])
            max_bullets_wo_prose = int(self.policy["global"]["max_bullets_without_prose"])
            section_failures: List[str] = []
            for section_name in required_sections:
                section = next((s for s in headings if s.title == section_name), None)
                if not section:
                    continue

                sec_in_code = False
                sec_words = 0
                sec_bullets = 0
                sec_has_citation = False

                for lineno in range(section.start + 1, section.end + 1):
                    line = lines[lineno - 1]
                    stripped = line.strip()
                    if stripped.startswith("```"):
                        sec_in_code = not sec_in_code
                        continue
                    if sec_in_code:
                        continue
                    if not stripped:
                        continue
                    if HEADING_RE.match(line):
                        continue
                    if CITATION_RE.search(line):
                        sec_has_citation = True
                    if BULLET_RE.match(line):
                        sec_bullets += 1
                        continue
                    sec_words += len(WORD_RE.findall(line))

                if sec_words < min_prose_words:
                    section_failures.append(f"section '{section_name}' has only {sec_words} prose words")
                if sec_bullets > max_bullets_wo_prose and sec_words < (min_prose_words + 20):
                    section_failures.append(
                        f"section '{section_name}' is list-heavy ({sec_bullets} bullets, {sec_words} prose words)"
                    )
                if not sec_has_citation:
                    section_failures.append(f"section '{section_name}' has no citation")

            if section_failures:
                checks.append(CheckResult("section_quality", "fail", "; ".join(section_failures[:10])))
            else:
                checks.append(CheckResult("section_quality", "pass", "all required sections pass prose/citation checks"))

            # Terminology checks.
            required_terms = self.policy["terminology"]["required_terms"].get(str(rel), [])
            text_lower = text.lower()
            missing_terms = [term for term in required_terms if term.lower() not in text_lower]
            if missing_terms:
                checks.append(
                    CheckResult(
                        "terminology_required",
                        "fail",
                        "missing required terms: " + ", ".join(missing_terms),
                    )
                )
            else:
                checks.append(CheckResult("terminology_required", "pass", "required terms present"))

        # Banned-phrase checks for all docs.
        banned_hits = []
        lowered = text.lower()
        for phrase in self.policy["terminology"]["banned_phrases"]:
            if phrase.lower() in lowered:
                banned_hits.append(phrase)
        if banned_hits:
            checks.append(
                CheckResult("banned_phrases", "fail", "contains banned phrases: " + ", ".join(banned_hits))
            )
        else:
            checks.append(CheckResult("banned_phrases", "pass", "no banned phrases found"))

        status = "pass" if all(c.status == "pass" for c in checks) else "fail"

        return {
            "path": f"docs/{rel}",
            "status": status,
            "metrics": {
                "word_count": word_count,
                "bullet_ratio": round(bullet_ratio, 6),
                "citation_count": citation_count,
            },
            "checks": [c.__dict__ for c in checks],
        }

    def _line_count(self, path: Path) -> int:
        cached = self._line_cache.get(path)
        if cached is not None:
            return cached
        count = len(path.read_text(encoding="utf-8", errors="ignore").splitlines())
        self._line_cache[path] = count
        return count


def validate_claim_ledger(repo_root: Path, docs_root: Path) -> List[CheckResult]:
    checks: List[CheckResult] = []
    ledger_path = docs_root / ".quality" / "claim-ledger.json"
    if not ledger_path.exists():
        checks.append(CheckResult("claim_ledger_exists", "fail", "docs/.quality/claim-ledger.json is missing"))
        return checks

    try:
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    except Exception as exc:  # pylint: disable=broad-except
        checks.append(CheckResult("claim_ledger_parse", "fail", f"cannot parse claim ledger: {exc}"))
        return checks

    claims = ledger.get("claims") if isinstance(ledger, dict) else None
    if not isinstance(claims, list) or not claims:
        checks.append(CheckResult("claim_ledger_nonempty", "fail", "claim ledger has no claims"))
        return checks

    bad_entries = []
    for idx, claim in enumerate(claims, start=1):
        if not isinstance(claim, dict):
            bad_entries.append(f"claim {idx} is not an object")
            continue
        cid = claim.get("claim_id")
        doc = claim.get("doc")
        evidence = claim.get("evidence")
        if not cid or not doc:
            bad_entries.append(f"claim {idx} missing claim_id/doc")
            continue
        if not isinstance(evidence, list) or not evidence:
            bad_entries.append(f"claim {cid} has no evidence")
            continue
        for ev in evidence:
            if not isinstance(ev, dict):
                bad_entries.append(f"claim {cid} has malformed evidence entry")
                continue
            path = ev.get("path")
            line = ev.get("line")
            if not path or not isinstance(line, int) or line < 1:
                bad_entries.append(f"claim {cid} has invalid evidence path/line")
                continue
            abs_path = (repo_root / path).resolve()
            if not abs_path.exists():
                bad_entries.append(f"claim {cid} references missing file {path}")

    if bad_entries:
        checks.append(CheckResult("claim_ledger_valid", "fail", "; ".join(bad_entries[:12])))
    else:
        checks.append(CheckResult("claim_ledger_valid", "pass", f"validated {len(claims)} claims"))

    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Cortex documentation quality gates")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--docs-root", default="docs", help="Docs root")
    parser.add_argument(
        "--policy",
        default="docs/.quality/policy.json",
        help="Policy JSON path",
    )
    parser.add_argument(
        "--report",
        default="docs/.quality/review-report.json",
        help="Output report JSON path",
    )
    parser.add_argument(
        "--no-fail",
        action="store_true",
        help="Always exit 0 (still writes report)",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    docs_root = (repo_root / args.docs_root).resolve()
    policy_path = (repo_root / args.policy).resolve()

    if not docs_root.exists():
        print(f"docs root not found: {docs_root}", file=sys.stderr)
        return 2
    if not policy_path.exists():
        print(f"policy file not found: {policy_path}", file=sys.stderr)
        return 2

    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    analyzer = DocAnalyzer(repo_root=repo_root, docs_root=docs_root, policy=policy)

    core_docs_cfg = policy["core_docs"]
    core_doc_names = set(core_docs_cfg.keys())

    md_docs = sorted(
        p
        for p in docs_root.rglob("*.md")
        if p.name != "index.md"
        and "_source" not in p.parts
        and ".quality" not in p.parts
    )

    doc_results = []
    for doc_path in md_docs:
        rel_key = doc_path.relative_to(docs_root).as_posix()
        core_cfg = core_docs_cfg.get(rel_key)
        doc_results.append(analyzer.analyze_doc(doc_path, core_cfg))

    global_checks = validate_claim_ledger(repo_root, docs_root)

    failed_checks = 0
    total_checks = 0
    for result in doc_results:
        for check in result["checks"]:
            total_checks += 1
            if check["status"] == "fail":
                failed_checks += 1

    global_doc_result = {
        "path": "docs/.quality/claim-ledger.json",
        "status": "pass" if all(c.status == "pass" for c in global_checks) else "fail",
        "metrics": {"word_count": 0, "bullet_ratio": 0.0, "citation_count": 0},
        "checks": [c.__dict__ for c in global_checks],
    }
    for check in global_doc_result["checks"]:
        total_checks += 1
        if check["status"] == "fail":
            failed_checks += 1

    all_results = doc_results + [global_doc_result]
    status = "pass" if failed_checks == 0 else "fail"

    report = {
        "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "status": status,
        "totals": {
            "documents": len(all_results),
            "checks": total_checks,
            "failed_checks": failed_checks,
        },
        "documents": all_results,
    }

    report_path = (repo_root / args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(f"docs-qc status: {status}")
    print(f"documents checked: {len(all_results)}")
    print(f"checks: {total_checks}, failed: {failed_checks}")
    print(f"report: {report_path}")

    if failed_checks:
        for doc in all_results:
            failing = [c for c in doc["checks"] if c["status"] == "fail"]
            if not failing:
                continue
            print(f"\n{doc['path']}")
            for item in failing:
                print(f"  - {item['name']}: {item['message']}")

    if args.no_fail:
        return 0
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
