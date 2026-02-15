# Subagent Workflow for Documentation Revisions

This file defines the intended multi-agent workflow for future documentation rewrites. It is model-agnostic but assumes availability of high-capability prose agents (for example GPT-5.2 xhigh) and lower-cost verification agents.

## Roles

`truth-miner`
- Extract implementation facts from `rust/`, `sql/`, `config/`, and `bin/`.
- Emit claim candidates with source references.
- Never invent architecture rationale.

`outline-architect` (recommended: GPT-5.2 xhigh)
- Build decision-complete outlines from claim sets.
- Ensure each doc has explicit sections for invariants, failure model, and performance behavior.

`draft-writer` (recommended: GPT-5.2 xhigh)
- Write prose-first technical narrative from approved outline.
- Include source references inline using `[src: path:line]`.

`factual-auditor`
- Validate claims against source references.
- Flag unsupported or contradictory claims.

`adversarial-reviewer` (recommended: GPT-5.2 xhigh)
- Attack assumptions and identify missing failure paths.
- Require concrete revisions, not generic suggestions.

`style-density-checker`
- Enforce bullet ratio, word density, required section coverage, and terminology policy.
- Reject list-heavy pages with low explanatory depth.

## Execution Contract

1. Truth extraction first.
2. Outline approval second.
3. Drafting third.
4. Factual and adversarial checks before merge.
5. `make docs-qc` must pass.

No draft is accepted if any hard gate fails.
