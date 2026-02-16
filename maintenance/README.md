# Maintenance Automation

`run_multiagent_repo_audit.sh` orchestrates a multi-agent repository maintenance flow:

1. Shard source files and run parallel `gpt-5.3-codex-spark` audits.
2. Run one `gpt-5.3-codex` pass to dedupe/compile findings.
3. Run one `gpt-5.3-codex-spark` session per finding to open GitHub issues.

## Quick Start

```bash
maintenance/run_multiagent_repo_audit.sh --yes
```

## Safe Dry Run

```bash
maintenance/run_multiagent_repo_audit.sh --dry-run
```

## Common Options

- `--shards N`: number of shard review agents.
- `--review-parallel N`: concurrent shard reviewers.
- `--issue-parallel N`: concurrent issue creator agents.
- `--run-dir PATH`: artifact/log directory (default in `/tmp`).
- `--max-files N`: cap reviewed files (useful for smoke tests).
- `--sandbox-mode MODE`: `bypass` (default) or Codex sandbox mode.
- `--review-model`, `--dedupe-model`, `--issue-model`: override model names.

## Outputs

- `maintenance/REPORT.md`: deduplicated findings.
- `maintenance/ISSUES_CREATED.md`: one line per issue-creation result.
- run artifacts in `/tmp/cortex-maintenance-<timestamp>` (or `--run-dir`).

## Issue Worker Orchestrator

`run_issue_worker.sh` runs one issue-focused automation cycle:

1. Accept a comma-separated label union (`--tag-union`).
2. Select the highest priority matching open issue (`P0 > P1 > P2 > unlabeled`, then lowest issue number).
3. Mark it in progress with `status/in-progress`.
4. Create a fresh worktree/branch for that issue.
5. Launch `codex exec` with `gpt-5.3-codex` + `xhigh` effort to fix, test, commit, push, and open a PR.

Quick start:

```bash
maintenance/run_issue_worker.sh --tag-union "area/config,area/security"
```

Dry run (select only, no claim or Codex launch):

```bash
maintenance/run_issue_worker.sh --tag-union "area/config,area/security" --dry-run
```

Main outputs:

- No run artifacts are retained.
- The script uses a temporary run directory under `/tmp` and removes it on exit.
- Durable state is kept in GitHub issue/PR updates only.
