# Docs QC Runner

`run.py` enforces hard documentation quality gates for Cortex.

## What it checks

- word-count bounds for core architecture docs
- required section presence
- citation validity (`[src: path:line]`) and citation density
- section-level prose depth and anti-list-heavy checks
- bullet density limits
- required terminology and banned-phrase checks
- claim-ledger existence and basic structural validation

## Usage

```bash
cd ~/src/cortex
python3 scripts/docs_qc/run.py --repo-root .
```

Write report without failing process:

```bash
cd ~/src/cortex
python3 scripts/docs_qc/run.py --repo-root . --no-fail
```
