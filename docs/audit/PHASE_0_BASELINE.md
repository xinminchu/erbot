# Phase 0 Baseline

- Repository path: R:/Dev/ERBOT
- Date/time: 2026-07-16
- Git branch: main
- Git commit: 3acf0e05108d79fde165adda437d5f68882a5db0
- Git status: clean except for the untracked Dissertation_Thesis2024UMB/ directory
- Top-level inventory: R/, tests/, data/, paper/, inst/, man/, results/, Literature/, Dissertation_Thesis2024UMB/
- Inventory scope: docs/audit/REPOSITORY_INVENTORY.csv is a Phase 0 structural inventory (top-level directories plus items directly affected by the reorganization), not a complete file-level scientific inventory.
- Major safety findings: repository root is valid; no obvious secrets were found in the inspected files; legacy absolute-path usage exists in R/run_eval_report.R and R/01-progress.R-style headers; the repository contains large data and literature directories that are ignored by Git and should be treated as external assets.
- Unresolved blockers: none for the audit phase; the untracked dissertation directory is retained unmodified for now.
