# Path Audit

Findings recorded at audit time (file and line references are to the pre-reorganization state). Status: all items below were corrected in Phase 0 — the setwd() call was removed when R/run_eval_report.R moved to scripts/experiments/run_eval_report.R, and the four legacy header comments were removed. tests/testthat/test-repo-audit.R now guards against regressions.

| file | line | current value | risk | proposed relative-path replacement | automatic correction safe |
| --- | --- | --- | --- | --- | --- |
| R/run_eval_report.R | 19 | setwd("D:/ResearchHub/01_projects/erbot") | High | Use the project root from the current file or package root via a helper instead of setwd() | No |
| R/01-progress.R | 2 | # File: D:/erbot/R/01-progress.R | Low | Remove the legacy header comment or replace with a relative path note | Yes |
| R/04-features.R | 2 | # File: D:/erbot/R/04-features.R | Low | Remove the legacy header comment or replace with a relative path note | Yes |
| R/30-reporting.R | 2 | # File: D:/erbot/R/30-reporting.R | Low | Remove the legacy header comment or replace with a relative path note | Yes |
| R/50-tuning.R | 3 | # File: D:/erbot/R/50-tuning.R | Low | Remove the legacy header comment or replace with a relative path note | Yes |
