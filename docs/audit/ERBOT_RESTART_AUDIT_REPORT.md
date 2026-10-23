# ERBOT Restart Audit Report

## 1. Executive summary
- The repository was audited for safety, path portability, structure, and governance. The audit did not run large experiments or rebuild the paper.
- The main portability risk was an absolute-path-based evaluation runner using setwd().
- The repository structure was reorganized into a more explicit package-centric layout with audit and research governance documents.

## 2. Repository state before restructuring
- The repository root was R:/Dev/ERBOT.
- The repository was on branch main at commit 3acf0e05108d79fde165adda437d5f68882a5db0.
- The main code lived in R/ and tests/.
- Historical paper material and a thesis directory existed outside the canonical package structure.
- docs/audit/REPOSITORY_INVENTORY.csv records this state as a Phase 0 structural inventory (top-level directories plus items directly affected by the reorganization); it is not a complete file-level scientific inventory of the repository.

## 3. Repository state after restructuring
- Canonical source remains in R/.
- Governance and audit documents are now in docs/.
- Experiment entry points are represented under scripts/experiments/.
- Historical material is quarantined in archive/legacy_untrusted/.

## 4. Files moved
- R/run_eval_report.R -> scripts/experiments/run_eval_report.R
- Dissertation_Thesis2024UMB/ -> archive/legacy_untrusted/Dissertation_Thesis2024UMB/

## 5. Files renamed
- None in this phase.

## 6. Files quarantined
- archive/legacy_untrusted/Dissertation_Thesis2024UMB/

## 7. Files intentionally not changed
- R/ package source files and tests/ remained in place.
- Root package metadata files remained in place.

## 8. Legacy results discovered
- The paper/ directory contains prior LaTeX, PDFs, and markdown result artifacts that were not revalidated.
- results/ERreports/ contains historical personal reference material only. It is classified as historical_reference_excluded: it is kept local and untracked, is not inspected or cited as research evidence, and its exclusion from version control is intentional and is not a reproducibility defect.

## 9. Absolute paths found and corrected
- A hard-coded setwd() to D:/ResearchHub/01_projects/erbot was removed from the evaluation runner and replaced with a project-root-based approach.
- Legacy header comments referencing D:/erbot were removed from R/01-progress.R, R/04-features.R, R/30-reporting.R, and R/50-tuning.R in this phase.

## 10. Remaining path risks
- No retired absolute paths (D:/, OneDrive, ResearchHub) remain in canonical source under R/, scripts/, or tests/; this is enforced by tests/testthat/test-repo-audit.R.

## 11. Dependency findings
- The package relies on multiple R packages declared in DESCRIPTION and also has optional dependencies for Python and deep-learning baselines.

## 12. Code-quality findings
- The repository has a valid package shape, but some scripts are experiment-oriented and should be isolated from canonical package logic.

## 13. Data provenance findings
- Data provenance and licensing were not fully established from repository evidence, so the dataset catalog marks them as UNKNOWN.

## 14. Git status and commit list
- Git status after the audit reorganization should be reviewed with git status.
- No commits were created during this phase because the repository changes were intentionally scoped to documentation, migration, and a single path fix.

## 15. Lightweight validation results
- Added a lightweight repository-integrity test.
- Parsed and path-checked relevant source files.

## 16. Remaining blockers
- The repository still contains historical paper materials and large data assets that need manual review before they can be fully treated as canonical assets.

## 17. Fixed framework decisions
- The three binary dimensions of the multiview 2x2x2 framework are fixed by user decision: (1) clustering approach: pairwise-threshold clustering vs graph/community-detection clustering; (2) fusion order: early fusion (fusion-first) vs late fusion (clustering-first); (3) ER regime: deduplication vs record linkage.
- The four method cells over clustering approach x fusion order are PE, PL, GE, and GL; each is evaluated under both ER regimes.

## 18. Recommended next task
- Define the first canonical multiview experiment configuration and wire one small end-to-end smoke test around it.

## 19. Exact commands needed to reproduce the lightweight validation
- Rscript -e "testthat::test_file('tests/testthat/test-repo-audit.R')"
- Rscript -e "invisible(lapply(c('.Rprofile','R/01-progress.R','R/04-features.R','R/30-reporting.R','R/50-tuning.R','scripts/experiments/run_eval_report.R','tests/testthat/test-repo-audit.R'), parse))"
- git diff --cached --check
