# Reorganization Plan

## 1. Current-state summary
- The repository is a package-like project with core R source under R/, tests under tests/, and a large paper and literature tree.
- The current repository has a single untracked thesis directory and several legacy experiment-style scripts that should be isolated from canonical package source.
- The evaluation runner still uses an absolute path and a setwd() call, which creates a portability risk.

## 2. Target-state architecture
- Keep canonical package logic in R/.
- Place experiment entry points under scripts/experiments/.
- Keep governance and audit documents under docs/.
- Keep paper sources under paper/ and treat generated artifacts as results/ rather than source.
- Treat the dissertation directory and similar legacy material as archive/legacy_untrusted/.

## 3. Exact proposed file moves
- Move R/run_eval_report.R to scripts/experiments/run_eval_report.R.
- Move the untracked Dissertation_Thesis2024UMB/ directory to archive/legacy_untrusted/Dissertation_Thesis2024UMB/.

## 4. Exact proposed renames
- None required in this phase.

## 5. Files to remain unchanged
- R/ package sources and tests/ remain in place.
- Existing package metadata files remain in the root.

## 6. Files to quarantine
- Dissertation_Thesis2024UMB/ is quarantined as historical material.

## 7. Files requiring manual review
- Large data directories and literature directories should be reviewed before they are treated as canonical data.
- Any future experiment scripts that are not clearly entry points should be reviewed before moving.

## 8. Path corrections
- Replace the hard-coded setwd() in the evaluation runner with a project-root helper based on the repository root.
- Remove legacy absolute-path comments from R/*.R files where safe.

## 9. Dependency corrections
- No dependency changes are made in this phase; the audit captures package gaps and environment assumptions.

## 10. Git implications
- Use git mv where possible for tracked files. The untracked dissertation directory will be moved with a filesystem move and then tracked with git add if desired.

## 11. Risks and rollback procedure
- Risk: path changes may affect local scripts. Roll back by restoring the original file from git before the move.
- Risk: moving the dissertation directory could affect references that are still pending review. Roll back by moving the directory back to its original path.

## 12. Expected final repository tree
- R/ for package functions.
- scripts/experiments/ for executable scripts.
- docs/audit/ and docs/research/ for governance documents.
- archive/legacy_untrusted/ for non-canonical historical material.
