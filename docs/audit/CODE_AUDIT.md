# Code Audit

## Duplicated functions
- The repository contains multiple analysis and runner scripts, but no obvious duplicate package functions were identified in the inspected source.

## Dead or unreachable code
- Some historical experiment runners may be superseded by package functions; these should be moved to archive or scripts/experiments and treated as entry points rather than canonical package logic.

## Scripts that belong in package functions
- R/run_eval_report.R is an experiment-style driver; it was moved to scripts/experiments/run_eval_report.R in Phase 0.

## Package functions with experiment-specific behavior
- Some functions in R/ may still assume specific datasets or local outputs; this should be reviewed, but not extensively refactored in this phase.

## Global-state dependence
- The code uses the working directory and package-relative paths in a few places; these should be replaced with project-root helpers.

## setwd() usage
- R/run_eval_report.R contained a hard-coded setwd() call; it was the main path risk and was removed in Phase 0 when the script moved to scripts/experiments/run_eval_report.R.

## Implicit random seeds
- Some scripts use seed values explicitly; others may rely on defaults. This should be recorded in the experiment standard.

## Non-deterministic behavior
- The current repository has some dataset-dependent and environment-dependent behavior, especially around optional packages and runtime paths.

## Manual intervention requirements
- Some scripts require local data and optional Python dependencies; these should be documented as not fully automated.

## Hidden input assumptions
- Several dataset loaders assume data are present under the project root; these should be made explicit.

## Output overwriting
- The evaluation runner writes outputs into results/eval_report/ and could overwrite existing files; this should be documented and perhaps isolated by experiment ID in future work.

## Missing input validation
- Several functions assume files exist and use stop() messages; extra validation is recommended but out of scope for this audit-only phase.

## Missing tests
- The repository had tests but no general repository-integrity test for path and working-directory assumptions; tests/testthat/test-repo-audit.R was added in Phase 0 to cover this.

## Conflicting function names
- None identified in the inspected source.

## Inconsistent naming
- Historical names such as run_eval_report and exp3 redesign scripts are still embedded across the repository; these should remain in scripts/experiments/ and be renamed if they become canonical entry points.

## R CMD check risks
- The package contains many Rd files and external dependencies; the repository should not be claimed as R CMD check clean until the dependency environment and package metadata are validated.
