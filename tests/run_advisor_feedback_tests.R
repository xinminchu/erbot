# Advisor-feedback test runner (2026-10-01).
# Usage (from the erbot package root):
#   Rscript tests/run_advisor_feedback_tests.R
# Loads the package with pkgload (no install needed) and runs the
# advisor-feedback test file. Exits non-zero on any failure.

if (!requireNamespace("pkgload", quietly = TRUE)) {
  stop("Package 'pkgload' is required to run these tests without installing erbot.")
}
if (!requireNamespace("testthat", quietly = TRUE)) {
  stop("Package 'testthat' is required to run these tests.")
}

suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))

.erbot_root <- normalizePath(getwd())
assign(".erbot_root", .erbot_root, envir = globalenv())

res <- testthat::test_file(
  "tests/testthat/test-advisor-feedback-2026-10-01.R",
  reporter = "summary"
)
df <- as.data.frame(res)
n_failed <- sum(df$failed) + sum(df$error)
cat(sprintf(
  "\n[advisor-feedback] tests: %d | failed: %d | skipped: %d\n",
  sum(df$nb), n_failed, sum(df$skipped)
))
if (n_failed > 0) quit(status = 1L)
cat("[advisor-feedback] ALL PASS\n")
