# Wave 1 validation runner (2026-06-12).
# Usage (from the erbot package root):
#   Rscript tests/run_wave1_validation.R
# Loads the package with pkgload (no install needed) and runs the Wave 1
# bugfix test file. Uses testthat if installed; otherwise a minimal
# compatible shim. Exits non-zero on any failure.

suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))

.erbot_root <- normalizePath(getwd())
assign(".erbot_root", .erbot_root, envir = globalenv())

if (requireNamespace("testthat", quietly = TRUE)) {
  res <- testthat::test_file("tests/testthat/test-wave1-bugfixes.R",
                             reporter = "summary")
  df <- as.data.frame(res)
  cat(sprintf("\n[wave1] tests: %d | failed: %d | skipped: %d\n",
              sum(df$nb), sum(df$failed), sum(df$skipped)))
  if (sum(df$failed) > 0) quit(status = 1L)
  cat("[wave1] ALL PASS\n")
} else {
  # ── minimal testthat-compatible shim ─────────────────────────────────────
  .n_pass <- 0L; .n_fail <- 0L; .n_skip <- 0L
  .fail_msgs <- character(0)
  .skip_signal <- structure(class = c("wave1_skip", "condition"),
                            list(message = "skip", call = NULL))

  .chk <- function(ok, what, label = NULL) {
    lbl <- if (is.null(label)) what else paste0(what, " [", label, "]")
    if (isTRUE(ok)) { .n_pass <<- .n_pass + 1L }
    else { .n_fail <<- .n_fail + 1L; .fail_msgs <<- c(.fail_msgs, lbl)
           cat("  FAIL:", lbl, "\n") }
    invisible(ok)
  }
  expect_true  <- function(x, label = NULL) .chk(isTRUE(x), "expect_true", label)
  expect_false <- function(x, label = NULL) .chk(isFALSE(x), "expect_false", label)
  expect_identical <- function(a, b, label = NULL)
    .chk(identical(a, b), "expect_identical", label)
  expect_equal <- function(a, b, tolerance = 1e-8, label = NULL)
    .chk(isTRUE(all.equal(a, b, tolerance = tolerance)), "expect_equal", label)
  expect_gt  <- function(a, b, label = NULL) .chk(a >  b, "expect_gt",  label)
  expect_gte <- function(a, b, label = NULL) .chk(a >= b, "expect_gte", label)
  expect_lte <- function(a, b, label = NULL) .chk(a <= b, "expect_lte", label)
  expect_no_error <- function(expr, label = NULL) {
    ok <- tryCatch({ force(expr); TRUE }, error = function(e) {
      cat("    error:", conditionMessage(e), "\n"); FALSE })
    .chk(ok, "expect_no_error", label)
  }
  skip_if_not_installed <- function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) stop(.skip_signal)
  }
  test_that <- function(desc, code) {
    cat("*", desc, "\n")
    tryCatch(force(code),
             wave1_skip = function(c) { .n_skip <<- .n_skip + 1L
                                        cat("  (skipped)\n") },
             error = function(e) { .n_fail <<- .n_fail + 1L
               .fail_msgs <<- c(.fail_msgs, paste0(desc, ": ERROR ",
                                                   conditionMessage(e)))
               cat("  ERROR:", conditionMessage(e), "\n") })
  }

  source("tests/testthat/test-wave1-bugfixes.R", local = FALSE)

  cat(sprintf("\n[wave1] assertions passed: %d | failed: %d | skipped tests: %d\n",
              .n_pass, .n_fail, .n_skip))
  if (.n_fail > 0L) { cat("[wave1] FAILURES:\n - ",
                          paste(.fail_msgs, collapse = "\n - "), "\n", sep = "")
                      quit(status = 1L) }
  cat("[wave1] ALL PASS\n")
}
