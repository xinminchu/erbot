# Runner for the BUG-22 label-leakage guardrail tests (2026-06-12).
#   Rscript tests/run_label_leakage_tests.R
# Uses testthat if installed; otherwise a minimal compatible shim (testthat is
# not in the renv library). Exits non-zero on any failure.

suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
.erbot_root <- normalizePath(getwd())
assign(".erbot_root", .erbot_root, envir = globalenv())

TEST_FILE <- "tests/testthat/test-label-leakage.R"

if (requireNamespace("testthat", quietly = TRUE)) {
  res <- testthat::test_file(TEST_FILE, reporter = "summary")
  df  <- as.data.frame(res)
  cat(sprintf("\n[label-leakage] tests: %d | failed: %d | skipped: %d\n",
              sum(df$nb), sum(df$failed), sum(df$skipped)))
  if (sum(df$failed) > 0) quit(status = 1L)
  cat("[label-leakage] ALL PASS\n")
} else {
  .n_pass <- 0L; .n_fail <- 0L; .n_skip <- 0L; .fail_msgs <- character(0)
  .skip_signal <- structure(class = c("ll_skip", "condition"),
                            list(message = "skip", call = NULL))
  .chk <- function(ok, what, label = NULL) {
    lbl <- if (is.null(label)) what else paste0(what, " [", label, "]")
    if (isTRUE(ok)) .n_pass <<- .n_pass + 1L
    else { .n_fail <<- .n_fail + 1L; .fail_msgs <<- c(.fail_msgs, lbl)
           cat("  FAIL:", lbl, "\n") }
    invisible(ok)
  }
  expect_true  <- function(x, label = NULL) .chk(isTRUE(x), "expect_true", label)
  expect_false <- function(x, label = NULL) .chk(isFALSE(x), "expect_false", label)
  expect_equal <- function(a, b, tolerance = 1e-8, label = NULL)
    .chk(isTRUE(all.equal(a, b, tolerance = tolerance)), "expect_equal", label)
  expect_setequal <- function(a, b, label = NULL)
    .chk(setequal(a, b), "expect_setequal", label)
  expect_gt <- function(a, b, label = NULL) .chk(a > b, "expect_gt", label)
  expect_no_error <- function(expr, label = NULL) {
    ok <- tryCatch({ force(expr); TRUE }, error = function(e) {
      cat("    error:", conditionMessage(e), "\n"); FALSE })
    .chk(ok, "expect_no_error", label)
  }
  expect_warning <- function(expr, regexp = NULL, label = NULL) {
    got <- FALSE
    withCallingHandlers(tryCatch(force(expr), error = function(e) NULL),
      warning = function(w) {
        if (is.null(regexp) || grepl(regexp, conditionMessage(w)))
          got <<- TRUE
        invokeRestart("muffleWarning")
      })
    .chk(got, "expect_warning", label)
  }
  test_that <- function(desc, code) {
    cat("*", desc, "\n")
    tryCatch(force(code),
             ll_skip = function(c) { .n_skip <<- .n_skip + 1L; cat("  (skipped)\n") },
             error = function(e) { .n_fail <<- .n_fail + 1L
               .fail_msgs <<- c(.fail_msgs, paste0(desc, ": ERROR ", conditionMessage(e)))
               cat("  ERROR:", conditionMessage(e), "\n") })
  }
  source(TEST_FILE, local = FALSE)
  cat(sprintf("\n[label-leakage] assertions passed: %d | failed: %d | skipped: %d\n",
              .n_pass, .n_fail, .n_skip))
  if (.n_fail > 0L) {
    cat("[label-leakage] FAILURES:\n - ", paste(.fail_msgs, collapse = "\n - "), "\n", sep = "")
    quit(status = 1L)
  }
  cat("[label-leakage] ALL PASS\n")
}
