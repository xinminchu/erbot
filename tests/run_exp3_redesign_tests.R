# Runner for Exp 3 redesign guardrail tests (2026-06-13).
#   Rscript tests/run_exp3_redesign_tests.R
# testthat-or-shim (testthat absent from renv). Exits non-zero on failure.

suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
.erbot_root <- normalizePath(getwd()); assign(".erbot_root", .erbot_root, envir = globalenv())
TEST_FILE <- "tests/testthat/test-exp3-redesign.R"

if (requireNamespace("testthat", quietly = TRUE)) {
  res <- testthat::test_file(TEST_FILE, reporter = "summary")
  df <- as.data.frame(res)
  cat(sprintf("\n[exp3-redesign] tests: %d | failed: %d | skipped: %d\n",
              sum(df$nb), sum(df$failed), sum(df$skipped)))
  if (sum(df$failed) > 0) quit(status = 1L)
  cat("[exp3-redesign] ALL PASS\n")
} else {
  .p <- 0L; .f <- 0L; .s <- 0L; .msg <- character(0)
  .chk <- function(ok, what, label=NULL){ l <- if(is.null(label)) what else paste0(what," [",label,"]")
    if(isTRUE(ok)) .p<<-.p+1L else {.f<<-.f+1L; .msg<<-c(.msg,l); cat("  FAIL:",l,"\n")}; invisible(ok) }
  expect_true <- function(x,label=NULL) .chk(isTRUE(x),"expect_true",label)
  expect_false <- function(x,label=NULL) .chk(isFALSE(x),"expect_false",label)
  expect_setequal <- function(a,b,label=NULL) .chk(setequal(a,b),"expect_setequal",label)
  expect_gt <- function(a,b,label=NULL) .chk(a>b,"expect_gt",label)
  expect_gte <- function(a,b,label=NULL) .chk(a>=b,"expect_gte",label)
  expect_lte <- function(a,b,label=NULL) .chk(a<=b,"expect_lte",label)
  expect_no_error <- function(expr,label=NULL){ ok<-tryCatch({force(expr);TRUE},
    error=function(e){cat("    error:",conditionMessage(e),"\n");FALSE}); .chk(ok,"expect_no_error",label) }
  test_that <- function(desc,code){ cat("*",desc,"\n")
    tryCatch(force(code), error=function(e){.f<<-.f+1L; .msg<<-c(.msg,paste0(desc,": ERROR ",conditionMessage(e)))
      cat("  ERROR:",conditionMessage(e),"\n")}) }
  suppressWarnings(source(TEST_FILE, local = FALSE))
  cat(sprintf("\n[exp3-redesign] assertions passed: %d | failed: %d\n", .p, .f))
  if (.f > 0L) { cat("[exp3-redesign] FAILURES:\n - ", paste(.msg, collapse="\n - "), "\n", sep="")
    quit(status = 1L) }
  cat("[exp3-redesign] ALL PASS\n")
}
