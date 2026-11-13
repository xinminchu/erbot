# Regression tests for the 2026-10-06 phase-2 honesty fixes
# (default-path truth leakage) and the 21/22/23 module merge.

test_that("phase2: er_run defaults are honest (no silent supervised selection)", {
  f <- formals(er_run)
  expect_equal(f$merge, "consensus")
  expect_equal(f$supervised_selection, FALSE)
})

test_that("phase2: er_merge(method='best') warns when truth drives selection", {
  cl <- list(m1 = c(1L, 1L, 2L, 2L), m2 = c(1L, 2L, 1L, 2L))
  S <- Matrix::Diagonal(4)
  tv <- c(1L, 1L, 2L, 2L)
  expect_warning(
    er_merge(cl, S, method = "best", truth_vec = tv),
    "supervised"
  )
  # no truth -> first method, no warning
  expect_no_warning(res <- er_merge(cl, S, method = "best"))
  expect_equal(res, cl$m1)
})

test_that("phase2: scenario presets use unsupervised merge defaults", {
  p <- er_scenario_presets()
  expect_equal(nrow(p), 4L)
  expect_true(all(p$merge == "consensus"))
  expect_equal(sort(p$scenario),
               c("full", "screening", "smoke", "unsupervised_quick"))
})

test_that("phase2: er_explain_error maps failures to stage/reason/next_step", {
  e <- er_explain_error("cannot open file 'foo.csv'")
  expect_equal(e$stage, "load")
  expect_true(nzchar(e$reason) && nzchar(e$next_step))
  e2 <- er_explain_error("pair budget exceeded", stage = "block")
  expect_equal(e2$stage, "block")
})

test_that("phase2: new 21/22/23 modules are exported and load", {
  expect_true(is.function(er_run_scenario))
  expect_true(is.function(er_stratified_three_way_split))
  expect_true(is.function(er_augment_fit_pairs))
  expect_true(is.function(er_parse_d10k_aggregate))
  expect_true(is.function(er_d10k_similarity))
})
