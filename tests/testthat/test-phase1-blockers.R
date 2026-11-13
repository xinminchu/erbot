# Regression tests for the 2026-10-06 phase-1 blocker fixes.
# Run with: testthat::test_local()  (or devtools::test()) from the package root.
# These tests need: tibble, dplyr (already Imports of erbot).

test_that("B-01: er_similarity survives NA/empty-string mixes (jw/lv/jaccard/bow)", {
  df <- data.frame(
    title = c("apple inc", NA, "", "apple incorporated", "banana"),
    stringsAsFactors = FALSE
  )
  pairs <- tibble::tibble(idx1 = c(1L, 1L, 2L, 3L), idx2 = c(2L, 4L, 3L, 5L))
  for (m in c("jw", "lv", "jaccard", "bow")) {
    spec <- list(list(name = "title", type = m))
    sim <- er_similarity(df, pairs, spec = spec)
    expect_equal(length(sim$title), 4L, info = paste("method", m))
    # NA vs text -> NA ; NA vs "" -> NA ; "" vs text -> NA
    expect_true(is.na(sim$title[1]), info = paste("method", m, "pair 1"))
    expect_true(is.na(sim$title[3]), info = paste("method", m, "pair 3"))
    expect_true(is.na(sim$title[4]), info = paste("method", m, "pair 4"))
    # real vs real -> a number in [0, 1]
    expect_true(!is.na(sim$title[2]) && sim$title[2] >= 0 && sim$title[2] <= 1,
                info = paste("method", m, "pair 2"))
  }
})

test_that("B-04: er_evaluate aligns named predictions by id, not position", {
  # truth: a->1, b->1, c->2, d->2
  truth <- data.frame(id = c("a", "b", "c", "d"),
                      cluster_id = c(1L, 1L, 2L, 2L),
                      stringsAsFactors = FALSE)
  id_vec <- c("a", "b", "c", "d")
  # mismatched id sets must error loudly, not silently misalign (no ARI backend needed)
  pred_bad <- setNames(c(1L, 1L, 2L, 2L), c("a", "b", "c", "zzz"))
  expect_error(er_evaluate(list(bad = pred_bad), truth, id_vec = id_vec))
  # unnamed vector of wrong length must error
  expect_error(er_evaluate(list(short = c(1L, 1L)), truth, id_vec = id_vec))
  # perfect prediction in REVERSED order with names -> ARI must be 1 after alignment
  if (!requireNamespace("GCMER", quietly = TRUE) &&
      !requireNamespace("mclust", quietly = TRUE))
    skip("ARI backend (GCMER/mclust) not installed")
  pred_rev <- setNames(c(2L, 2L, 1L, 1L), c("d", "c", "b", "a"))
  perf <- er_evaluate(list(rev = pred_rev), truth, id_vec = id_vec)
  expect_equal(nrow(perf), 1L)
  expect_equal(perf$ARI[[1]], 1)
})

test_that("B-02: blocking estimates budget before allocating; missing keys excluded", {
  df <- data.frame(key = c("aa", "aa", "ab", NA, "", "ab"),
                   stringsAsFactors = FALSE)
  # missing keys (rows 4,5) must not pair with each other or anyone
  p <- er_block(df, method = "standard", block_key = "key", max_pairs = 100)
  expect_true(all(p$idx1 != 4L & p$idx2 != 4L))
  expect_true(all(p$idx1 != 5L & p$idx2 != 5L))
  # "aa" block -> (1,2); "ab" block -> (3,6)
  expect_equal(nrow(p), 2L)
  # a single huge block exceeding budget is skipped, not OOM-allocated
  big <- data.frame(key = rep("same", 3000), stringsAsFactors = FALSE)
  expect_warning(
    p2 <- er_block(big, method = "standard", block_key = "key", max_pairs = 100),
    "pair budget"
  )
  expect_equal(nrow(p2), 0L)
})

test_that("B-02: er_block auto without diag never crashes (M-29)", {
  df <- data.frame(x = rnorm(6000))
  expect_no_error(p <- er_block(df))  # n > 5000, no diag -> sn fallback on row order
  expect_true(all(c("idx1", "idx2") %in% names(p)))
})

test_that("B-03: er_stability_ari compares the SAME records (intersection)", {
  skip_if_not_installed("mclust")
  # two well-separated blobs: any sane clustering is perfectly stable
  set.seed(7)
  Z <- rbind(matrix(rnorm(40, mean = 0), ncol = 2),
             matrix(rnorm(40, mean = 8), ncol = 2))
  fit <- function(X, ...) {
    km <- kmeans(X, centers = 2, nstart = 10)
    km$cluster
  }
  s <- er_stability_ari(Z, B = 6L, frac = 0.8, fit_fun = fit, seed = 42)
  expect_true(!is.na(s) && s > 0.9)
  # degenerate input -> NA, not a crash or a bogus number
  expect_true(is.na(er_stability_ari(Z[1:3, , drop = FALSE], fit_fun = fit)))
})
