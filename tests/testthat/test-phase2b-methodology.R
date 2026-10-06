########################################
# Phase 2b methodology tests (2026-10-06)
#
# Regression tests for the honest-evaluation rewrites:
#   1. er_tune(): external objectives ("ari"/"pairwise_f1") must fail loudly
#      without truth, and with truth must use Fit/Validation/Test with the
#      test set evaluated exactly once.
#   2. er_protocol_b(): the test set must be a true entity-disjoint
#      held-out (no entity spans selection/test), labels must align to the
#      test records, and evaluation must happen once on test.
#   3. er_cv(): folds must be entity-disjoint and merge must default to the
#      unsupervised "consensus" (test truth must not select the merge).
########################################
library(testthat)
library(erbot)

# ── 1. er_tune: loud failure without truth ────────────────────────────────────

test_that("er_tune: ari objective without truth fails loudly (no silent NAs)", {
  df <- data.frame(txt = paste("record", seq_len(20)), stringsAsFactors = FALSE)
  # The honesty gate runs before any embedding, so no text2vec needed here.
  expect_error(
    er_tune(df, "txt",
            methods = "kmeans",
            grids = list(kmeans = data.frame(k = 2, nstart = 1)),
            objective = "ari", truth = NULL),
    "truth"
  )
  expect_error(
    er_tune(df, "txt",
            methods = "kmeans",
            grids = list(kmeans = data.frame(k = 2, nstart = 1)),
            objective = "pairwise_f1",
            truth = list(truth_pairs = NULL)),
    "truth"
  )
})

# ── 2. er_tune: honest Fit/Validation/Test ────────────────────────────────────

test_that("er_tune: external objective uses entity-disjoint Fit/Validation/Test", {
  skip_if_not_installed("text2vec")
  skip_if_not_installed("irlba")
  skip_if_not_installed("mclust")

  words <- c("apple", "bravo", "charlie", "delta", "echo", "foxtrot")
  ent <- rep(seq_along(words), each = 10)
  w <- words[ent]
  txt <- paste(w, w, "shared filler words here", seq_along(ent))
  df <- data.frame(txt = txt, stringsAsFactors = FALSE)

  res <- er_tune(df, "txt",
                 methods = "kmeans",
                 grids = list(kmeans = data.frame(k = c(2, 3), nstart = 1)),
                 objective = "ari",
                 truth = list(truth_vec = ent),
                 seed = 11L)

  expect_true(res$honest)
  sp <- res$split
  expect_named(sp, c("fit", "validation", "test"))

  # Entity-disjoint: no gold entity spans two partitions.
  expect_length(intersect(ent[sp$fit], ent[sp$validation]), 0)
  expect_length(intersect(ent[sp$fit], ent[sp$test]), 0)
  expect_length(intersect(ent[sp$validation], ent[sp$test]), 0)
  # Every record assigned exactly once.
  expect_setequal(unlist(sp), seq_len(nrow(df)))

  # Curves report VALIDATION metrics (dev-set selection), not test metrics.
  expect_true(all(c("ari", "pairwise_f1") %in% names(res$curves)))
  expect_false(is.null(res$best$kmeans))

  # The winner is evaluated ONCE on the held-out test set.
  tm <- res$test_metrics
  expect_false(is.null(tm))
  expect_true(is.numeric(tm$ari) && length(tm$ari) == 1L)
  expect_false(is.na(tm$ari))
  expect_equal(tm$n_test, length(sp$test))
  expect_equal(tm$n_test_labelled, length(sp$test))  # all labelled here
})

test_that("er_tune: internal objective keeps legacy full-data behaviour", {
  skip_if_not_installed("text2vec")
  skip_if_not_installed("irlba")

  df <- data.frame(txt = paste("record", rep(c("aa", "bb"), 10), seq_len(20)),
                   stringsAsFactors = FALSE)
  res <- er_tune(df, "txt",
                 methods = "kmeans",
                 grids = list(kmeans = data.frame(k = 2, nstart = 1)),
                 objective = "silhouette_penalized",
                 truth = NULL)
  expect_false(res$honest)
  expect_null(res$test_metrics)
  expect_null(res$split)
  expect_true(nrow(res$curves) >= 1L)
})

# ── 3. er_protocol_b: true held-out ───────────────────────────────────────────

test_that("er_protocol_b: test is an entity-disjoint held-out evaluated once", {
  skip_if_not_installed("igraph")

  ent <- rep(1:6, each = 5)
  n <- length(ent)
  id_vec <- paste0("r", seq_len(n))
  truth_tbl <- tibble::tibble(id = id_vec, cluster_id = ent)
  pr <- utils::combn(n, 2)
  pairs <- tibble::tibble(idx1 = pr[1, ], idx2 = pr[2, ])
  sim_list <- list(name = ifelse(ent[pairs$idx1] == ent[pairs$idx2], 0.9, 0.1))

  res <- er_protocol_b(sim_list, pairs, n, truth_tbl, id_vec,
                       method_grid = "threshold_cc",
                       tau_grid = c(0.3, 0.5, 0.7),
                       n_folds = 2L, test_fraction = 0.3,
                       base_seed = 5L, verbose = FALSE)

  # Entity-disjoint selection/test: no entity spans the boundary.
  expect_length(intersect(ent[res$selection_idx], ent[res$test_idx]), 0)
  expect_setequal(c(res$selection_idx, res$test_idx), seq_len(n))

  # Labels are for the test records only, aligned to test_id_vec.
  expect_length(res$labels, length(res$test_idx))
  expect_equal(res$test_id_vec, id_vec[res$test_idx])

  # Selection-set CV produced per-fold winners.
  expect_true(nrow(res$fold_best) >= 1L)
  expect_true(all(c("fold", "method", "tau", "train_ARI") %in% names(res$fold_best)))

  # Consensus params are sane.
  expect_identical(res$consensus_method, "threshold_cc")
  expect_true(res$consensus_tau %in% c(0.3, 0.5, 0.7))

  # One-time held-out metrics exist and are numeric.
  expect_true(is.numeric(res$ARI) && length(res$ARI) == 1L)
  expect_true(is.numeric(res$Bcubed_F) && length(res$Bcubed_F) == 1L)
  # Clean toy: perfect separation -> ARI should be 1.
  expect_equal(res$ARI, 1)
})

# ── 4. er_cv: entity-disjoint folds, honest merge default ─────────────────────

test_that("er_cv: merge defaults to consensus (not supervised best)", {
  expect_identical(formals(er_cv)$merge, "consensus")
})

test_that("er_cv: folds are entity-disjoint (no train/test entity leakage)", {
  skip_if_not_installed("igraph")

  ent <- rep(1:6, each = 5)
  df <- data.frame(
    id = paste0("r", seq_along(ent)),
    name = paste(c("alpha", "beta", "gamma", "delta", "epsilon", "zeta")[ent],
                 seq_along(ent)),
    stringsAsFactors = FALSE
  )
  truth <- tibble::tibble(id = df$id, cluster_id = ent)

  res <- er_cv(df, truth, K = 2L, seed = 3L, block = "none",
               weights = "equal", cluster_methods = "threshold_cc",
               verbose = FALSE)

  expect_true(res$n_folds_ok >= 1L)
  expect_length(res$folds, 2L)
  for (f in res$folds) {
    train_ent <- ent[f$train_idx]
    test_ent <- ent[f$test_idx]
    # No entity appears on both sides of the fold.
    expect_length(intersect(train_ent, test_ent), 0)
    # Fold covers all records exactly once.
    expect_setequal(c(f$train_idx, f$test_idx), seq_along(ent))
  }
})
