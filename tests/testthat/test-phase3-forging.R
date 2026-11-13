########################################
# Phase 3 forging tests (2026-10-06)
#
#   1. RNG discipline: functions that seed internally must preserve the
#      caller's global RNG state, and stay reproducible.
#   2. Dense guards: n x n densification paths refuse loudly above max_n
#      instead of OOMing.
#   3. er_absorb_small: sparse rewrite matches dense semantics.
#   4. API surface: neural experiments and plumbing are not exported;
#      the public pipeline API is intact.
########################################
library(testthat)
library(erbot)

# ── 1. RNG discipline ─────────────────────────────────────────────────────────

test_that("er_split preserves the caller's RNG state", {
  set.seed(123)
  before <- .Random.seed
  id_vec <- paste0("r", seq_len(20))
  truth <- tibble::tibble(id = id_vec, cluster_id = rep(1:4, each = 5))
  er_split(id_vec, truth = truth, k = 2L, seed = 99L)
  expect_identical(.Random.seed, before)
})

test_that("er_split is reproducible regardless of global RNG state", {
  id_vec <- paste0("r", seq_len(20))
  truth <- tibble::tibble(id = id_vec, cluster_id = rep(1:4, each = 5))
  f1 <- er_split(id_vec, truth = truth, k = 2L, seed = 7L)
  runif(10)  # advance the global stream; must not matter
  f2 <- er_split(id_vec, truth = truth, k = 2L, seed = 7L)
  expect_identical(f1, f2)
})

test_that(".rng_save restores state via on.exit", {
  set.seed(42)
  before <- .Random.seed
  local({
    .rng_restore <- erbot:::.rng_save()
    on.exit(.rng_restore(), add = TRUE)
    set.seed(999)
    runif(5)
  })
  expect_identical(.Random.seed, before)
})

# ── 2. Dense guards ───────────────────────────────────────────────────────────

test_that(".check_dense_n refuses loudly above max_n", {
  expect_error(erbot:::.check_dense_n(10000, "test matrix"), "Refusing")
  expect_error(erbot:::.check_dense_n(6000, "test matrix", max_n = 5000L),
               "0.3 GB")
  expect_silent(erbot:::.check_dense_n(100, "test matrix"))
})

test_that("er_cluster refuses dense hclust/PAM/GC above the guard", {
  skip_if_not_installed("Matrix")
  n <- 6000L
  # Sparse diagonal: cheap to build, would be 288 MB dense.
  S <- Matrix::sparseMatrix(i = seq_len(n), j = seq_len(n), x = 1,
                            dims = c(n, n))
  expect_error(er_cluster(S, method = "hclust_avg", k = 5), "Refusing")
  expect_error(er_cluster(S, method = "pam", k = 5), "Refusing")
  expect_error(er_cluster(S, method = "gc", threshold = 0.5), "Refusing")
})

# ── 3. er_absorb_small sparse rewrite ─────────────────────────────────────────

test_that("er_absorb_small absorbs singletons into the most similar large cluster", {
  skip_if_not_installed("Matrix")
  # 6 records: cluster 3 is a singleton, most similar to cluster 2.
  S <- Matrix::Matrix(matrix(c(
    1,  .9, .8, .1, .1, .1,
    .9, 1,  .85,.1, .1, .1,
    .8, .85,1,  .1, .1, .1,
    .1, .1, .1, 1,  .9, .85,
    .1, .1, .1, .9, 1,  .9,
    .1, .1, .1, .85,.9, 1), 6, 6), sparse = TRUE)
  labels <- c(1L, 1L, 1L, 2L, 2L, 3L)
  out <- er_absorb_small(labels, S, m_min = 2L)
  # Record 6 joins cluster 2 (mean sim 0.875) rather than cluster 1 (0.1).
  expect_equal(out[6], out[4])
  expect_equal(length(unique(out)), 2L)
})

# ── 4. API surface ────────────────────────────────────────────────────────────

test_that("neural experiments and plumbing are not exported", {
  ns <- getNamespaceExports("erbot")
  for (fn in c("RecordEncoder", "contrastive_loss", "stability_penalty",
               "train_one_epoch", "run_training", "build_knn_graph",
               "perturb_graph", "run_graph_clustering",
               "projection_from_membership", "graph_laplacian_matrix",
               "graph_smoothness_loss")) {
    expect_false(fn %in% ns, info = fn)
  }
  for (fn in c("er_require", "er_measure_tm", "er_with_seed",
               "er_gc_peak_mb", "er_safe_write_csv")) {
    expect_false(fn %in% ns, info = fn)
  }
  # The public pipeline API is intact.
  for (fn in c("er_run", "er_split", "er_tune", "er_cv", "er_protocol_a",
               "er_protocol_b", "er_block", "er_cluster", "er_merge",
               "er_evaluate", "er_ablation_table")) {
    expect_true(fn %in% ns, info = fn)
  }
})
