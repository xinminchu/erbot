# Regression tests for phase 5: learned blocking via a neural retriever
# (R/25-learned_blocking.R). The retriever is trained for the blocking
# objective (recall@K), not classification accuracy.
#
# Key behaviors locked in:
#  * a trained retriever beats untrained n-gram cosine on recall@K;
#  * K comes from the pair budget alone (er_k_for_budget) -- truth never
#    selects K;
#  * er_blocking_heldout() splits entities (not pairs) and reports recall
#    on unseen entities only.

.make_dup_texts <- function() {
  set.seed(42)
  first <- c("Alpha", "Beta", "Gamma", "Delta", "Sigma",
             "Omega", "Kappa", "Zeta", "Theta", "Lambda")
  texts <- character(0); truth <- integer(0)
  for (e in 1:20) {
    base <- paste(sample(first, 1), "Trading", e, "Corp")
    for (v in 1:4) {
      t <- base
      r <- runif(1)
      if (r < 0.3) t <- gsub("Trading", "Trd", t, fixed = TRUE)
      else if (r < 0.5) t <- gsub("Corp", "Corporation", t, fixed = TRUE)
      else if (r < 0.65) {
        i <- sample.int(nchar(t), 1)
        t <- paste0(substr(t, 1, i - 1), substr(t, i + 1, nchar(t)))
      }
      texts <- c(texts, t); truth <- c(truth, e)
    }
  }
  list(texts = texts, truth = truth)
}

test_that("char n-grams produce a dense non-negative matrix", {
  d <- .make_dup_texts()
  X <- er_char_ngrams(d$texts)
  expect_true(is.matrix(X))
  expect_equal(nrow(X), length(d$texts))
  expect_equal(ncol(X), 512L)
  expect_true(all(X >= 0))
  expect_true(any(X > 0))
})

test_that("trained retriever beats untrained cosine on recall@K", {
  d <- .make_dup_texts()
  X <- er_char_ngrams(d$texts)
  tl <- split(seq_along(d$truth), d$truth)
  di <- unlist(lapply(tl, function(m) utils::combn(m, 2L)[1L, ]))
  dj <- unlist(lapply(tl, function(m) utils::combn(m, 2L)[2L, ]))

  model <- er_train_retriever(X, di, dj, epochs = 10L, seed = 42L)
  E <- er_embed_records(model, X)
  expect_equal(nrow(E), nrow(X))
  expect_equal(ncol(E), 64L)
  # rows are L2-normalized
  expect_true(all(abs(sqrt(rowSums(E^2)) - 1) < 1e-6))

  df <- data.frame(name = d$texts, stringsAsFactors = FALSE)
  p_learned <- er_block_embed(df, "name", model = model, k = 10L)
  p_plain <- er_block_embed(df, "name", model = NULL, k = 10L)
  r_learned <- er_recall_at_k(p_learned, d$truth)
  r_plain <- er_recall_at_k(p_plain, d$truth)
  expect_gt(r_learned, r_plain)
  expect_gt(r_learned, 0.9)
})

test_that("K comes from the budget, not from truth", {
  expect_equal(er_k_for_budget(400L, 4000), 20L)
  expect_equal(er_k_for_budget(35L, 2e6), 114285L)  # capped later by n - 1
  expect_equal(er_k_for_budget(100L, 10), 1L)       # floor of 1
})

test_that("held-out eval splits entities and scores unseen only", {
  d <- .make_dup_texts()
  X <- er_char_ngrams(d$texts)
  res <- er_blocking_heldout(X, d$truth, k = 10L, test_size = 0.3,
                             seed = 42L, epochs = 10L)
  expect_true(res$recall >= 0 && res$recall <= 1)
  expect_equal(res$n_train_entities + res$n_test_entities, 20L)
  expect_gt(res$n_train_pairs, 0L)
  expect_gt(res$n_test_pairs, 0L)
  # train and test pairs are disjoint by construction (entity split)
  expect_gt(res$recall, 0.9)
})

test_that("retriever refuses to train on < 4 pairs", {
  X <- matrix(rnorm(10 * 512), 10, 512)
  expect_error(er_train_retriever(X, 1L, 2L), ">= 4")
})
