# Regression tests for phase 4: the advisor's 11 supervised classifiers fused
# into the package (R/24-supervised_classifiers.R), replacing er_cluster's
# old svm/gbm methods. Firth logistic is the default.
#
# Key behaviors locked in:
#  * plain-glm logistic is structurally invalid under complete separation
#    (the advisor's Scenario-1 finding), while Firth stays finite;
#  * structural inapplicability -> valid = FALSE + reason, never a crash;
#  * er_cluster's supervised methods train on pair features from sim_list.

# ── Toy pair data ─────────────────────────────────────────────────────────────

.make_well_behaved <- function() {
  set.seed(42)
  n_pos <- 60L; n_neg <- 60L
  f1 <- c(rnorm(n_pos, 0.75, 0.15), rnorm(n_neg, 0.35, 0.15))
  f2 <- c(rnorm(n_pos, 0.70, 0.15), rnorm(n_neg, 0.30, 0.15))
  list(
    x = data.frame(f1 = pmin(1, pmax(0, f1)), f2 = pmin(1, pmax(0, f2))),
    y = rep(c(1L, 0L), c(n_pos, n_neg))
  )
}

# Complete separation on f1: plain glm() must diverge here.
.make_separated <- function() {
  set.seed(7)
  n <- 40L
  f1 <- c(rnorm(n, 0.95, 0.02), rnorm(n, 0.05, 0.02))
  f2 <- c(rnorm(n, 0.90, 0.05), rnorm(n, 0.10, 0.05))
  list(
    x = data.frame(f1 = pmin(1, pmax(0, f1)), f2 = pmin(1, pmax(0, f2))),
    y = rep(c(1L, 0L), c(n, n))
  )
}

.pkg_for <- function(classifier, logistic_method = "firth") {
  switch(classifier,
         logistic = if (logistic_method == "firth") "brglm2" else NA,
         lda = "MASS", qda = "MASS",
         knn = "FNN", wknn = "FNN",
         tree = "rpart", rf = "randomForest",
         xgboost = "xgboost", nnet = "nnet",
         fellegi_sunter = NA,
         svm_radial = "e1071")
}

test_that("er_supervised_classifiers lists the advisor's 11 families", {
  expect_equal(er_supervised_classifiers(),
               c("logistic", "lda", "qda", "knn", "wknn", "tree",
                 "rf", "xgboost", "nnet", "fellegi_sunter", "svm_radial"))
})

test_that("er_pair_features builds one row per pair, one column per field", {
  sim_list <- list(title = c(0.9, 0.2, 0.8), authors = c(0.7, 0.1, 0.6))
  pairs <- data.frame(idx1 = c(1L, 1L, 2L), idx2 = c(2L, 3L, 3L))
  feat <- er_pair_features(sim_list, pairs)
  expect_equal(dim(feat), c(3L, 2L))
  expect_equal(feat$title, c(0.9, 0.2, 0.8))
  expect_error(er_pair_features(list(a = 1:2), pairs), "length")
})

test_that("all 11 classifiers fit well-behaved toy data", {
  d <- .make_well_behaved()
  for (cl in er_supervised_classifiers()) {
    pkg <- .pkg_for(cl)
    if (!is.na(pkg) && !requireNamespace(pkg, quietly = TRUE))
      next
    fit <- er_pair_classify(d$x, d$y, classifier = cl, seed = 11L)
    expect_true(isTRUE(fit$valid),
                info = paste(cl, "reason:", fit$invalid_reason))
    expect_true(all(fit$train_prob >= 0 & fit$train_prob <= 1), info = cl)
    expect_equal(length(fit$train_prob), nrow(d$x), info = cl)
    # mean predicted P(match) should be higher for true matches
    pr <- fit$predict(d$x)
    expect_true(mean(pr[d$y == 1L]) > mean(pr[d$y == 0L]), info = cl)
  }
})

test_that("advisor's core finding: glm diverges under separation, Firth survives", {
  d <- .make_separated()
  glm_fit <- er_pair_classify(d$x, d$y, classifier = "logistic",
                              logistic_method = "glm")
  expect_false(isTRUE(glm_fit$valid))
  expect_match(glm_fit$invalid_reason, "converge|boundary|finite|rank",
               ignore.case = TRUE)
  if (!requireNamespace("brglm2", quietly = TRUE))
    skip("brglm2 not installed")
  firth_fit <- er_pair_classify(d$x, d$y, classifier = "logistic",
                                logistic_method = "firth")
  expect_true(isTRUE(firth_fit$valid),
              info = firth_fit$invalid_reason)
  expect_true(all(is.finite(firth_fit$train_prob)))
  # Firth default in er_pair_classify
  dflt <- er_pair_classify(d$x, d$y, classifier = "logistic")
  expect_true(isTRUE(dflt$valid))
  expect_equal(dflt$params$logistic_method, "firth")
})

test_that("invalid fits report, never crash: one-class y, tiny qda minority", {
  d <- .make_well_behaved()
  expect_error(er_pair_classify(d$x, rep(1L, nrow(d$x)),
                                classifier = "logistic"),
               "both classes")
  if (requireNamespace("MASS", quietly = TRUE)) {
    tiny <- list(x = d$x[1:12, ], y = c(rep(1L, 2L), rep(0L, 10L)))
    q <- er_pair_classify(tiny$x, tiny$y, classifier = "qda")
    expect_false(isTRUE(q$valid))
    expect_match(q$invalid_reason, "minority|rank|variance", ignore.case = TRUE)
  }
})

test_that("RNG state is preserved across stochastic fits", {
  d <- .make_well_behaved()
  for (cl in c("rf", "xgboost", "nnet", "svm_radial")) {
    pkg <- .pkg_for(cl)
    if (!requireNamespace(pkg, quietly = TRUE)) next
    set.seed(999)
    before <- .Random.seed
    invisible(er_pair_classify(d$x, d$y, classifier = cl, seed = 3L))
    expect_identical(.Random.seed, before, info = cl)
  }
})

test_that("er_cluster supervised methods: missing inputs fail loudly", {
  S <- Matrix::Diagonal(4)
  expect_error(er_cluster(S, method = "logistic", truth_vec = c(1L, 1L, 2L, 2L)),
               "sim_list and pairs")
  expect_error(er_cluster(S, method = "logistic"), "truth_vec")
  expect_error(er_cluster(S, method = "svm"), "match.arg|should be one of")
})

test_that("er_cluster logistic end-to-end on a tiny dedup problem", {
  if (!requireNamespace("brglm2", quietly = TRUE))
    skip("brglm2 not installed")
  df <- data.frame(
    name = c("apple inc", "apple incorporated", "apple corp",
             "banana ltd", "banana limited", "cherry co"),
    city = c("new york", "new york", "boston",
             "austin", "austin", "denver"),
    stringsAsFactors = FALSE)
  truth_vec <- c(1L, 1L, 1L, 2L, 2L, 3L)
  pairs <- er_block(df, method = "none")
  spec <- list(list(name = "name", type = "jw"),
               list(name = "city", type = "jw"))
  sim <- er_similarity(df, pairs, spec = spec)
  cmb <- er_combine(sim)
  S <- er_pairs_to_sparse(pairs, cmb, n = nrow(df))
  labs <- er_cluster(S, method = "logistic",
                     sim_list = sim, pairs = pairs, truth_vec = truth_vec)
  expect_equal(length(labs), nrow(df))
  expect_true(is.integer(labs))
  # the banana pair is the most match-like in the data: identical city,
  # near-identical name -> must be clustered together
  expect_equal(labs[4], labs[5])
  # the cherry singleton shares nothing with the rest -> stays apart
  expect_false(labs[6] %in% labs[1:5])
})

test_that("er_cluster falls back with a warning when the classifier is invalid", {
  if (!requireNamespace("MASS", quietly = TRUE))
    skip("MASS not installed")
  # identical within-entity names -> the TRUE class has zero variance ->
  # qda is structurally inapplicable -> louvain fallback
  df <- data.frame(name = c("aa", "aa", "bb", "bb"),
                   stringsAsFactors = FALSE)
  truth_vec <- c(1L, 1L, 2L, 2L)
  pairs <- er_block(df, method = "none")
  spec <- list(list(name = "name", type = "jw"))
  sim <- er_similarity(df, pairs, spec = spec)
  S <- er_pairs_to_sparse(pairs, er_combine(sim), n = nrow(df))
  expect_warning(
    labs <- er_cluster(S, method = "qda",
                       sim_list = sim, pairs = pairs, truth_vec = truth_vec),
    "Falling back to louvain"
  )
  expect_equal(length(labs), nrow(df))
})
