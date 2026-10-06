# 24-supervised_classifiers.R
#
# Supervised pairwise classifiers for entity resolution.
#
# This module ports the advisor's 11-family classifier benchmark into the
# package as single-fit, well-disciplined building blocks. Each classifier maps
# pair features (per-field similarities, one row per candidate pair) to a
# match probability P(match | pair).
#
# Design notes (advisor's methodology, preserved):
#  * Structural inapplicability (e.g. logistic non-convergence under complete
#    separation, QDA rank-deficient class covariances) is REPORTED via
#    valid = FALSE + invalid_reason, never silently returned as garbage and
#    never a hard crash. This is an applicability exclusion, not a performance
#    judgement.
#  * logistic defaults to Firth's penalized likelihood ("firth"), which stays
#    finite under complete separation where plain glm() diverges. Plain glm()
#    remains available via logistic_method = "glm".
#  * KNN train probabilities are leave-one-out style (FNN::get.knn excludes
#    the query point itself), as in the advisor's implementation.
#  * These classifiers are SUPERVISED: they train on truth-derived pair
#    labels. Do not use truth-driven classifier selection for reporting;
#    see er_tune() / the honest-evaluation protocol for model selection.

#' Names of the supervised pairwise classifiers
#'
#' The eleven classifier families ported from the advisor's benchmark.
#' @return Character vector of classifier names.
#' @export
er_supervised_classifiers <- function() {
  c("logistic", "lda", "qda", "knn", "wknn", "tree",
    "rf", "xgboost", "nnet", "fellegi_sunter", "svm_radial")
}

# ── Pair features ─────────────────────────────────────────────────────────────

#' Build the pair-feature matrix from per-field similarities
#'
#' Each row is one candidate pair, each column one field's similarity score.
#' This is the feature space the supervised classifiers train on.
#'
#' @param sim_list Named list of numeric vectors, one per field, each of length
#'   \code{nrow(pairs)} (output of \code{er_similarity}).
#' @param pairs Data frame with integer columns \code{idx1}, \code{idx2}
#'   (output of the blocking step).
#' @return A data frame with one row per pair and one column per field.
#' @export
er_pair_features <- function(sim_list, pairs) {
  if (is.null(sim_list) || !length(sim_list))
    stop("er_pair_features: sim_list must be a non-empty named list.")
  if (is.null(pairs) || !all(c("idx1", "idx2") %in% names(pairs)))
    stop("er_pair_features: pairs must have idx1/idx2 columns.")
  n <- nrow(pairs)
  for (nm in names(sim_list)) {
    v <- sim_list[[nm]]
    if (length(v) != n)
      stop("er_pair_features: sim_list[[", nm, "]] has length ", length(v),
           " but pairs has ", n, " rows.")
  }
  as.data.frame(lapply(sim_list, function(v) as.numeric(v)),
                check.names = FALSE)
}

# ── Internal helpers (ported from the advisor's module) ──────────────────────

.make_design_matrix <- function(x, use_interactions = TRUE) {
  x <- as.data.frame(x, check.names = FALSE)
  if (!use_interactions || ncol(x) == 1L) return(as.matrix(x))
  quoted <- paste0("`", gsub("`", "", names(x), fixed = TRUE), "`")
  formula <- stats::as.formula(paste("~ (", paste(quoted, collapse = " + "), ")^2"))
  design <- stats::model.matrix(formula, data = x)
  design[, colnames(design) != "(Intercept)", drop = FALSE]
}

.fit_scaler <- function(x) {
  center <- colMeans(x)
  scale <- apply(x, 2L, stats::sd)
  scale[!is.finite(scale) | scale == 0] <- 1
  list(center = center, scale = scale)
}

.apply_scaler <- function(x, scaler) {
  sweep(sweep(x, 2L, scaler$center, "-"), 2L, scaler$scale, "/")
}

.clamp_probability <- function(x) {
  pmin(1, pmax(0, as.numeric(x)))
}

# Advisor's logistic validity checks: non-convergence / aliased coefficients /
# rank deficiency / boundary solutions all mean "structurally inapplicable".
.logistic_invalid_reason <- function(fit) {
  if (!isTRUE(fit$converged)) {
    return("Logistic GLM did not converge; the fit is excluded from model selection.")
  }
  coefs <- fit$coefficients
  if (!length(coefs) || any(!is.finite(coefs))) {
    return("Logistic GLM has aliased or non-finite coefficients; the fit is excluded from model selection.")
  }
  if (is.null(fit$rank) || fit$rank < length(coefs)) {
    return("Logistic GLM design matrix is rank deficient; the fit is excluded from model selection.")
  }
  if (isTRUE(fit$boundary)) {
    return("Logistic GLM ended on a boundary solution; the fit is excluded from model selection.")
  }
  ""
}

# Firth's penalized likelihood stays finite under complete separation, so the
# only exclusions here are genuine fitting failures, not separation.
.firth_logistic_invalid_reason <- function(fit) {
  if (!isTRUE(fit$converged)) {
    return("Firth-penalized logistic regression did not converge; the fit is excluded from model selection.")
  }
  coefs <- fit$coefficients
  if (!length(coefs) || any(!is.finite(coefs))) {
    return("Firth-penalized logistic regression has non-finite coefficients; the fit is excluded from model selection.")
  }
  ""
}

# Columns with usable pooled within-class variance, then a full-rank subset
# (QR pivot). Simplified port of the advisor's discriminant column selection.
.discriminant_columns <- function(x, y, require_each_group = FALSE, tol = 1e-4) {
  x <- as.matrix(x)
  y <- droplevels(factor(y))
  groups <- lapply(levels(y), function(lv) which(y == lv))
  keep <- vapply(seq_len(ncol(x)), function(j) {
    vals <- lapply(groups, function(rows) x[rows, j])
    if (any(lengths(vals) < 2L)) return(FALSE)
    vars <- vapply(vals, stats::var, numeric(1))
    if (require_each_group) {
      all(is.finite(vars) & sqrt(pmax(0, vars)) >= tol)
    } else {
      counts <- lengths(vals)
      pooled <- sum((counts - 1L) * vars) / sum(counts - 1L)
      is.finite(pooled) && sqrt(max(0, pooled)) >= tol
    }
  }, logical(1))
  cols <- which(keep)
  if (!length(cols)) return(integer())
  # Full-rank subset, preserving source-column order (advisor's QR pivot).
  centered <- x[, cols, drop = FALSE]
  for (lv in levels(y)) {
    rows <- which(y == lv)
    centered[rows, ] <- sweep(centered[rows, , drop = FALSE], 2L,
                             colMeans(centered[rows, , drop = FALSE]), "-")
  }
  qr_dec <- qr(centered, tol = 1e-7)
  if (qr_dec$rank < 1L) return(integer())
  sort(cols[qr_dec$pivot[seq_len(qr_dec$rank)]])
}

# Single-k KNN with inverse-distance weighting option.
# Train probabilities use FNN::get.knn (self-search excludes the query point),
# giving leave-one-out-style scores as in the advisor's implementation.
.knn_train_prob <- function(x_scaled, y, k, weighted) {
  knn <- FNN::get.knn(x_scaled, k = k)
  .knn_prob_from_neighbors(knn$nn.index, knn$nn.dist, y, k, weighted)
}

.knn_predict_prob <- function(x_train_scaled, y, x_new_scaled, k, weighted) {
  knn <- FNN::get.knnx(x_train_scaled, x_new_scaled, k = k)
  .knn_prob_from_neighbors(knn$nn.index, knn$nn.dist, y, k, weighted)
}

.knn_prob_from_neighbors <- function(nn_index, nn_dist, y, k, weighted) {
  y <- factor(y, levels = c(FALSE, TRUE))
  pos <- y[nn_index] == "TRUE"
  if (!weighted) {
    return(.clamp_probability(rowSums(pos) / k))
  }
  w <- 1 / (nn_dist + 1e-16)
  w_pos <- rowSums(w * pos)
  w_tot <- rowSums(w)
  w_tot[w_tot == 0] <- 1
  .clamp_probability(w_pos / w_tot)
}

# Supervised Fellegi-Sunter: per-field binned log-likelihood ratios plus the
# log prior odds. Ported from the advisor's module.
.fs_fit <- function(x, y, bins = 5L, laplace = 0.5) {
  if (bins < 2L) stop("Fellegi-Sunter bins must be at least 2.")
  if (laplace <= 0) stop("Fellegi-Sunter Laplace smoothing must be positive.")
  y_match <- as.logical(as.character(y))
  prior <- mean(y_match)
  if (prior <= 0 || prior >= 1) stop("Fellegi-Sunter requires both classes.")
  x <- as.data.frame(x, check.names = FALSE)
  field_weights <- vector("list", ncol(x))
  names(field_weights) <- names(x)
  for (j in seq_len(ncol(x))) {
    bin_index <- pmin(bins, floor(.clamp_probability(x[[j]]) * bins) + 1L)
    m_count <- tabulate(bin_index[y_match], nbins = bins) + laplace
    u_count <- tabulate(bin_index[!y_match], nbins = bins) + laplace
    field_weights[[j]] <- log((m_count / sum(m_count)) / (u_count / sum(u_count)))
  }
  list(bins = bins, log_prior_odds = stats::qlogis(prior),
       field_weights = field_weights, fields = names(x))
}

.fs_predict <- function(model, newdata) {
  newdata <- as.data.frame(newdata, check.names = FALSE)[, model$fields, drop = FALSE]
  score <- rep(model$log_prior_odds, nrow(newdata))
  for (j in seq_along(model$fields)) {
    bin_index <- pmin(model$bins,
                      floor(.clamp_probability(newdata[[j]]) * model$bins) + 1L)
    score <- score + model$field_weights[[j]][bin_index]
  }
  .clamp_probability(stats::plogis(score))
}

# Chunked prediction for large pair sets (advisor's prediction_chunk_size).
.predict_chunked <- function(predict_fn, newdata, chunk_size = 100000L) {
  n <- nrow(newdata)
  if (n <= chunk_size) return(.clamp_probability(predict_fn(newdata)))
  out <- numeric(n)
  starts <- seq(1L, n, by = chunk_size)
  for (s in starts) {
    e <- min(n, s + chunk_size - 1L)
    out[s:e] <- .clamp_probability(predict_fn(newdata[s:e, , drop = FALSE]))
  }
  out
}

# Normalize pair labels to a two-level factor with levels FALSE/TRUE.
# Accepts logical, numeric 0/1, or strings like "0"/"1"/"true"/"false".
.as_binary_factor <- function(y) {
  if (is.logical(y)) {
    yn <- y
  } else if (is.numeric(y)) {
    if (anyNA(y) || !all(y %in% c(0, 1)))
      stop("er_pair_classify: numeric y must contain only 0 and 1.")
    yn <- y == 1
  } else {
    v <- tolower(trimws(as.character(y)))
    yn <- ifelse(v %in% c("1", "true", "t", "yes"), TRUE,
          ifelse(v %in% c("0", "false", "f", "no"), FALSE, NA))
  }
  if (anyNA(yn))
    stop("er_pair_classify: y must be binary (0/1 or TRUE/FALSE), without NA.")
  yf <- factor(yn, levels = c(FALSE, TRUE))
  if (nlevels(droplevels(yf)) < 2L)
    stop("er_pair_classify: y must contain both classes.")
  yf
}

.require_pkg <- function(pkg, what) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(what, " requires package '", pkg, "'. ",
         "Install it with install.packages(\"", pkg, "\"), then retry.",
         call. = FALSE)
  }
}

# Extra (non-base) package each classifier needs; "" = none beyond the
# package's Imports. Used to prune "all" in er_cluster_all().
.classifier_pkg <- function(classifier, logistic_method = "firth") {
  switch(classifier,
         logistic = if (identical(logistic_method, "firth")) "brglm2" else "",
         lda = "MASS", qda = "MASS",
         knn = "FNN", wknn = "FNN",
         tree = "rpart", rf = "randomForest",
         xgboost = "xgboost", nnet = "nnet",
         fellegi_sunter = "",
         svm_radial = "e1071",
         "")
}

# ── Main entry point ─────────────────────────────────────────────────────────

#' Fit one supervised pairwise classifier
#'
#' Fits a single classifier from the advisor's eleven families on pair features
#' and returns a reusable match-probability predictor. Structural
#' inapplicability (e.g. logistic non-convergence under complete separation,
#' rank-deficient QDA covariances) is reported as \code{valid = FALSE} with a
#' human-readable \code{invalid_reason} rather than a crash or silent garbage.
#'
#' @param features Data frame or matrix: one row per pair, one column per
#'   field similarity (see \code{er_pair_features}).
#' @param y Pair labels: 0/1, TRUE/FALSE, or a two-level factor. Both classes
#'   must be present.
#' @param classifier One of \code{\link{er_supervised_classifiers}()}.
#' @param use_interactions Logical. Add pairwise interaction terms to the
#'   design matrix (logistic, xgboost). Default TRUE.
#' @param logistic_method \code{"firth"} (default) or \code{"glm"}. Firth's
#'   penalized likelihood stays finite under complete separation, where plain
#'   \code{glm()} diverges.
#' @param knn_k Integer. Number of neighbors for \code{"knn"}/\code{"wknn"}.
#' @param tree_cp,tree_maxdepth Complexity and depth for \code{"tree"}.
#' @param rf_mtry \code{"sqrt"} or \code{"all"}; \code{rf_ntree} trees for
#'   \code{"rf"}.
#' @param xgb_eta,xgb_max_depth,xgb_nrounds XGBoost hyperparameters.
#' @param nnet_size,nnet_decay Neural-net hyperparameters.
#' @param fs_bins,fs_laplace Fellegi-Sunter bins and Laplace smoothing.
#' @param svm_cost,svm_gamma Radial-SVM hyperparameters.
#' @param seed Integer seed for stochastic fits (rf, xgboost, nnet, svm).
#'   The caller's RNG state is preserved.
#' @param chunk_size Rows per prediction chunk for large pair sets.
#' @return A list with:
#'   \itemize{
#'     \item \code{classifier}: the classifier name.
#'     \item \code{valid}: TRUE if the fit is usable.
#'     \item \code{invalid_reason}: human-readable reason when
#'       \code{valid = FALSE}, else \code{""}.
#'     \item \code{train_prob}: fitted match probabilities on the training
#'       pairs (NULL when invalid).
#'     \item \code{predict}: function(newdata) returning match probabilities
#'       in [0, 1] (NULL when invalid).
#'     \item \code{params}: the hyperparameters actually used.
#'   }
#' @export
er_pair_classify <- function(features, y,
                             classifier = c("logistic", "lda", "qda", "knn",
                                            "wknn", "tree", "rf", "xgboost",
                                            "nnet", "fellegi_sunter",
                                            "svm_radial"),
                             use_interactions = TRUE,
                             logistic_method = c("firth", "glm"),
                             knn_k = 5L,
                             tree_cp = 0.01, tree_maxdepth = 10L,
                             rf_mtry = c("sqrt", "all"), rf_ntree = 500L,
                             xgb_eta = 0.1, xgb_max_depth = 4L,
                             xgb_nrounds = 100L,
                             nnet_size = 5L, nnet_decay = 0.01,
                             fs_bins = 5L, fs_laplace = 0.5,
                             svm_cost = 1, svm_gamma = 0.5,
                             seed = 1L,
                             chunk_size = 100000L) {
  classifier <- match.arg(classifier)
  logistic_method <- match.arg(logistic_method)
  rf_mtry <- match.arg(rf_mtry)

  x <- as.data.frame(features, check.names = FALSE)
  if (!nrow(x) || !ncol(x))
    stop("er_pair_classify: features must have at least one row and column.")
  y <- .as_binary_factor(y)
  y_num <- as.integer(y == "TRUE")

  invalid <- function(reason) {
    list(classifier = classifier, valid = FALSE, invalid_reason = reason,
         train_prob = NULL, predict = NULL,
         params = list(use_interactions = use_interactions,
                       logistic_method = logistic_method))
  }
  ok <- function(train_prob, predict_fn, params) {
    list(classifier = classifier, valid = TRUE, invalid_reason = "",
         train_prob = .clamp_probability(train_prob),
         predict = function(newdata) {
           .predict_chunked(predict_fn,
                            as.data.frame(newdata, check.names = FALSE),
                            chunk_size)
         },
         params = params)
  }
  # Advisor's safely_fit: unexpected fitting errors become invalid results
  # with the error message, not crashes.
  guarded <- function(expr) {
    tryCatch(expr, error = function(e) invalid(conditionMessage(e)))
  }

  x_interaction <- .make_design_matrix(x, use_interactions)
  x_base <- as.matrix(x)

  if (classifier == "logistic") {
    use_firth <- identical(logistic_method, "firth")
    if (use_firth) .require_pkg("brglm2", "logistic_method = \"firth\"")
    guarded({
      train_df <- as.data.frame(x_interaction, check.names = TRUE)
      train_df$outcome <- y_num
      fit <- if (use_firth) {
        stats::glm(outcome ~ ., data = train_df, family = stats::binomial(),
                   method = brglm2::brglmFit, type = "AS_mean")
      } else {
        stats::glm(outcome ~ ., data = train_df, family = stats::binomial())
      }
      reason <- if (use_firth) .firth_logistic_invalid_reason(fit)
                else .logistic_invalid_reason(fit)
      if (nzchar(reason)) return(invalid(reason))
      train_prob <- stats::predict(fit, type = "response")
      fit0 <- fit; ui0 <- use_interactions
      pred <- function(newdata) {
        design <- .make_design_matrix(newdata, ui0)
        stats::predict(fit0,
                       newdata = as.data.frame(design, check.names = TRUE),
                       type = "response")
      }
      ok(train_prob, pred,
         list(use_interactions = use_interactions,
              logistic_method = logistic_method))
    })
  } else if (classifier == "lda") {
    .require_pkg("MASS", "lda")
    guarded({
      cols <- .discriminant_columns(x_interaction, y)
      if (!length(cols))
        return(invalid("LDA: no predictor has usable pooled within-class variance."))
      lda_x <- x_interaction[, cols, drop = FALSE]
      fit <- MASS::lda(x = lda_x, grouping = y)
      train_prob <- predict(fit, lda_x)$posterior[, "TRUE"]
      fit0 <- fit; cols0 <- cols; ui0 <- use_interactions
      pred <- function(newdata) {
        design <- .make_design_matrix(newdata, ui0)
        predict(fit0, design[, cols0, drop = FALSE])$posterior[, "TRUE"]
      }
      ok(train_prob, pred, list(retained_predictors = colnames(lda_x)))
    })
  } else if (classifier == "qda") {
    .require_pkg("MASS", "qda")
    guarded({
      cols <- .discriminant_columns(x_interaction, y, require_each_group = TRUE)
      reason <- ""
      if (!length(cols)) {
        reason <- "QDA: no predictor has usable variance within each outcome class."
      } else if (min(table(y)) <= length(cols)) {
        reason <- paste0("QDA: minority-class pair count ", min(table(y)),
                         " is not greater than the ", length(cols),
                         " predictors required for class-specific covariance estimation.")
      } else {
        qda_x <- x_interaction[, cols, drop = FALSE]
        full_rank <- vapply(levels(y), function(lv) {
          gx <- qda_x[y == lv, , drop = FALSE]
          qr(scale(gx, center = TRUE, scale = FALSE))$rank == ncol(qda_x)
        }, logical(1))
        if (!all(full_rank))
          reason <- "QDA: at least one outcome class has a rank-deficient predictor covariance matrix."
      }
      if (nzchar(reason)) return(invalid(reason))
      qda_x <- x_interaction[, cols, drop = FALSE]
      fit <- MASS::qda(x = qda_x, grouping = y)
      train_prob <- predict(fit, qda_x)$posterior[, "TRUE"]
      fit0 <- fit; cols0 <- cols; ui0 <- use_interactions
      pred <- function(newdata) {
        design <- .make_design_matrix(newdata, ui0)
        predict(fit0, design[, cols0, drop = FALSE])$posterior[, "TRUE"]
      }
      ok(train_prob, pred, list(retained_predictors = colnames(qda_x)))
    })
  } else if (classifier %in% c("knn", "wknn")) {
    .require_pkg("FNN", classifier)
    guarded({
      k <- as.integer(knn_k)
      if (length(k) != 1L || is.na(k) || k < 1L)
        stop("knn_k must be one positive integer.")
      if (k >= nrow(x_base))
        stop("knn_k must be smaller than the training-pair count.")
      weighted <- identical(classifier, "wknn")
      scaler <- .fit_scaler(x_base)
      x_scaled <- .apply_scaler(x_base, scaler)
      train_prob <- .knn_train_prob(x_scaled, y, k, weighted)
      xs0 <- x_scaled; y0 <- y; sc0 <- scaler
      pred <- function(newdata) {
        xn <- .apply_scaler(as.matrix(as.data.frame(newdata,
                                                    check.names = FALSE)), sc0)
        .knn_predict_prob(xs0, y0, xn, k, weighted)
      }
      ok(train_prob, pred, list(k = k, weighted = weighted))
    })
  } else if (classifier == "tree") {
    .require_pkg("rpart", "tree")
    guarded({
      train_df <- data.frame(match = y, x, check.names = TRUE)
      fit <- rpart::rpart(match ~ ., data = train_df, method = "class",
                          control = rpart::rpart.control(cp = tree_cp,
                                                         maxdepth = tree_maxdepth))
      train_prob <- predict(fit, type = "prob")[, "TRUE"]
      fit0 <- fit
      pred <- function(newdata) {
        predict(fit0, newdata = as.data.frame(newdata, check.names = TRUE),
                type = "prob")[, "TRUE"]
      }
      ok(train_prob, pred, list(cp = tree_cp, maxdepth = tree_maxdepth))
    })
  } else if (classifier == "rf") {
    .require_pkg("randomForest", "rf")
    guarded({
      mtry_value <- if (rf_mtry == "sqrt") max(1L, floor(sqrt(ncol(x_base))))
                    else ncol(x_base)
      .rng_restore <- .rng_save()
      on.exit(.rng_restore(), add = TRUE)
      set.seed(seed)
      fit <- randomForest::randomForest(x = x_base, y = y,
                                       mtry = mtry_value, ntree = rf_ntree)
      train_prob <- predict(fit, type = "prob")[, "TRUE"]
      fit0 <- fit
      pred <- function(newdata) {
        predict(fit0, newdata = as.matrix(as.data.frame(newdata,
                                                        check.names = FALSE)),
                type = "prob")[, "TRUE"]
      }
      ok(train_prob, pred, list(mtry = mtry_value, ntree = rf_ntree))
    })
  } else if (classifier == "xgboost") {
    .require_pkg("xgboost", "xgboost")
    guarded({
      dtrain <- xgboost::xgb.DMatrix(data = x_interaction, label = y_num)
      .rng_restore <- .rng_save()
      on.exit(.rng_restore(), add = TRUE)
      set.seed(seed)
      fit <- xgboost::xgb.train(
        params = list(objective = "binary:logistic", eval_metric = "logloss",
                      eta = xgb_eta, max_depth = xgb_max_depth, nthread = 1L),
        data = dtrain, nrounds = xgb_nrounds, verbose = 0L)
      train_prob <- predict(fit, dtrain)
      fit0 <- fit; ui0 <- use_interactions
      pred <- function(newdata) {
        design <- .make_design_matrix(newdata, ui0)
        predict(fit0, xgboost::xgb.DMatrix(design))
      }
      ok(train_prob, pred,
         list(eta = xgb_eta, max_depth = xgb_max_depth, nrounds = xgb_nrounds))
    })
  } else if (classifier == "nnet") {
    .require_pkg("nnet", "nnet")
    guarded({
      scaler <- .fit_scaler(x_interaction)
      x_scaled <- .apply_scaler(x_interaction, scaler)
      .rng_restore <- .rng_save()
      on.exit(.rng_restore(), add = TRUE)
      set.seed(seed)
      fit <- nnet::nnet(x = x_scaled, y = y_num, size = nnet_size,
                       decay = nnet_decay, entropy = TRUE, maxit = 500L,
                       MaxNWts = max(1000L, (ncol(x_scaled) + 2L) *
                                       nnet_size * 5L),
                       trace = FALSE)
      train_prob <- as.numeric(predict(fit, x_scaled, type = "raw"))
      fit0 <- fit; ui0 <- use_interactions; sc0 <- scaler
      pred <- function(newdata) {
        design <- .make_design_matrix(newdata, ui0)
        as.numeric(predict(fit0, .apply_scaler(design, sc0), type = "raw"))
      }
      ok(train_prob, pred,
         list(size = nnet_size, decay = nnet_decay, maxit = 500L))
    })
  } else if (classifier == "fellegi_sunter") {
    guarded({
      fit <- .fs_fit(x, y, bins = fs_bins, laplace = fs_laplace)
      train_prob <- .fs_predict(fit, x)
      fit0 <- fit
      pred <- function(newdata) .fs_predict(fit0, newdata)
      ok(train_prob, pred, list(bins = fs_bins, laplace = fs_laplace))
    })
  } else if (classifier == "svm_radial") {
    .require_pkg("e1071", "svm_radial")
    guarded({
      scaler <- .fit_scaler(x_interaction)
      x_scaled <- .apply_scaler(x_interaction, scaler)
      .rng_restore <- .rng_save()
      on.exit(.rng_restore(), add = TRUE)
      set.seed(seed)
      fit <- e1071::svm(x = x_scaled, y = y, kernel = "radial",
                        cost = svm_cost, gamma = svm_gamma,
                        probability = TRUE, scale = FALSE)
      prob_from_fit <- function(fit_obj, mat) {
        pr <- predict(fit_obj, mat, probability = TRUE)
        pb <- attr(pr, "probabilities")
        if (is.null(pb) || !"TRUE" %in% colnames(pb))
          stop("SVM did not return the TRUE-class probability.")
        pb[, "TRUE"]
      }
      train_prob <- prob_from_fit(fit, x_scaled)
      fit0 <- fit; ui0 <- use_interactions; sc0 <- scaler
      pred <- function(newdata) {
        design <- .make_design_matrix(newdata, ui0)
        prob_from_fit(fit0, .apply_scaler(design, sc0))
      }
      ok(train_prob, pred, list(cost = svm_cost, gamma = svm_gamma))
    })
  }
}
