########################################
# File: R/07-cluster.R
# Unified clustering interface.
# er_cluster()     -- single method, takes combined sparse matrix S
# er_cluster_all() -- all methods, returns named list
#
# Supported methods:
#   hclust_avg    HC with average linkage on cosine distance (stats)
#   hclust_ward   HC with Ward.D2 linkage on Euclidean distance (stats)
#   pam           Partition Around Medoids (cluster)
#   threshold_cc  Connected components at similarity threshold (igraph)
#   louvain       Louvain community detection (igraph)
#   leiden        Leiden community detection (igraph + leidenbase)
#   label_prop    Label Propagation (igraph)
#   gc            Graph Coloring (GCMER, optional)
########################################

# ── k-selection helpers ────────────────────────────────────────────────────────

#' Tune the number of clusters k for centroidal methods
#'
#' Sweeps \code{k_grid}, evaluating each k by ARI against \code{truth_vec}
#' (supervised) or by average silhouette width (unsupervised), and returns the
#' best k.
#'
#' @param X Numeric feature matrix (rows = records).
#' @param k_grid Integer vector of candidate k values.
#' @param truth_vec Optional integer vector of ground-truth labels (length n).
#' @param method Character. Clustering method name (used to select the right
#'   linkage for HC vs PAM).
#' @param tune_metric Character. Ignored (ARI used for supervised tuning).
#' @return Integer scalar: the best k found.
#' @keywords internal
.tune_k <- function(X, k_grid, truth_vec = NULL, method = "hclust_avg",
                    tune_metric = "adj_rand") {
  k_grid <- sort(unique(k_grid[k_grid >= 2L & k_grid < nrow(X)]))
  if (!length(k_grid)) return(2L)

  if (!is.null(truth_vec)) {
    # Supervised: pick k maximising tune_metric
    scores <- vapply(k_grid, function(k) {
      labs <- if (method == "pam") {
        D <- er_cosine_dist(X)
        cluster::pam(stats::as.dist(D), k = k)$clustering
      } else if (method == "hclust_ward") {
        D <- stats::dist(X)   # Euclidean — Ward.D2 requires Euclidean geometry
        stats::cutree(stats::hclust(D, method = "ward.D2"), k = k)
      } else {
        D <- er_cosine_dist(X)
        stats::cutree(stats::hclust(stats::as.dist(D), method = "average"), k = k)
      }
      if (requireNamespace("GCMER", quietly = TRUE))
        tryCatch(GCMER::adj_rand(labs, truth_vec), error = function(e) -Inf)
      else -Inf
    }, numeric(1L))
    k_grid[which.max(scores)]
  } else {
    # Unsupervised: silhouette
    if (method == "hclust_ward") {
      D  <- stats::dist(X)   # Euclidean — Ward.D2 requires Euclidean geometry
      scores <- vapply(k_grid, function(k) {
        labs <- stats::cutree(stats::hclust(D, method = "ward.D2"), k = k)
        er_silhouette_avg(labs, D)
      }, numeric(1L))
    } else {
      D  <- er_cosine_dist(X)
      scores <- vapply(k_grid, function(k) {
        labs <- stats::cutree(stats::hclust(stats::as.dist(D), method = "average"), k = k)
        er_silhouette_avg(labs, D)
      }, numeric(1L))
    }
    scores[!is.finite(scores)] <- -Inf
    k_grid[which.max(scores)]
  }
}

# ── Centroidal clustering from sparse S ────────────────────────────────────────

#' Derive a dense feature matrix from a sparse similarity matrix via SVD
#'
#' Used by centroidal clustering methods (HC, PAM) that require a feature or
#' distance matrix rather than a graph.
#'
#' @param S Symmetric \code{dgCMatrix} (n × n).
#' @param svd_dim Integer. Number of SVD dimensions to retain.
#' @return Numeric matrix (n × \code{svd_dim}).
#' @keywords internal
.features_from_S <- function(S, svd_dim = 50L) {
  n <- nrow(S)
  d <- min(svd_dim, n - 1L, ncol(S) - 1L)
  if (d < 2L) return(matrix(0, n, 2))
  tryCatch({
    res <- irlba::irlba(S, nv = d)
    res$u %*% diag(res$d, nrow = d, ncol = d)
  }, error = function(e) {
    # fallback: random projection
    set.seed(42L); matrix(stats::rnorm(n * 2L), n, 2L)
  })
}

# ── Pairwise feature helpers (for supervised methods) ─────────────────────────

#' Build a feature matrix over all candidate pairs in S
#'
#' For each off-diagonal non-zero pair (i,j) in S, the feature vector is
#' \code{abs(X[i,] - X[j,])}, giving a record of element-wise absolute
#' differences in the SVD embedding space.
#'
#' @param S Symmetric \code{dgCMatrix}.
#' @param X Numeric feature matrix (n x d).
#' @return List with \code{feat} (matrix), \code{i}, \code{j} (integer vectors).
#' @keywords internal
.pair_features <- function(S, X) {
  E <- Matrix::summary(S)
  E <- E[E$i < E$j, , drop = FALSE]
  if (!nrow(E))
    return(list(feat = matrix(0, 0, ncol(X)), i = integer(), j = integer()))
  feat <- abs(X[E$i, , drop = FALSE] - X[E$j, , drop = FALSE])
  list(feat = feat, i = E$i, j = E$j)
}

#' Extract binary match labels for pairs from a truth vector
#' @param i_vec Integer vector of first-record indices.
#' @param j_vec Integer vector of second-record indices (same length as \code{i_vec}).
#' @param truth_vec Integer vector of ground-truth entity labels (length = n records).
#' @return Integer vector of 0/1 match labels; NA in either truth entry yields 0.
#' @keywords internal
.pair_labels <- function(i_vec, j_vec, truth_vec) {
  as.integer(
    !is.na(truth_vec[i_vec]) & !is.na(truth_vec[j_vec]) &
      truth_vec[i_vec] == truth_vec[j_vec]
  )
}

#' Rebuild a sparse similarity matrix from predicted pair probabilities
#' @param i_vec Integer vector of first-record indices.
#' @param j_vec Integer vector of second-record indices.
#' @param probs Numeric vector of match probabilities in \eqn{[0,1]}.
#' @param n Integer. Total number of records (matrix dimension).
#' @return Symmetric \code{dgCMatrix} of size \eqn{n \times n}.
#' @keywords internal
.probs_to_sparse <- function(i_vec, j_vec, probs, n) {
  Matrix::sparseMatrix(
    i    = c(i_vec, j_vec),
    j    = c(j_vec, i_vec),
    x    = c(probs, probs),
    dims = c(n, n)
  )
}

# ── er_cluster() ──────────────────────────────────────────────────────────────

#' Run a single clustering method
#'
#' Applies one clustering method to the combined sparse similarity matrix
#' \code{S}.  Centroidal methods (HC, PAM) derive features from \code{S} via
#' truncated SVD when no feature matrix \code{X} is provided.
#'
#' @param S A symmetric \code{dgCMatrix} (n × n) with diagonal 1, produced by
#'   \code{er_pairs_to_sparse()}.
#' @param method Character. One of \code{"hclust_avg"}, \code{"hclust_ward"},
#'   \code{"pam"}, \code{"threshold_cc"}, \code{"louvain"}, \code{"leiden"},
#'   \code{"label_prop"}, \code{"gc"}, \code{"svm"}, \code{"gbm"}.
#'   \code{"svm"} and \code{"gbm"} are supervised pairwise classifiers:
#'   they require \code{truth_vec} for training and packages \pkg{e1071}
#'   (SVM) or \pkg{xgboost} (GBM).
#' @param k Integer. Number of clusters for centroidal methods. If
#'   \code{NULL}, tuned automatically.
#' @param k_grid Integer vector. Values of k to sweep during auto-tuning.
#'   Default \code{c(5, 10, 15, 20, 30, 50)}.
#' @param threshold Numeric in \eqn{[0,1]}. Similarity cut-off for
#'   \code{"threshold_cc"} and \code{"gc"}. Default \code{0.5}.
#' @param resolution Numeric. Louvain/Leiden resolution. Default \code{1}.
#' @param X Optional feature matrix (n × d) for centroidal methods.  If
#'   \code{NULL}, derived from \code{S} via SVD.
#' @param svd_dim Integer. Number of SVD dimensions when deriving \code{X}
#'   from \code{S}. Default \code{50}.
#' @param truth_vec Optional integer vector of ground-truth labels (length n)
#'   for supervised k-tuning.
#' @param tune_metric Character. Metric to maximise during supervised tuning.
#'   Default \code{"adj_rand"}.
#'
#' @return Integer vector of cluster labels (length n).
#' @export
er_cluster <- function(S, method,
                       k           = NULL,
                       k_grid      = c(5L, 10L, 15L, 20L, 30L, 50L),
                       threshold   = 0.5,
                       resolution  = 1,
                       X           = NULL,
                       svd_dim     = 50L,
                       truth_vec   = NULL,
                       tune_metric = "adj_rand") {

  method <- match.arg(method, c("hclust_avg", "hclust_ward", "pam",
                                 "threshold_cc", "louvain", "leiden",
                                 "label_prop", "gc", "svm", "gbm"))
  n <- nrow(S)
  if (n < 2L) return(rep(1L, n))

  # ── Graph-based methods: work directly on S ──────────────────────────────
  if (method == "threshold_cc") {
    E <- Matrix::summary(S)
    E <- E[E$i < E$j & E$x >= threshold, , drop = FALSE]
    if (!nrow(E)) return(seq_len(n))
    g   <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                          vertices = data.frame(name = seq_len(n)))
    return(as.integer(igraph::components(g)$membership))
  }

  if (method == "louvain") {
    E <- Matrix::summary(S)
    E <- E[E$i < E$j & is.finite(E$x) & E$x > 0, , drop = FALSE]
    if (!nrow(E)) return(rep(1L, n))
    g   <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                          vertices = data.frame(name = seq_len(n)))
    igraph::E(g)$weight <- E$x
    cl  <- igraph::cluster_louvain(g, weights = igraph::E(g)$weight,
                                    resolution = resolution)
    memb <- rep(NA_integer_, n)
    memb[as.integer(igraph::V(g)$name)] <- as.integer(igraph::membership(cl))
    memb[is.na(memb)] <- max(memb, na.rm = TRUE) + seq_len(sum(is.na(memb)))
    return(memb)
  }

  if (method == "leiden") {
    if (!requireNamespace("igraph", quietly = TRUE))
      stop("leiden requires igraph.")
    E <- Matrix::summary(S)
    E <- E[E$i < E$j & is.finite(E$x) & E$x > 0, , drop = FALSE]
    if (!nrow(E)) return(rep(1L, n))
    g <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                        vertices = data.frame(name = seq_len(n)))
    igraph::E(g)$weight <- E$x
    cl <- tryCatch(
      igraph::cluster_leiden(g, weights = igraph::E(g)$weight,
                              resolution_parameter = resolution),
      error = function(e) igraph::cluster_louvain(g, weights = igraph::E(g)$weight)
    )
    memb <- rep(NA_integer_, n)
    memb[as.integer(igraph::V(g)$name)] <- as.integer(igraph::membership(cl))
    memb[is.na(memb)] <- max(memb, na.rm = TRUE) + seq_len(sum(is.na(memb)))
    return(memb)
  }

  if (method == "label_prop") {
    E <- Matrix::summary(S)
    E <- E[E$i < E$j & is.finite(E$x) & E$x > 0, , drop = FALSE]
    if (!nrow(E)) return(rep(1L, n))
    g <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                        vertices = data.frame(name = seq_len(n)))
    igraph::E(g)$weight <- E$x
    cl   <- igraph::cluster_label_prop(g, weights = igraph::E(g)$weight)
    memb <- rep(NA_integer_, n)
    memb[as.integer(igraph::V(g)$name)] <- as.integer(igraph::membership(cl))
    memb[is.na(memb)] <- max(memb, na.rm = TRUE) + seq_len(sum(is.na(memb)))
    return(memb)
  }

  if (method == "gc") {
    if (!requireNamespace("GCMER", quietly = TRUE))
      stop("Graph Coloring requires GCMER. Install: remotes::install_github('ddegras/GCMER')")
    D <- as.matrix(1 - S); diag(D) <- 0
    D <- pmax(0, D)
    res <- GCMER::resolve_entities(D, thresholds = threshold, method = "rlf")
    return(as.integer(res$ents[, 1]))
  }

  # ── Centroidal methods: derive feature matrix X if not supplied ───────────
  if (is.null(X)) X <- .features_from_S(S, svd_dim)

  pick_k <- k
  if (is.null(pick_k)) {
    pick_k <- .tune_k(X, k_grid, truth_vec, method, tune_metric)
  }
  pick_k <- max(2L, min(as.integer(pick_k), n - 1L))

  if (method == "hclust_avg") {
    D <- er_cosine_dist(X)
    hc <- stats::hclust(stats::as.dist(D), method = "average")
    return(as.integer(stats::cutree(hc, k = pick_k)))
  }

  if (method == "hclust_ward") {
    D <- stats::dist(X)   # Euclidean — Ward.D2 requires Euclidean geometry
    hc <- stats::hclust(D, method = "ward.D2")
    return(as.integer(stats::cutree(hc, k = pick_k)))
  }

  if (method == "pam") {
    D <- er_cosine_dist(X)
    return(as.integer(cluster::pam(stats::as.dist(D), k = pick_k)$clustering))
  }

  # ── Supervised pairwise classifiers ────────────────────────────────────────
  # Both SVM and GBM train on labeled pairs (i,j) using abs(X[i,]-X[j,]) as
  # features, predict match probabilities for all candidate pairs, rebuild a
  # similarity matrix, then apply threshold_cc.

  if (method == "svm") {
    if (!requireNamespace("e1071", quietly = TRUE))
      stop("svm requires package e1071. Install: install.packages('e1071')")
    if (is.null(truth_vec))
      stop("svm requires truth_vec for supervised training.")
    pf  <- .pair_features(S, X)
    if (!nrow(pf$feat)) return(seq_len(n))
    y   <- .pair_labels(pf$i, pf$j, truth_vec)
    lab <- !is.na(truth_vec[pf$i]) & !is.na(truth_vec[pf$j])
    if (sum(lab) < 10L || length(unique(y[lab])) < 2L) {
      warning("svm: too few labeled pairs; falling back to louvain.")
      return(er_cluster(S, "louvain", resolution = resolution,
                        X = X, svd_dim = svd_dim))
    }
    n_pos <- sum(y[lab]); n_neg <- sum(lab) - n_pos
    cw    <- c("0" = 1, "1" = max(1, n_neg / max(n_pos, 1L)))
    fit   <- e1071::svm(pf$feat[lab, ], factor(y[lab]),
                        kernel = "radial", probability = TRUE,
                        scale = TRUE, class.weights = cw)
    probs <- attr(predict(fit, pf$feat, probability = TRUE),
                  "probabilities")[, "1"]
    S_new <- .probs_to_sparse(pf$i, pf$j, probs, n)
    diag(S_new) <- 1
    return(er_cluster(S_new, "threshold_cc", threshold = threshold))
  }

  if (method == "gbm") {
    if (!requireNamespace("xgboost", quietly = TRUE))
      stop("gbm requires package xgboost. Install: install.packages('xgboost')")
    if (is.null(truth_vec))
      stop("gbm requires truth_vec for supervised training.")
    pf  <- .pair_features(S, X)
    if (!nrow(pf$feat)) return(seq_len(n))
    y   <- .pair_labels(pf$i, pf$j, truth_vec)
    lab <- !is.na(truth_vec[pf$i]) & !is.na(truth_vec[pf$j])
    if (sum(lab) < 10L || length(unique(y[lab])) < 2L) {
      warning("gbm: too few labeled pairs; falling back to louvain.")
      return(er_cluster(S, "louvain", resolution = resolution,
                        X = X, svd_dim = svd_dim))
    }
    n_pos  <- sum(y[lab]); n_neg <- sum(lab) - n_pos
    spw    <- max(1, n_neg / max(n_pos, 1L))   # scale_pos_weight for imbalance
    dtrain <- xgboost::xgb.DMatrix(pf$feat[lab, ], label = y[lab])
    dtest  <- xgboost::xgb.DMatrix(pf$feat)
    fit    <- xgboost::xgboost(data = dtrain, nrounds = 100L,
                               objective = "binary:logistic",
                               eta = 0.1, max_depth = 4L, subsample = 0.8,
                               scale_pos_weight = spw, verbose = 0L)
    probs  <- predict(fit, dtest)
    S_new  <- .probs_to_sparse(pf$i, pf$j, probs, n)
    diag(S_new) <- 1
    return(er_cluster(S_new, "threshold_cc", threshold = threshold))
  }

  rep(1L, n)   # fallback (should not reach here)
}

# ── er_cluster_all() ──────────────────────────────────────────────────────────

#' Run all clustering methods
#'
#' Applies every requested method to the combined similarity matrix \code{S}
#' and returns a named list of label vectors.
#'
#' @param S Symmetric \code{dgCMatrix} (n × n).
#' @param methods Character vector of methods, or \code{"all"} for all
#'   supported methods. Unavailable optional methods (leiden, gc) are silently
#'   skipped.
#' @param k Integer. Cluster count for centroidal methods (tuned if \code{NULL}).
#' @param k_grid Integer vector for k auto-tuning sweep.
#' @param threshold Numeric. Similarity threshold for \code{"threshold_cc"} and
#'   \code{"gc"}.
#' @param gc_thresholds Numeric vector. If provided, overrides \code{threshold}
#'   for graph coloring and sweeps, selecting best by silhouette or ARI.
#' @param resolution Numeric. Louvain/Leiden resolution.
#' @param X Optional feature matrix for centroidal methods.
#' @param svd_dim Integer. SVD dimensions when deriving X from S.
#' @param truth_vec Optional integer vector of ground-truth labels for supervised
#'   k-tuning.
#' @param tune_metric Character. Metric for supervised tuning.
#'   Default \code{"adj_rand"}.
#' @param verbose Logical. Print progress.
#'
#' @return Named list of integer vectors (one per method).
#' @export
er_cluster_all <- function(S,
                            methods      = "all",
                            k            = NULL,
                            k_grid       = c(5L, 10L, 15L, 20L, 30L, 50L),
                            threshold    = 0.5,
                            gc_thresholds = NULL,
                            resolution   = 1,
                            X            = NULL,
                            svd_dim      = 50L,
                            truth_vec    = NULL,
                            tune_metric  = "adj_rand",
                            verbose      = TRUE) {

  all_methods <- c("hclust_avg", "hclust_ward", "pam",
                   "threshold_cc", "louvain", "leiden", "label_prop", "gc",
                   "svm", "gbm")

  if (identical(methods, "all")) {
    methods <- all_methods
    # Drop optional packages if unavailable
    if (!requireNamespace("igraph", quietly = TRUE))
      methods <- setdiff(methods, c("leiden", "louvain", "label_prop", "threshold_cc"))
    if (!requireNamespace("GCMER", quietly = TRUE))
      methods <- setdiff(methods, "gc")
    # Supervised methods require both truth_vec and the relevant package
    if (is.null(truth_vec) || !requireNamespace("e1071", quietly = TRUE))
      methods <- setdiff(methods, "svm")
    if (is.null(truth_vec) || !requireNamespace("xgboost", quietly = TRUE))
      methods <- setdiff(methods, "gbm")
  }

  n <- nrow(S)
  if (is.null(X)) X <- .features_from_S(S, svd_dim)

  results <- list()
  for (m in methods) {
    if (verbose) message("  er_cluster_all: running ", m)
    labs <- tryCatch({
      if (m == "gc" && !is.null(gc_thresholds) && length(gc_thresholds) > 1L) {
        # Sweep GC thresholds, pick best
        D <- as.matrix(1 - S); diag(D) <- 0; D <- pmax(0, D)
        res_gc <- GCMER::resolve_entities(D, thresholds = gc_thresholds,
                                           method = "rlf")
        ents <- as.matrix(res_gc$ents)
        if (!is.null(truth_vec)) {
          aris <- vapply(seq_len(ncol(ents)), function(j) {
            if (requireNamespace("GCMER", quietly = TRUE))
              tryCatch(GCMER::adj_rand(as.integer(ents[, j]), truth_vec),
                       error = function(e) -Inf)
            else -Inf
          }, numeric(1L))
          as.integer(ents[, which.max(aris)])
        } else {
          sils <- vapply(seq_len(ncol(ents)), function(j)
            er_silhouette_avg(as.integer(ents[, j]),
                              stats::as.dist(pmax(0, 1 - as.matrix(S)))),
            numeric(1L))
          sils[!is.finite(sils)] <- -Inf
          as.integer(ents[, which.max(sils)])
        }
      } else {
        thr <- if (m == "gc") threshold else threshold
        er_cluster(S, method = m, k = k, k_grid = k_grid,
                   threshold = thr, resolution = resolution,
                   X = X, svd_dim = svd_dim,
                   truth_vec = truth_vec, tune_metric = tune_metric)
      }
    }, error = function(e) {
      message("  er_cluster_all: ", m, " failed: ", conditionMessage(e))
      rep(1L, n)
    })
    results[[m]] <- as.integer(labs)
  }
  results
}
