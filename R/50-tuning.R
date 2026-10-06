
########################################
########################################

#' @title ER Benchmark: Tuning, Methods (incl. Leiden), and Scalability
#' @description Turnkey utilities to (1) tune hyperparameters with robust internal criteria,
#' (2) run mainstream ER methods (kmeans/agglo/DBSCAN/Louvain/Leiden/CW/threshold-CC/MST+edit),
#' and (3) measure time & memory and generate scalability curves.
#' @keywords entity-resolution, clustering, graph, tuning, benchmarking
#' @importFrom stats kmeans dist hclust cutree
#' @importFrom utils head tail
#' @importFrom methods is
#' @exportPattern ^er_

# ---------- Utilities ---------------------------------------------------------

#' @title Ensure a package (soft dependency)
#' @param pkg Character. Package name to check.
#' @return Logical \code{TRUE} if available, \code{FALSE} (with a message) otherwise.
#' @keywords internal
er_require <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    message(sprintf("NOTE: package '%s' not installed; skipping features that need it.", pkg))
    return(FALSE)
  }
  TRUE
}

#' @title Measure time & memory of an expression
#' @param expr An R expression to evaluate.
#' @return Named list with \code{time_sec} (elapsed seconds), \code{peak_mem_MB}
#'   (peak RAM in MiB), and \code{result} (the value of \code{expr}).
#' @keywords internal
er_measure_tm <- function(expr) {
  if (er_require("peakRAM")) {
    out <- peakRAM::peakRAM(res <- force(expr))
    list(time_sec = as.numeric(out$Elapsed_Time_sec[1]),
         peak_mem_MB = as.numeric(out$Peak_RAM_Used_MiB[1]),
         result = res)
  } else {
    t <- system.time(res <- force(expr))
    bytes <- tryCatch(as.numeric(object.size(res)), error = function(e) NA_real_)
    list(time_sec = as.numeric(t[["elapsed"]]),
         peak_mem_MB = ifelse(is.na(bytes), NA_real_, bytes / 1024^2),
         result = res)
  }
}

# ---------- Features: TF-IDF + SVD -------------------------------------------

#' @title Text2Vec TF-IDF + SVD embeddings
#' @param text character vector
#' @param svd_dim integer, default 200
#' @param ngram c(lo, hi), default c(1,2)
#' @param max_features max vocab size
#' @return numeric matrix n x svd_dim
#' @export
er_tfidf_svd <- function(text, svd_dim = 200, ngram = c(1,2), max_features = 100000) {
  stopifnot(length(text) > 0)
  if (!er_require("text2vec") || !er_require("irlba")) {
    stop("text2vec and irlba are required for er_tfidf_svd().")
  }
  it <- text2vec::itoken(text, progressbar = FALSE)
  v  <- text2vec::create_vocabulary(it, ngram = ngram)
  v  <- text2vec::prune_vocabulary(v, term_count_min = 2, vocab_term_max = max_features)
  vectorizer <- text2vec::vocab_vectorizer(v)
  dtm <- text2vec::create_dtm(it, vectorizer)
  tfidf <- text2vec::TfIdf$new()
  X <- tfidf$fit_transform(dtm)
  s <- irlba::irlba(X, nu = svd_dim, nv = svd_dim)
  Z <- s$u %*% diag(s$d, nrow = length(s$d), ncol = length(s$d))
  Z
}

# ---------- Methods: Vector-space --------------------------------------------

#' @title K-means on embeddings
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param k Integer. Number of clusters.
#' @param nstart Integer. Number of random restarts. Default \code{10}.
#' @return Integer vector of cluster labels (length \code{n}).
#' @export
er_kmeans_from_Z <- function(Z, k, nstart = 10) {
  stopifnot(k >= 2, nrow(Z) >= k)
  stats::kmeans(Z, centers = k, nstart = nstart)$cluster
}

#' @title Agglomerative (average linkage) on cosine distance
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param k Integer. Number of clusters.
#' @return Integer vector of cluster labels (length \code{n}).
#' @export
er_agglomerative_cosine <- function(Z, k) {
  D <- er_cosine_dist(Z)
  hc <- stats::hclust(D, method = "average")
  stats::cutree(hc, k = k)
}

#' @title DBSCAN on embeddings
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param eps Numeric. Neighbourhood radius epsilon.
#' @param minPts Integer. Minimum points for core definition. Default \code{5}.
#' @return Integer vector of cluster labels; noise points receive unique singleton labels.
#' @export
er_dbscan_from_Z <- function(Z, eps, minPts = 5) {
  if (!er_require("dbscan")) stop("dbscan package required for er_dbscan_from_Z().")
  fit <- dbscan::dbscan(Z, eps = eps, minPts = minPts)
  lab <- fit$cluster
  if (any(lab == 0L)) {
    noise_idx <- which(lab == 0L)
    max_id <- max(lab)
    lab[noise_idx] <- seq_len(length(noise_idx)) + max_id
  }
  lab
}

# ---------- Graph building ----------------------------------------------------

#' @title Build cosine kNN graph with threshold
#' @param Z matrix n x d
#' @param k_knn integer
#' @param min_sim numeric between 0 and 1
#' @return igraph with weights = cosine similarity
#' @export
er_knn_graph <- function(Z, k_knn = 50, min_sim = 0.0) {
  if (!er_require("RANN") || !er_require("igraph")) {
    stop("RANN and igraph required for er_knn_graph().")
  }
  nn <- RANN::nn2(Z, query = Z, k = min(k_knn + 1, nrow(Z)))$nn.idx[, -1, drop = FALSE]
  n <- nrow(Z)
  nrm <- sqrt(rowSums(Z * Z) + 1e-12)
  src <- rep(seq_len(n), times = ncol(nn))
  dst <- as.vector(nn)
  num <- rowSums(Z[src, , drop = FALSE] * Z[dst, , drop = FALSE])
  den <- nrm[src] * nrm[dst]
  w   <- pmax(0, num / den)
  keep <- which(w >= min_sim & src < dst)
  if (!length(keep)) stop("No edges after threshold; try smaller min_sim or larger k_knn.")
  igraph::graph_from_data_frame(
    data.frame(from = src[keep], to = dst[keep], weight = w[keep]),
    directed = FALSE
  )
}

# ---------- Graph methods -----------------------------------------------------

#' @title Louvain on kNN graph
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param k_knn Integer. Number of nearest neighbours for graph construction. Default \code{50}.
#' @param min_sim Numeric. Minimum cosine similarity to retain an edge. Default \code{0.0}.
#' @return Integer membership vector with a \code{graph} attribute (the igraph used).
#' @export
er_louvain_from_Z <- function(Z, k_knn = 50, min_sim = 0.0) {
  if (!er_require("igraph")) stop("igraph required for er_louvain_from_Z().")
  g <- er_knn_graph(Z, k_knn = k_knn, min_sim = min_sim)
  cl <- igraph::cluster_louvain(g, weights = igraph::E(g)$weight)
  structure(igraph::membership(cl), graph = g)
}

#' @title Leiden on kNN graph (via leidenbase)
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param k_knn Integer. Number of nearest neighbours. Default \code{50}.
#' @param min_sim Numeric. Minimum cosine similarity edge threshold. Default \code{0.1}.
#' @param resolution_parameter Numeric. Higher values produce more, smaller communities. Default \code{0.5}.
#' @param partition_type Character. One of \code{"ModularityVertexPartition"} or
#'   \code{"CPMVertexPartition"}. Default \code{"ModularityVertexPartition"}.
#' @return Integer membership vector with a \code{graph} attribute (the igraph used).
#' @export
er_leiden_from_Z <- function(
  Z, k_knn = 50, min_sim = 0.1,
  resolution_parameter = 0.5,
  partition_type = "ModularityVertexPartition"
) {
  if (!er_require("igraph") || !er_require("leidenbase")) {
    stop("Packages 'igraph' and 'leidenbase' required for er_leiden_from_Z().")
  }
  g <- er_knn_graph(Z, k_knn = k_knn, min_sim = min_sim)
  part <- leidenbase::leiden_find_partition(
    g,
    partition_type = partition_type,
    weights = igraph::E(g)$weight,
    resolution_parameter = resolution_parameter,
    seed = 1,
    n_iterations = -1
  )
  structure(part$membership, graph = g)
}

#' @title Chinese Whispers (label propagation)
#' @param g An igraph object with edge \code{weight} attributes.
#' @param iters Integer. Number of propagation iterations. Default \code{20}.
#' @return Integer vector of cluster labels (length = number of vertices).
#' @export
er_chinese_whispers <- function(g, iters = 20) {
  if (!er_require("igraph")) stop("igraph required for er_chinese_whispers().")
  n <- igraph::vcount(g)
  lab <- seq_len(n)
  for (t in seq_len(iters)) {
    order <- sample.int(n)
    for (v in order) {
      ne  <- igraph::neighbors(g, v)
      if (length(ne) == 0) next
      ws  <- igraph::E(g)[from(v) %--% ne]$weight
      labs <- lab[as.integer(ne)]
      lab[v] <- as.integer(names(which.max(tapply(ws, labs, sum))))
    }
  }
  lab
}

#' @title Thresholded connected components on cosine similarity
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param min_sim Numeric. Minimum cosine similarity to retain an edge. Default \code{0.6}.
#' @param k_knn Integer. Number of nearest neighbours for graph construction. Default \code{100}.
#' @return Integer vector of connected-component membership labels.
#' @export
er_threshold_cc <- function(Z, min_sim = 0.6, k_knn = 100) {
  g <- er_knn_graph(Z, k_knn = k_knn, min_sim = min_sim)
  igraph::components(g)$membership
}

#' @title MST + Edit Distance threshold (single text field)
#' @param text Character vector of text values (one per record).
#' @param max_dist Integer. Maximum Levenshtein distance to retain an MST edge. Default \code{3}.
#' @return Integer vector of connected-component membership labels.
#' @export
er_mst_edit <- function(text, max_dist = 3) {
  if (!er_require("igraph") || !er_require("stringdist")) {
    stop("igraph and stringdist required for er_mst_edit().")
  }
  n <- length(text)
  if (n > 8000L) warning("er_mst_edit(): O(n^2) edit-distance; add blocking for n>8k.")
  D <- as.matrix(stringdist::stringdistmatrix(text, text, method = "lv"))
  g  <- igraph::graph_from_adjacency_matrix(D, mode = "undirected", weighted = TRUE, diag = FALSE)
  mst <- igraph::mst(g, weights = igraph::E(g)$weight)
  bad <- which(igraph::E(mst)$weight > max_dist)
  if (length(bad)) mst <- igraph::delete_edges(mst, bad)
  igraph::components(mst)$membership
}

# ---------- Internal & External Criteria -------------------------------------

#' @title Internal metrics (Silhouette/CH/DB/Modularity/Stability)
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param labels Integer vector of cluster labels (length \code{n}).
#' @param G Optional igraph graph; required for modularity computation. Default \code{NULL}.
#' @param sample_n Integer. Maximum sample size for silhouette computation. Default \code{2000}.
#' @return Named list with elements \code{silhouette}, \code{ch}, \code{db}, \code{modularity}.
#' @export
er_internal_metrics <- function(Z, labels, G = NULL, sample_n = 2000L) {
  stopifnot(length(labels) == nrow(Z))
  n <- nrow(Z); k <- length(unique(labels))

  # Silhouette (sampled, cosine)
  sil <- NA_real_
  if (k >= 2L) {
    idx <- if (n > sample_n) sample.int(n, sample_n) else seq_len(n)
    D   <- er_cosine_dist(Z[idx, , drop = FALSE])
    if (er_require("cluster")) {
      sil_obj <- cluster::silhouette(as.integer(labels[idx]), D)
      sil <- mean(sil_obj[, 3L])
    }
  }

  # CH & DB (Euclidean in Z)
  X <- Z
  mu <- colMeans(X)
  W <- 0; B <- 0
  centers <- rowsum(X, labels) / as.numeric(table(labels))
  for (lab in unique(labels)) {
    Xi <- X[labels == lab, , drop = FALSE]
    ci <- centers[as.character(lab), , drop = FALSE]
    W  <- W + sum((Xi - matrix(ci, nrow(Xi), ncol(Xi), TRUE))^2)
    B  <- B + nrow(Xi) * sum((ci - mu)^2)
  }
  CH <- if (k > 1L && k < n) (B/(k-1)) / (W/(n-k)) else NA_real_

  db <- NA_real_
  if (k >= 2L) {
    Si <- sapply(unique(labels), function(l) {
      Xi <- X[labels == l, , drop = FALSE]
      ci <- centers[as.character(l), , drop = FALSE]
      sqrt(mean(rowSums((Xi - matrix(ci, nrow(Xi), ncol(Xi), TRUE))^2)))
    })
    C <- as.matrix(dist(centers))
    R <- outer(Si, Si, "+") / (C + 1e-12)
    diag(R) <- -Inf
    db <- mean(apply(R, 1L, max), na.rm = TRUE)
  }

  Q <- NA_real_
  if (!is.null(G) && er_require("igraph")) {
    Q <- igraph::modularity(G, membership = labels, weights = igraph::E(G)$weight)
  }

  list(silhouette = sil, ch = CH, db = db, modularity = Q)
}

#' @title Penalized Silhouette (avoid monotone-in-k)
#' @param sil Numeric. Average silhouette width.
#' @param k Integer. Number of clusters.
#' @param n Integer. Total number of records.
#' @param alpha Numeric. Penalty weight; if \code{NULL} auto-set to \code{0.1 / log(sqrt(n))}.
#' @return Numeric penalized silhouette score, or \code{NA} if inputs are invalid.
#' @export
er_penalize_silhouette <- function(sil, k, n, alpha = NULL) {
  if (is.na(sil) || is.na(k) || k < 2L) return(NA_real_)
  if (is.null(alpha)) {
    kmax <- max(3, floor(sqrt(n)))
    alpha <- 0.1 / max(1, log(kmax))
  }
  sil - alpha * log(k)
}

#' @title Stability via repeated-subsample ARI
#'
#' B-03 rewrite (2026-10-06): the old implementation split ONE subsample into
#' two disjoint halves and computed ARI between clusterings of DIFFERENT record
#' sets, which is meaningless (ARI compares two partitions of the SAME set). It
#' also initialised the accumulator with 0 so skipped replicates polluted the
#' mean, and crashed on odd subsample sizes. The rewrite draws TWO independent
#' subsamples per replicate, clusters each, and computes ARI on their
#' INTERSECTION -- the standard subsampling-stability design.
#'
#' @param Z Numeric matrix \eqn{n \times d} of record embeddings.
#' @param B Integer. Number of replicates. Default \code{10}.
#' @param frac Numeric. Fraction of records per subsample. Default \code{0.8}.
#' @param fit_fun Function with signature \code{function(X, ...)} returning an
#'   integer label vector (one label per row of \code{X}). It may additionally
#'   declare \code{text_sub = NULL} to receive subsampled raw text (for
#'   text-based methods such as \code{er_mst_edit}).
#' @param seed Integer or \code{NULL}. Seed for the subsampling, applied in
#'   local scope (the caller's RNG state is preserved via \code{er_with_seed}).
#' @param text Optional character vector (length \code{nrow(Z)}) of raw record
#'   text; subsampled alongside \code{Z} and forwarded to \code{fit_fun} as
#'   \code{text_sub}.
#' @param ... Additional arguments passed to \code{fit_fun}.
#' @return Numeric mean ARI over successful replicates (\code{NA} for failed
#'   ones); \code{NA_real_} if \code{mclust} is unavailable or fewer than 2
#'   replicates succeed.
#' @export
er_stability_ari <- function(Z, B = 10L, frac = 0.8, fit_fun,
                             seed = NULL, text = NULL, ...) {
  if (!er_require("mclust")) return(NA_real_)
  n <- nrow(Z)
  if (is.null(n) || n < 4L) return(NA_real_)
  if (!is.null(text) && length(text) != n)
    stop("er_stability_ari: 'text' must have length nrow(Z).")
  m <- max(2L, floor(frac * n))
  ARIs <- rep(NA_real_, B)

  run <- function() {
    for (b in seq_len(B)) {
      i1 <- sample.int(n, m)
      i2 <- sample.int(n, m)
      t1 <- if (is.null(text)) NULL else text[i1]
      t2 <- if (is.null(text)) NULL else text[i2]
      lab1 <- tryCatch(fit_fun(Z[i1, , drop = FALSE], text_sub = t1, ...),
                       error = function(e) NULL)
      lab2 <- tryCatch(fit_fun(Z[i2, , drop = FALSE], text_sub = t2, ...),
                       error = function(e) NULL)
      if (is.null(lab1) || is.null(lab2)) next
      if (length(lab1) != m || length(lab2) != m) next
      common <- intersect(i1, i2)
      if (length(common) < 2L) next
      l1 <- lab1[match(common, i1)]
      l2 <- lab2[match(common, i2)]
      if (any(is.na(l1)) || any(is.na(l2))) next
      ARIs[b] <<- tryCatch(mclust::adjustedRandIndex(l1, l2),
                           error = function(e) NA_real_)
    }
    if (sum(!is.na(ARIs)) < 2L) return(NA_real_)
    mean(ARIs, na.rm = TRUE)
  }
  if (is.null(seed)) run() else er_with_seed(seed, run())
}

#' @title External metrics (ARI, pairwise F1)
#' @param labels Integer vector of predicted cluster labels.
#' @param truth_pairs Optional data frame with columns \code{i}, \code{j}, \code{label}
#'   (1 = match, 0 = non-match) for pairwise F1.
#' @param truth_vec Optional integer vector of ground-truth entity labels for ARI.
#' @return Named list with \code{ari} and \code{pairwise_f1}.
#' @export
er_external_metrics <- function(labels, truth_pairs = NULL, truth_vec = NULL) {
  out <- list(ari = NA_real_, pairwise_f1 = NA_real_)
  if (!is.null(truth_vec) && er_require("mclust")) {
    out$ari <- mclust::adjustedRandIndex(labels, truth_vec)
  }
  if (!is.null(truth_pairs)) {
    TP <- FP <- FN <- 0L
    for (r in seq_len(nrow(truth_pairs))) {
      i <- truth_pairs$i[r]; j <- truth_pairs$j[r]; y <- truth_pairs$label[r]
      pred <- as.integer(labels[i] == labels[j])
      if (pred == 1L && y == 1L) TP <- TP + 1L
      if (pred == 1L && y == 0L) FP <- FP + 1L
      if (pred == 0L && y == 1L) FN <- FN + 1L
    }
    prec <- ifelse(TP + FP == 0, NA_real_, TP/(TP+FP))
    rec  <- ifelse(TP + FN == 0, NA_real_, TP/(TP+FN))
    out$pairwise_f1 <- ifelse(is.na(prec) || is.na(rec) || prec+rec==0, NA_real_, 2*prec*rec/(prec+rec))
  }
  out
}

# ---------- Tuning Engine (incl. Leiden) -------------------------------------

#' @title Tune ER methods over parameter grids (incl. Leiden if available)
#'
#' @description Tune hyperparameters of the vector-space ER methods. Two
#'   disciplines are supported:
#'
#'   * **Internal objectives** (`"silhouette"`, `"silhouette_penalized"`,
#'     `"ch"`, `"db_min"`, `"modularity"`, `"stability"`): unsupervised;
#'     candidates are scored on the full data (no labels involved).
#'
#'   * **External objectives** (`"ari"`, `"pairwise_f1"`): the objective is
#'     computed ON gold labels, so tuning on the same labels that are reported
#'     would be test-set tuning. Phase-2b honest protocol (2026-10-06):
#'     entities (never records) are split into **Fit / Validation / Test**
#'     via [er_stratified_three_way_split()]; candidates are scored on
#'     **Validation only** (dev-set selection); the winner is run **once** on
#'     **Test** and those one-time metrics are reported in
#'     `$test_metrics`. Test labels never drive any selection decision, and
#'     each partition is embedded independently so no TF-IDF vocabulary leaks
#'     across partitions. The Fit partition is currently unused by the
#'     unsupervised candidates (reserved for representation learning) and is
#'     returned in `$split` for inspection.
#'
#' @param data data.frame with ER text fields
#' @param fields character vector of fields to use (concatenated)
#' @param methods subset of:
#'   c("kmeans","agglo","dbscan","louvain","leiden","cw","threshold_cc","mst_edit")
#' @param grids named list of parameter grids per method
#' @param objective one of: "silhouette","silhouette_penalized","ch","db_min","modularity","stability","ari","pairwise_f1".
#'   The last two are external: they require `truth$truth_vec` and trigger the
#'   honest Fit/Validation/Test protocol.
#' @param truth optional list(truth_vec=..., truth_pairs=...). `truth_vec` is an
#'   integer/factor/character vector of gold entity labels aligned to the rows
#'   of `data` (`NA` = unlabelled). In honest mode only `truth_vec` is used;
#'   `truth_pairs` is ignored.
#' @param svd_dim integer, default 200
#' @param sample_n_sil integer, default 2000
#' @param stability_B integer, default 10
#' @param split_fractions named numeric vector with `fit`/`validation`/`test`
#'   summing to 1 (honest mode only). Default `c(0.35, 0.35, 0.30)`.
#' @param seed integer RNG seed for the entity split (honest mode only).
#'   Default 42.
#' @return list with:
#'   * `curves`: data.frame of per-candidate metrics (validation metrics in
#'     honest mode, full-data metrics otherwise). Failed cells get `NA`
#'     metrics and a `cell_error` message instead of aborting the tuning run.
#'   * `best`: named list of the best row per method.
#'   * `honest`: logical; whether the Fit/Validation/Test protocol was used.
#'   * `objective`, `split` (entity-disjoint index lists, honest mode only),
#'     `test_metrics` (one-time held-out evaluation of the winner, honest
#'     mode only).
#' @export
er_tune <- function(data, fields,
                    methods = c("kmeans","agglo","dbscan","louvain","leiden","cw","threshold_cc","mst_edit"),
                    grids   = list(),
                    objective = "silhouette_penalized",
                    truth = NULL,
                    svd_dim = 200,
                    sample_n_sil = 2000L,
                    stability_B = 10L,
                    split_fractions = c(fit = 0.35, validation = 0.35, test = 0.30),
                    seed = 42L) {

  objective <- match.arg(objective,
    c("silhouette", "silhouette_penalized", "ch", "db_min", "modularity",
      "stability", "ari", "pairwise_f1"))
  EXTERNAL <- c("ari", "pairwise_f1")

  # Honest-tuning gate FIRST, before any expensive embedding: external
  # objectives select parameters on gold labels, so without truth there is
  # nothing legitimate to tune. Fail loudly instead of returning silent NAs.
  if (objective %in% EXTERNAL &&
      (is.null(truth) || is.null(truth$truth_vec))) {
    stop("er_tune: objective='", objective, "' selects parameters on gold labels, ",
         "so it requires truth$truth_vec for the Fit/Validation/Test split. ",
         "Use an internal objective (e.g. 'silhouette_penalized') for unsupervised tuning.")
  }
  honest <- objective %in% EXTERNAL
  if (honest && !requireNamespace("mclust", quietly = TRUE)) {
    stop("er_tune: honest tuning with external objectives requires package 'mclust' ",
         "for ARI computation.")
  }

  stopifnot(all(fields %in% names(data)))
  n <- nrow(data)
  txt <- do.call(paste, c(unname(data[fields]), sep = " "))

  # filter out methods lacking deps
  has_igraph  <- er_require("igraph")
  has_RANN    <- er_require("RANN")
  has_leiden  <- er_require("leidenbase")
  methods <- unique(Filter(function(m) {
    switch(m,
      kmeans = TRUE,
      agglo  = TRUE,
      dbscan = er_require("dbscan"),
      louvain= has_igraph && has_RANN,
      leiden = has_igraph && has_RANN && has_leiden,
      cw     = has_igraph && has_RANN,
      threshold_cc = has_igraph && has_RANN,
      mst_edit = er_require("stringdist") && has_igraph,
      FALSE)
  }, methods))
  if (!length(methods)) stop("No methods available (missing dependencies).")

  tune_args <- list(objective = objective, sample_n_sil = sample_n_sil,
                    stability_B = stability_B)

  if (honest) {
    tv <- truth$truth_vec
    if (length(tv) != n)
      stop("er_tune: length(truth$truth_vec) must equal nrow(data).")
    if (is.null(names(split_fractions)) ||
        !all(c("fit", "validation", "test") %in% names(split_fractions))) {
      stop("er_tune: 'split_fractions' must be a named numeric vector with ",
           "fit/validation/test entries summing to 1.")
    }
    # Entity coding robust to integer/factor/character labels; NA = unlabelled.
    ent <- as.integer(factor(tv))
    ids <- paste0("er_tune_rec", seq_len(n))
    truth_tbl <- tibble::tibble(id = ids, cluster_id = ent)
    sp <- er_stratified_three_way_split(ids, truth_tbl,
                                        fractions = split_fractions, seed = seed)
    idx_fit <- sp$indices$fit
    idx_val <- sp$indices$validation
    idx_tst <- sp$indices$test
    if (length(idx_val) < 4L || length(idx_tst) < 2L) {
      stop("er_tune: validation/test partitions too small for honest tuning (n=",
           n, "). Use more records or an internal objective.")
    }
    if (sum(!is.na(ent[idx_val])) < 2L) {
      stop("er_tune: fewer than 2 labelled records in the validation partition; ",
           "cannot select parameters.")
    }

    # Each partition embedded independently: no vocabulary leaks across
    # Fit/Validation/Test.
    Z_val <- .embed_part(txt[idx_val], svd_dim)
    Z_tst <- .embed_part(txt[idx_tst], svd_dim)
    f1_val <- data[[fields[1]]][idx_val]
    f1_tst <- data[[fields[1]]][idx_tst]

    # SELECTION: score every candidate on Validation only.
    sel <- .tune_method_grid(methods, grids, Z_val, txt[idx_val], f1_val,
                             ent[idx_val], honest = TRUE, tune_args)

    wscores <- unlist(sel$best_scores)
    if (!length(wscores) || all(!is.finite(wscores))) {
      stop("er_tune: no candidate produced a valid validation score.")
    }
    wmethod <- names(which.max(wscores))
    wparams <- sel$best_params[[wmethod]]

    # FINAL (once): run the winner on Test; these are the reported metrics.
    tcell <- tryCatch(
      er_measure_tm(.tune_run_cell(wmethod, wparams, Z_tst, txt[idx_tst], f1_tst)),
      error = function(e) e
    )
    if (inherits(tcell, "error")) {
      stop("er_tune: winning parameters failed on the test partition: ",
           conditionMessage(tcell))
    }
    lab_t <- !is.na(ent[idx_tst])
    if (sum(lab_t) < 2L) {
      warning("er_tune: fewer than 2 labelled records in test; test metrics are NA.")
      tm <- list(ari = NA_real_, pair_f1 = NA_real_, b3_f1 = NA_real_,
                 pair_precision = NA_real_, pair_recall = NA_real_)
    } else {
      s <- er_external_scores(tcell$result$labels[lab_t], ent[idx_tst][lab_t])
      tm <- list(ari = s$ari, pair_f1 = s$pair_f1, b3_f1 = s$b3_f1,
                 pair_precision = s$pair_precision, pair_recall = s$pair_recall)
    }
    test_metrics <- c(list(method = wmethod, params = wparams,
                           n_test = length(idx_tst),
                           n_test_labelled = sum(lab_t)), tm)

    return(list(curves = sel$curves, best = sel$best, honest = TRUE,
                objective = objective,
                split = list(fit = idx_fit, validation = idx_val, test = idx_tst),
                test_metrics = test_metrics))
  }

  # Legacy path: internal objective, no labels involved.
  Z <- er_tfidf_svd(txt, svd_dim = svd_dim)
  all <- .tune_method_grid(methods, grids, Z, txt, data[[fields[1]]], truth,
                           honest = FALSE, tune_args)
  list(curves = all$curves, best = all$best, honest = FALSE,
       objective = objective, split = NULL, test_metrics = NULL)
}

# ---------- Honest-tuning internals (not exported: dot-prefixed) ---------------

#' Per-partition TF-IDF+SVD embedding with dimension fallback.
#'
#' Each partition is embedded independently (fit_transform inside the
#' partition) so no vocabulary leaks across Fit/Validation/Test. `irlba`
#' needs `svd_dim < min(nrow, ncol)` of the document-term matrix; tiny
#' partitions may not support the requested dimension, so halve and retry.
#' @keywords internal
.embed_part <- function(txtp, svd_dim) {
  if (!requireNamespace("text2vec", quietly = TRUE) ||
      !requireNamespace("irlba", quietly = TRUE)) {
    stop("er_tune: packages 'text2vec' and 'irlba' are required for text embeddings.")
  }
  d <- max(2L, min(as.integer(svd_dim), length(txtp) - 1L))
  repeat {
    out <- tryCatch(er_tfidf_svd(txtp, svd_dim = d), error = function(e) e)
    if (!inherits(out, "error")) return(out)
    if (d <= 2L) stop("er_tune: embedding failed: ", conditionMessage(out))
    d <- max(2L, floor(d / 2))
  }
}

#' Run one (method, params) cell on a partition's embeddings.
#' @keywords internal
.tune_run_cell <- function(m, params, Zp, txtp, f1txtp) {
  if (m == "kmeans") {
    lab <- er_kmeans_from_Z(Zp, k = params$k, nstart = params$nstart %||% 10)
    list(labels = lab, graph = NULL)
  } else if (m == "agglo") {
    lab <- er_agglomerative_cosine(Zp, k = params$k)
    list(labels = lab, graph = NULL)
  } else if (m == "dbscan") {
    lab <- er_dbscan_from_Z(Zp, eps = params$eps, minPts = params$minPts)
    list(labels = lab, graph = NULL)
  } else if (m == "louvain") {
    lab <- er_louvain_from_Z(Zp, k_knn = params$k_knn, min_sim = params$min_sim)
    G <- attr(lab, "graph"); list(labels = as.integer(lab), graph = G)
  } else if (m == "leiden") {
    lab <- er_leiden_from_Z(Zp, k_knn = params$k_knn, min_sim = params$min_sim,
                            resolution_parameter = params$resolution_parameter)
    G <- attr(lab, "graph"); list(labels = as.integer(lab), graph = G)
  } else if (m == "cw") {
    G <- er_knn_graph(Zp, k_knn = params$k_knn %||% 50, min_sim = params$min_sim %||% 0.1)
    lab <- er_chinese_whispers(G, iters = params$iters)
    list(labels = lab, graph = G)
  } else if (m == "threshold_cc") {
    lab <- er_threshold_cc(Zp, min_sim = params$min_sim, k_knn = params$k_knn)
    G <- er_knn_graph(Zp, k_knn = params$k_knn, min_sim = params$min_sim)
    list(labels = lab, graph = G)
  } else if (m == "mst_edit") {
    lab <- er_mst_edit(f1txtp, max_dist = params$max_dist)
    list(labels = lab, graph = NULL)
  } else stop("Unknown method.")
}

#' Default parameter grid for one method (same grids as before).
#' @keywords internal
.tune_default_grid <- function(m) {
  switch(m,
    kmeans = expand.grid(k = seq(10, 300, by = 10), nstart = 10),
    agglo  = expand.grid(k = seq(10, 300, by = 10)),
    dbscan = expand.grid(eps = c(0.8, 1.0, 1.2), minPts = c(5, 10)),
    louvain= expand.grid(k_knn = c(20, 50, 100), min_sim = c(0.0, 0.1, 0.2)),
    leiden = expand.grid(k_knn = c(20, 50, 100), min_sim = c(0.0, 0.1, 0.2),
                         resolution_parameter = c(0.2, 0.5, 1.0)),
    cw     = expand.grid(iters = c(10, 20, 50), k_knn = 50, min_sim = 0.1),
    threshold_cc = expand.grid(k_knn = c(50, 100), min_sim = c(0.5, 0.6, 0.7)),
    mst_edit = expand.grid(max_dist = c(1, 2, 3)),
    stop(sprintf("Unknown method %s", m))
  )
}

#' Score one fitted cell: internal metrics always; external metrics depend on
#' the discipline. In honest mode `truthp` is the partition's integer entity
#' vector (NA = unlabelled) and only labelled records are scored, via the
#' pair-counting implementation (no pair enumeration). In legacy mode `truthp`
#' is the user-supplied `truth` list (or NULL).
#' @keywords internal
.tune_score_row <- function(m, params, labels, G, Zp, txtp, truthp,
                            honest, tune_args) {
  objective    <- tune_args$objective
  sample_n_sil <- tune_args$sample_n_sil
  stability_B  <- tune_args$stability_B

  int <- er_internal_metrics(Zp, labels, G = G, sample_n = sample_n_sil)
  ps  <- er_penalize_silhouette(int$silhouette, length(unique(labels)), nrow(Zp))
  stab <- if (identical(objective, "stability")) {
    er_stability_ari(Zp, B = stability_B, text = txtp, fit_fun = function(X, text_sub = NULL, ...) {
      switch(m,
        kmeans = er_kmeans_from_Z(X, k = params$k),
        agglo  = er_agglomerative_cosine(X, k = params$k),
        dbscan = er_dbscan_from_Z(X, eps = params$eps, minPts = params$minPts),
        louvain= as.integer(er_louvain_from_Z(X, k_knn = params$k_knn, min_sim = params$min_sim)),
        leiden = as.integer(er_leiden_from_Z(X, k_knn = params$k_knn, min_sim = params$min_sim,
                                             resolution_parameter = params$resolution_parameter %||% 0.5)),
        cw     = {
          Gx <- er_knn_graph(X, k_knn = params$k_knn %||% 50, min_sim = params$min_sim %||% 0.1)
          er_chinese_whispers(Gx, iters = params$iters %||% 20)
        },
        threshold_cc = er_threshold_cc(X, min_sim = params$min_sim, k_knn = params$k_knn),
        # M-41 fix (2026-10-06): the old code passed params$text, which is
        # always NULL on this path -> er_mst_edit(NULL) errored. Use the
        # subsampled raw text forwarded by er_stability_ari instead.
        mst_edit = er_mst_edit(text_sub, max_dist = params$max_dist)
      )
    })
  } else NA_real_

  if (honest) {
    lab <- !is.na(truthp)
    ext <- if (sum(lab) >= 2L) {
      s <- er_external_scores(labels[lab], truthp[lab])
      list(ari = s$ari, pairwise_f1 = s$pair_f1)
    } else {
      list(ari = NA_real_, pairwise_f1 = NA_real_)
    }
  } else {
    ext <- if (!is.null(truthp)) {
      er_external_metrics(labels, truth_pairs = truthp$truth_pairs,
                          truth_vec = truthp$truth_vec)
    } else {
      list(ari = NA_real_, pairwise_f1 = NA_real_)
    }
  }

  data.frame(
    method = m,
    k = if (!is.null(params$k)) params$k else NA_integer_,
    k_knn = if (!is.null(params$k_knn)) params$k_knn else NA_integer_,
    min_sim = if (!is.null(params$min_sim)) params$min_sim else NA_real_,
    eps = if (!is.null(params$eps)) params$eps else NA_real_,
    minPts = if (!is.null(params$minPts)) params$minPts else NA_real_,
    resolution_parameter = if (!is.null(params$resolution_parameter)) params$resolution_parameter else NA_real_,
    max_dist = if (!is.null(params$max_dist)) params$max_dist else NA_real_,
    silhouette = int$silhouette,
    silhouette_pen = ps,
    ch = int$ch,
    db = int$db,
    modularity = int$modularity,
    stability = stab,
    ari = ext$ari,
    pairwise_f1 = ext$pairwise_f1,
    stringsAsFactors = FALSE
  )
}

#' One NA row with the same columns as .tune_score_row(), plus the error.
#' @keywords internal
.tune_na_row <- function(m, params, err) {
  row <- data.frame(
    method = m,
    k = if (!is.null(params$k)) params$k else NA_integer_,
    k_knn = if (!is.null(params$k_knn)) params$k_knn else NA_integer_,
    min_sim = if (!is.null(params$min_sim)) params$min_sim else NA_real_,
    eps = if (!is.null(params$eps)) params$eps else NA_real_,
    minPts = if (!is.null(params$minPts)) params$minPts else NA_real_,
    resolution_parameter = if (!is.null(params$resolution_parameter)) params$resolution_parameter else NA_real_,
    max_dist = if (!is.null(params$max_dist)) params$max_dist else NA_real_,
    silhouette = NA_real_, silhouette_pen = NA_real_,
    ch = NA_real_, db = NA_real_, modularity = NA_real_,
    stability = NA_real_, ari = NA_real_, pairwise_f1 = NA_real_,
    stringsAsFactors = FALSE
  )
  row$cell_error <- err
  row$time_sec <- NA_real_
  row$peak_mem_MB <- NA_real_
  row
}

#' Run the full method loop on one partition.
#'
#' Each cell is guarded: a failing parameter combination yields an NA row
#' (with `cell_error`) instead of aborting the whole tuning run. This matters
#' for honest mode, where default grids (e.g. k up to 300) routinely exceed
#' small validation partitions.
#'
#' @return list(curves, best, best_params, best_scores).
#' @keywords internal
.tune_method_grid <- function(methods, grids, Zp, txtp, f1txtp, truthp,
                              honest, tune_args) {
  objective <- tune_args$objective
  rows <- list()
  best_by_method <- list()
  best_params <- list()
  best_scores <- list()

  for (m in methods) {
    g <- grids[[m]]
    if (is.null(g)) g <- .tune_default_grid(m)

    method_rows <- vector("list", nrow(g))
    best_row <- NULL; best_par <- NULL; best_score <- -Inf

    for (i in seq_len(nrow(g))) {
      params <- as.list(g[i, , drop = FALSE])
      cell <- tryCatch({
        tm <- er_measure_tm(.tune_run_cell(m, params, Zp, txtp, f1txtp))
        list(ok = TRUE, labels = tm$result$labels, graph = tm$result$graph,
             time_sec = tm$time_sec, peak_mem_MB = tm$peak_mem_MB)
      }, error = function(e) list(ok = FALSE, err = conditionMessage(e)))

      if (cell$ok) {
        row <- .tune_score_row(m, params, cell$labels, cell$graph, Zp, txtp,
                               truthp, honest, tune_args)
        row$cell_error <- NA_character_
        row$time_sec    <- cell$time_sec
        row$peak_mem_MB <- cell$peak_mem_MB
      } else {
        row <- .tune_na_row(m, params, cell$err)
      }
      method_rows[[i]] <- row

      sc <- switch(objective,
        silhouette = row$silhouette,
        silhouette_penalized = row$silhouette_pen,
        ch = row$ch,
        db_min = -row$db,
        modularity = row$modularity,
        stability = row$stability,
        ari = row$ari,
        pairwise_f1 = row$pairwise_f1,
        stop("Unknown objective")
      )
      if (!is.na(sc) && sc > best_score) {
        best_score <- sc; best_row <- row; best_par <- params
      }
    }

    method_df <- do.call(rbind, method_rows)
    rows[[m]] <- method_df
    best_by_method[[m]] <- best_row
    best_params[[m]] <- best_par
    best_scores[[m]] <- best_score
  }

  curves <- do.call(rbind, rows)
  list(curves = curves, best = best_by_method,
       best_params = best_params, best_scores = best_scores)
}

# ---------- Scalability -------------------------------------------------------
# ---------- Scalability -------------------------------------------------------

#' @title Scalability curves by subsampling n
#' @param data Data frame containing the text fields.
#' @param fields Character vector of field names to concatenate as input text.
#' @param n_seq Integer vector of subsample sizes to benchmark.
#' @param method Character. Clustering method name (one of those supported by
#'   \code{er_tune()}). Default \code{"kmeans"}.
#' @param params_fixed Named list of fixed hyperparameters for the method.
#' @param svd_dim Integer. TF-IDF SVD embedding dimension. Default \code{200}.
#' @return Data frame with columns \code{n}, \code{method}, \code{time_sec},
#'   \code{peak_mem_MB}, \code{k} (number of clusters found).
#' @export
er_scaling_curve <- function(data, fields, n_seq,
                             method = "kmeans",
                             params_fixed = list(),
                             svd_dim = 200) {
  set.seed(1)
  stopifnot(all(fields %in% names(data)))
  txt_all <- do.call(paste, c(unname(data[fields]), sep = " "))
  out <- list()
  for (n in n_seq) {
    idx <- if (length(txt_all) > n) sample.int(length(txt_all), n) else seq_along(txt_all)
    Z <- er_tfidf_svd(txt_all[idx], svd_dim = svd_dim)
    tm <- er_measure_tm({
      lab <- switch(method,
        kmeans = er_kmeans_from_Z(Z, k = params_fixed$k %||% max(2, floor(sqrt(n)))),
        agglo  = er_agglomerative_cosine(Z, k = params_fixed$k %||% max(2, floor(sqrt(n)))),
        dbscan = er_dbscan_from_Z(Z, eps = params_fixed$eps %||% 1.0, minPts = params_fixed$minPts %||% 5),
        louvain= { m <- er_louvain_from_Z(Z, k_knn = params_fixed$k_knn %||% 50, min_sim = params_fixed$min_sim %||% 0.1); as.integer(m) },
        leiden = { m <- er_leiden_from_Z(Z, k_knn = params_fixed$k_knn %||% 50, min_sim = params_fixed$min_sim %||% 0.1,
                                         resolution_parameter = params_fixed$resolution_parameter %||% 0.5); as.integer(m) },
        threshold_cc = er_threshold_cc(Z, min_sim = params_fixed$min_sim %||% 0.6, k_knn = params_fixed$k_knn %||% 100),
        cw = {
          G <- er_knn_graph(Z, k_knn = params_fixed$k_knn %||% 50, min_sim = params_fixed$min_sim %||% 0.1)
          er_chinese_whispers(G, iters = params_fixed$iters %||% 20)
        },
        stop("Unsupported method in scaling.")
      )
      lab
    })
    out[[length(out)+1]] <- data.frame(
      n = n,
      method = method,
      time_sec = tm$time_sec,
      peak_mem_MB = tm$peak_mem_MB,
      k = length(unique(tm$result)),
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, out)
}

#' ER quality metrics: CPM, modularity, silhouette, DB, CH, stability, pairwise & B3
#'
#' Unified helpers to score graph/vectors clusterings in ER.
#' - Graph objectives: CPM (resolution gamma), modularity.
#' - Vector metrics: Silhouette, Davies–Bouldin (DB), Calinski–Harabasz (CH).
#' - Stability: mean pairwise ARI across multiple runs (labels of same length).
#' - External with truth: Pairwise P/R/F1 (no explicit pair enumeration), B³ P/R/F1, ARI.
#'
#' @param g igraph graph (optional; for CPM/modularity).
#' @param membership Integer vector of cluster labels (length = n).
#' @param X Numeric matrix/data.frame of features/embeddings (rows = n) (optional; for silhouette/DB/CH).
#' @param D Optional distance object/matrix for silhouette (if provided, X is ignored for silhouette).
#' @param gamma Numeric CPM resolution parameter (default 0.05).
#' @param weights Optional edge weights vector matching E(g) (defaults to E(g)$weight if present).
#' @param stability_labels Optional list of label vectors (same length as membership) for stability ARI.
#' @param truth Optional integer/factor vector of gold cluster ids (length = n) for external metrics.
#' @param dist_method Distance method for silhouette if D is not given (default "euclidean").
#'
#' @return Named list with available metrics.
#' @export
#' @importFrom igraph vcount ecount induced_subgraph E modularity is.igraph
#' @importFrom stats dist
#' @importFrom cluster silhouette
er_eval_metrics <- function(
  g                = NULL,
  membership,
  X                = NULL,
  D                = NULL,
  gamma            = 0.05,
  weights          = NULL,
  stability_labels = NULL,
  truth            = NULL,
  dist_method      = "euclidean"
) {
  stopifnot(length(membership) >= 2L)

  out <- list(n = length(membership), k = length(unique(membership)))

  # ---------------- Graph-based objectives ----------------
  if (!is.null(g) && igraph::is.igraph(g)) {
    # Modularity (igraph’s standard modularity; resolution parameter is not exposed)
    out$modularity <- try_silent(igraph::modularity(g, membership, weights = weights))

    # CPM score (resolution-free baseline gamma)
    out$cpm <- try_silent(er_cpm_score(g, membership, gamma = gamma, weights = weights))
  }

  # ---------------- Vector-based internal indices ----------------
  if (!is.null(D) || !is.null(X)) {
    # Silhouette
    if (is.null(D)) {
      D <- stats::dist(X, method = dist_method)
    }
    sil <- try_silent(cluster::silhouette(membership, D))
    out$silhouette_mean <- if (!inherits(sil, "try-error")) mean(sil[, 3]) else NA_real_

    # DB & CH (implemented to avoid heavyweight deps)
    if (!is.null(X)) {
      db_ch <- try_silent(er_db_ch_indices(X, membership))
      if (!inherits(db_ch, "try-error")) {
        out$davies_bouldin <- db_ch$db
        out$calinski_harabasz <- db_ch$ch
      } else {
        out$davies_bouldin <- NA_real_
        out$calinski_harabasz <- NA_real_
      }
    }
  }

  # ---------------- Stability (mean ARI across runs) ----------------
  if (!is.null(stability_labels) && length(stability_labels) >= 2L) {
    out$stability_mean_ari <- try_silent(er_mean_ari(stability_labels))
  }

  # ---------------- External metrics (with truth) ----------------
  if (!is.null(truth)) {
    ext <- try_silent(er_external_scores(membership, truth))
    if (!inherits(ext, "try-error")) out <- c(out, ext)
  }

  out
}

#' CPM quality function for a partition on a graph
#' @param g igraph graph.
#' @param membership Integer vector of cluster labels.
#' @param gamma Numeric CPM resolution parameter. Default \code{0.05}.
#' @param weights Optional numeric edge weight vector; defaults to \code{E(g)$weight}.
#' @return Numeric CPM score: \eqn{\sum_c [e_c - \gamma n_c(n_c-1)/2]}.
#' @keywords internal
#' @export
er_cpm_score <- function(g, membership, gamma = 0.05, weights = NULL) {
  if (is.null(weights) && "weight" %in% igraph::edge_attr_names(g)) {
    weights <- igraph::E(g)$weight
  }
  # sum_c (e_c - gamma * n_c*(n_c-1)/2); e_c is edge count or weight sum inside c
  score <- 0
  for (lab in unique(membership)) {
    vids <- which(membership == lab)
    subg <- igraph::induced_subgraph(g, vids)
    n_c  <- igraph::vcount(subg)
    if (n_c < 2) next
    if (!is.null(weights) && length(weights) == igraph::ecount(g)) {
      # Map to subgraph edges
      # (simplest robust way: sum weights of edges present in subgraph)
      e_c <- sum(igraph::E(subg)$weight %||% rep(1, igraph::ecount(subg)))
    } else {
      e_c <- igraph::ecount(subg)
    }
    score <- score + (e_c - gamma * n_c * (n_c - 1) / 2)
  }
  as.numeric(score)
}

#' Davies–Bouldin and Calinski–Harabasz indices
#' @param X Numeric matrix with rows = records.
#' @param membership Integer vector of cluster labels (length = \code{nrow(X)}).
#' @return Named list with \code{db} (Davies–Bouldin; lower is better) and
#'   \code{ch} (Calinski–Harabasz; higher is better).
#' @keywords internal
#' @export
er_db_ch_indices <- function(X, membership) {
  X <- as.matrix(X)
  labs <- as.integer(factor(membership))
  k <- length(unique(labs))
  n <- nrow(X)
  if (k < 2L || n < k) return(list(db = NA_real_, ch = NA_real_))

  # cluster centers, scatters
  centers <- vapply(split(seq_len(n), labs), function(idx) colMeans(X[idx, , drop = FALSE]), numeric(ncol(X)))
  centers <- t(centers)           # k x p
  # within scatter (mean distance to center per cluster)
  wc <- vapply(seq_len(k), function(j) {
    idx <- which(labs == j); if (length(idx) == 0) return(0)
    mean(rowSums((X[idx, , drop = FALSE] - matrix(centers[j, ], nrow = length(idx), ncol = ncol(X), byrow = TRUE))^2))^0.5
  }, numeric(1))

  # Davies–Bouldin
  # R_ij = (S_i + S_j) / M_ij; DB = mean_i max_{j != i} R_ij
  dist_centers <- as.matrix(dist(centers))
  R <- matrix(0, k, k)
  for (i in 1:k) {
    for (j in 1:k) if (i != j) {
      R[i, j] <- (wc[i] + wc[j]) / max(dist_centers[i, j], .Machine$double.eps)
    }
  }
  db <- mean(apply(R + diag(-Inf, k), 1, max))

  # Calinski–Harabasz: (B/(k-1)) / (W/(n-k))
  grand <- colMeans(X)
  W <- 0; B <- 0
  for (j in 1:k) {
    idx <- which(labs == j); nj <- length(idx); if (nj == 0) next
    # within
    W <- W + sum(rowSums((X[idx, , drop = FALSE] - matrix(centers[j, ], nrow = nj, ncol = ncol(X), byrow = TRUE))^2))
    # between
    B <- B + nj * sum((centers[j, ] - grand)^2)
  }
  ch <- (B / max(k - 1, 1)) / (W / max(n - k, 1))
  list(db = as.numeric(db), ch = as.numeric(ch))
}

#' Mean ARI across a list of label vectors (stability)
#' @param labels_list List of integer vectors of equal length (one per clustering run).
#' @return Numeric mean pairwise ARI across all pairs of runs, or \code{NA} if fewer than 2 runs.
#' @export
er_mean_ari <- function(labels_list) {
  L <- length(labels_list)
  if (L < 2L) return(NA_real_)
  idx_pairs <- utils::combn(L, 2)
  aris <- apply(idx_pairs, 2, function(p) {
    mclust::adjustedRandIndex(labels_list[[p[1]]], labels_list[[p[2]]])
  })
  mean(aris)
}

#' External scores (pairwise P/R/F1, B³ P/R/F1, ARI)
#' @param pred Integer or factor vector of predicted cluster labels (length \code{n}).
#' @param truth Integer or factor vector of ground-truth entity labels (length \code{n}).
#' @return Named list with \code{pair_precision}, \code{pair_recall}, \code{pair_f1},
#'   \code{b3_precision}, \code{b3_recall}, \code{b3_f1}, and \code{ari}.
#' @export
er_external_scores <- function(pred, truth) {
  stopifnot(length(pred) == length(truth))
  pred  <- as.integer(factor(pred))
  truth <- as.integer(factor(truth))
  n <- length(pred)

  # Pairwise counts without enumerating all pairs
  n_pairs <- function(counts) sum(counts * (counts - 1) / 2)
  tab_pt <- table(pred, truth)             # k x g
  tp <- sum(tab_pt * (tab_pt - 1) / 2)     # sum over intersections C(s_ij, 2)
  pp <- n_pairs(rowSums(tab_pt))           # predicted pairs
  gp <- n_pairs(colSums(tab_pt))           # gold pairs

  precision <- if (pp > 0) tp / pp else 0
  recall    <- if (gp > 0) tp / gp else 0
  f1_pair   <- if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0

  # B³
  sizes_pred  <- rowSums(tab_pt)[match(pred, as.integer(rownames(tab_pt)))]
  sizes_truth <- colSums(tab_pt)[match(truth, as.integer(colnames(tab_pt)))]
  # overlap per item i is just count in its (pred, truth)
  overlap_vec <- tab_pt[cbind(pred, truth)][seq_len(n)]
  b3_precision <- mean(overlap_vec / sizes_pred)
  b3_recall    <- mean(overlap_vec / sizes_truth)
  b3_f1 <- if (b3_precision + b3_recall > 0) 2 * b3_precision * b3_recall / (b3_precision + b3_recall) else 0

  # ARI
  ari <- mclust::adjustedRandIndex(pred, truth)

  list(
    pair_precision = as.numeric(precision),
    pair_recall    = as.numeric(recall),
    pair_f1        = as.numeric(f1_pair),
    b3_precision   = as.numeric(b3_precision),
    b3_recall      = as.numeric(b3_recall),
    b3_f1          = as.numeric(b3_f1),
    ari            = as.numeric(ari)
  )
}

# %||% and try_silent are defined in R/00-utils.R
