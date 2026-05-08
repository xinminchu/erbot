########################################
# File: R/08-merge.R
# Cluster post-processing: transitivity enforcement, small-cluster
# absorption, and consensus merging across multiple methods.
########################################

# ── Transitivity enforcement ──────────────────────────────────────────────────

#' Enforce transitivity on a clustering
#'
#' Builds a graph from all pairs with \eqn{S(i,j) \ge \tau} and returns
#' connected components.  If \eqn{A \sim B} and \eqn{B \sim C}, then all three
#' are placed in the same cluster.
#'
#' @param S Symmetric \code{dgCMatrix} (n × n).
#' @param threshold Numeric. Similarity cut-off.  Default \code{0.5}.
#'
#' @return Integer vector of cluster labels (length n).
#' @export
er_transitivity <- function(S, threshold = 0.5) {
  n <- nrow(S)
  E <- Matrix::summary(S)
  E <- E[E$i < E$j & is.finite(E$x) & E$x >= threshold, , drop = FALSE]
  if (!nrow(E)) return(seq_len(n))
  g    <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                         vertices = data.frame(name = seq_len(n)))
  as.integer(igraph::components(g)$membership)
}

# ── Small-cluster absorption ──────────────────────────────────────────────────

#' Absorb small clusters into their nearest larger neighbour
#'
#' Clusters of size \eqn{< m\_min} are merged with the cluster that has the
#' highest mean similarity to their members.
#'
#' @param labels Integer vector of cluster labels (length n).
#' @param S Symmetric \code{dgCMatrix} (n × n).
#' @param m_min Integer. Minimum cluster size; clusters below this are
#'   absorbed. Default \code{2}.
#'
#' @return Integer vector of updated cluster labels.
#' @export
er_absorb_small <- function(labels, S, m_min = 2L) {
  labels <- as.integer(labels)
  n      <- length(labels)
  sizes  <- table(labels)

  small_ids <- as.integer(names(sizes[sizes < m_min]))
  if (!length(small_ids)) return(labels)

  large_ids <- setdiff(unique(labels), small_ids)
  if (!length(large_ids)) return(labels)

  S_mat <- as.matrix(S)

  for (cid in small_ids) {
    members <- which(labels == cid)
    # compute mean similarity from each member to each large cluster
    best_cid <- NA_integer_; best_score <- -Inf
    for (lid in large_ids) {
      lmembers <- which(labels == lid)
      score <- mean(S_mat[members, lmembers], na.rm = TRUE)
      if (is.finite(score) && score > best_score) {
        best_score <- score; best_cid <- lid
      }
    }
    if (!is.na(best_cid)) labels[members] <- best_cid
  }

  # Re-index to consecutive integers
  as.integer(factor(labels))
}

# ── Consensus merging ─────────────────────────────────────────────────────────

#' Consensus clustering from multiple methods
#'
#' A pair \eqn{(i,j)} is co-clustered in the consensus if at least fraction
#' \code{alpha} of the input methods place them in the same cluster.
#' Connected components of the consensus co-membership graph define clusters.
#'
#' @param cluster_list Named list of integer label vectors (length n each).
#' @param n Integer. Total number of records.
#' @param alpha Numeric in \eqn{(0,1]}. Fraction of methods that must agree
#'   to merge a pair.  \code{0.5} = majority vote (default).  \code{1.0}
#'   = intersection (all agree).
#'
#' @return Integer vector of consensus cluster labels.
#' @export
er_consensus <- function(cluster_list, n, alpha = 0.5) {
  M    <- length(cluster_list)
  if (M == 0L) stop("er_consensus: cluster_list is empty.")
  thresh <- max(1L, ceiling(alpha * M))

  # Count co-membership across all pairs
  # Use efficient approach: build co-membership adjacency matrix by summing
  # outer products (1 for same cluster, 0 otherwise).
  co_count <- Matrix::Matrix(0, n, n, sparse = TRUE)
  for (labs in cluster_list) {
    labs <- as.integer(labs)
    # For each cluster, add a 1 to all pairs within it
    for (cid in unique(labs)) {
      idx <- which(labs == cid)
      if (length(idx) < 2L) next
      m    <- length(idx)
      gr_i <- rep(idx, times = m)
      gr_j <- rep(idx, each  = m)
      keep <- gr_i < gr_j
      co_count <- co_count +
        Matrix::sparseMatrix(i = gr_i[keep], j = gr_j[keep],
                              x = rep(1, sum(keep)), dims = c(n, n), repr = "C")
    }
  }

  # Edges where at least thresh methods agree
  E <- Matrix::summary(co_count)
  E <- E[E$x >= thresh, , drop = FALSE]
  if (!nrow(E)) return(seq_len(n))

  g <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                      vertices = data.frame(name = seq_len(n)))
  as.integer(igraph::components(g)$membership)
}

# ── er_merge() ── main entry point ────────────────────────────────────────────

#' Post-process clustering results
#'
#' Applies one or more post-processing steps to produce a final clustering.
#'
#' @param cluster_list Named list of integer label vectors from
#'   \code{er_cluster_all()}.
#' @param S Symmetric \code{dgCMatrix} (n × n).
#' @param method Character. Post-processing strategy:
#'   \describe{
#'     \item{\code{"transitivity"}}{Build connected components at
#'       \code{threshold} on the best single input method (highest ARI if
#'       truth given, else first method).}
#'     \item{\code{"consensus"}}{Majority-vote co-membership across all
#'       methods (see \code{er_consensus()}).}
#'     \item{\code{"best"}}{Return the single best method's labels without
#'       further modification (best = highest ARI if truth provided, else
#'       first method).}
#'     \item{\code{"none"}}{Return \code{cluster_list[[1]]} unchanged.}
#'   }
#' @param threshold Numeric. Similarity threshold for transitivity merge.
#'   Default \code{0.5}.
#' @param alpha Numeric. Consensus fraction for \code{"consensus"} method.
#'   Default \code{0.5}.
#' @param absorb_small Logical. Apply small-cluster absorption after merging.
#'   Default \code{FALSE}.
#' @param m_min Integer. Minimum cluster size for absorption. Default \code{2}.
#' @param truth_vec Optional ground-truth integer vector for method selection.
#'
#' @return Integer vector of final cluster labels (length n).
#' @export
er_merge <- function(cluster_list, S,
                     method       = "transitivity",
                     threshold    = 0.5,
                     alpha        = 0.5,
                     absorb_small = FALSE,
                     m_min        = 2L,
                     truth_vec    = NULL) {

  method <- match.arg(method, c("transitivity", "consensus", "best", "none"))
  n      <- nrow(S)
  if (!length(cluster_list)) stop("er_merge: cluster_list is empty.")

  # Pick "best" method for transitivity/best strategies
  .pick_best <- function() {
    if (!is.null(truth_vec) && requireNamespace("GCMER", quietly = TRUE)) {
      valid <- !is.na(truth_vec)
      if (sum(valid) < 2L) return(cluster_list[[1L]])
      aris <- vapply(cluster_list, function(labs)
        tryCatch(GCMER::adj_rand(as.integer(labs)[valid], truth_vec[valid]),
                 error = function(e) -Inf),
        numeric(1L))
      cluster_list[[which.max(aris)]]
    } else {
      cluster_list[[1L]]
    }
  }

  labels <- switch(method,
    "none"         = as.integer(cluster_list[[1L]]),
    "best"         = as.integer(.pick_best()),
    "transitivity" = er_transitivity(S, threshold = threshold),
    "consensus"    = er_consensus(cluster_list, n = n, alpha = alpha)
  )

  if (absorb_small) labels <- er_absorb_small(labels, S, m_min = m_min)
  as.integer(labels)
}
