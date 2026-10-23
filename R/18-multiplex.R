########################################
# File: R/18-multiplex.R
# Multiplex community detection for multi-view ER (Strategy C).
#
# er_multiplex()      -- main entry point
# .multiplex_supra    -- internal: supra-adjacency Louvain (small n)
# .multiplex_aggregate-- internal: weighted aggregate Louvain (large n)
########################################

#' Multiplex community detection for multi-view entity resolution
#'
#' Strategy~C (Multiplex) treats each field's similarity matrix as a layer of a
#' multiplex graph and maximises joint modularity across layers.  Two variants
#' are provided based on dataset size:
#'
#' \describe{
#'   \item{Supra-adjacency (\eqn{n \le \code{supra\_max\_n}})}{
#'     Builds an \eqn{(p \cdot n) \times (p \cdot n)} supra-adjacency matrix
#'     with within-layer similarity edges and between-layer coupling edges
#'     (weight \eqn{\omega}), then runs Louvain/Leiden on the expanded graph.
#'     Each record is assigned the majority-vote community of its \eqn{p}
#'     layer copies.
#'   }
#'   \item{Aggregate (\eqn{n > \code{supra\_max\_n}})}{
#'     Forms a single weighted graph \eqn{S_{\text{agg}} = \tfrac{1}{p}
#'     \sum_k S^{(k)}} and runs Louvain with resolution \eqn{\gamma}.
#'     The coupling \eqn{\omega} is not modelled explicitly but its effect is
#'     captured implicitly by the uniform layer averaging.
#'   }
#' }
#'
#' \strong{Typical usage (Exp~1, Scenario S4):}
#' \enumerate{
#'   \item Compute per-field similarities: \code{er_similarity()}.
#'   \item Build sparse matrices: \code{er_pairs_to_sparse()}.
#'   \item Fuse and cluster: \code{er_multiplex(S_list, gamma, omega)}.
#' }
#'
#' @param S_list Named list of \eqn{p \ge 2} sparse \code{dgCMatrix} objects,
#'   each \eqn{n \times n} with non-negative entries.  Built by
#'   \code{er_pairs_to_sparse()}.  Accepts numeric vectors if \code{pairs} and
#'   \code{n} are supplied.
#' @param gamma Numeric.  Resolution parameter for the Louvain/Leiden
#'   modularity objective.  Higher values produce more communities.
#'   Default \code{1.0}.
#' @param omega Numeric.  Inter-layer coupling weight (\eqn{[0, \infty)}).
#'   Only used in the supra-adjacency variant.  Default \code{0.5}.
#' @param method Character.  Community detection algorithm: \code{"louvain"}
#'   (default) or \code{"leiden"}.
#' @param pairs \code{tibble(idx1, idx2)} from \code{er_block()}.  Required
#'   only when \code{S_list} contains numeric vectors.
#' @param n Integer.  Total number of records.  Required only when
#'   \code{S_list} contains numeric vectors.
#' @param supra_max_n Integer.  Records below this use the supra-adjacency
#'   approach; above it the aggregate approach is used.  Default \code{3000L}.
#' @param seed Integer.  RNG seed for Louvain.  Default \code{42L}.
#' @param verbose Logical.  Print progress.  Default \code{FALSE}.
#'
#' @return Integer vector of cluster labels (length \code{n}).  Pass to
#'   \code{er_evaluate()} for assessment.
#'
#' @references
#' Mucha, P. J., Richardson, T., Macon, K., Porter, M. A., & Onnela, J.-P.
#' (2010).  Community structure in time-dependent, multiscale, and multiplex
#' networks.  \emph{Science}, \strong{328}(5980), 876--878.
#'
#' Blondel, V. D., Guillaume, J.-L., Lambiotte, R., & Lefebvre, E. (2008).
#' Fast unfolding of communities in large networks.
#' \emph{Journal of Statistical Mechanics}, \strong{2008}(10), P10008.
#'
#' @seealso \code{\link{er_snf}}, \code{\link{er_compare_paradigms}},
#'   \code{\link{er_cluster}}
#'
#' @examples
#' \dontrun{
#' df    <- er_load("restaurant")
#' diag  <- er_diagnose(df)
#' pairs <- er_block(df, diag)
#' sim   <- er_similarity(df, pairs)
#' n     <- nrow(df)
#'
#' S_list <- lapply(sim, function(sv) er_pairs_to_sparse(pairs, sv, n))
#' labs   <- er_multiplex(S_list, gamma = 1.0, omega = 0.5)
#' }
#' @export
er_multiplex <- function(S_list,
                          gamma       = 1.0,
                          omega       = 0.5,
                          method      = c("louvain", "leiden"),
                          pairs       = NULL,
                          n           = NULL,
                          supra_max_n = 3000L,
                          seed        = 42L,
                          verbose     = FALSE) {

  method <- match.arg(method)

  if (!requireNamespace("igraph", quietly = TRUE))
    stop("er_multiplex requires igraph. Install: install.packages('igraph')")

  if (!is.list(S_list) || length(S_list) < 2L)
    stop("er_multiplex: S_list must be a list of at least 2 matrices.")

  # Coerce numeric vectors to sparse matrices if pairs + n are supplied
  first_elem <- S_list[[1L]]
  if (is.numeric(first_elem) && !inherits(first_elem, "Matrix")) {
    if (is.null(pairs) || is.null(n))
      stop("er_multiplex: when S_list contains numeric vectors, ",
           "'pairs' and 'n' must be provided.")
    n      <- as.integer(n)
    S_list <- lapply(S_list, function(sv)
      er_pairs_to_sparse(pairs, sv, n, na_fill = 0))
  }

  p <- length(S_list)
  n <- nrow(S_list[[1L]])

  # Validate all matrices have compatible dimensions
  for (v in seq_len(p)) {
    S <- S_list[[v]]
    if (!inherits(S, "sparseMatrix"))
      S_list[[v]] <- Matrix::Matrix(S, sparse = TRUE)
    if (nrow(S_list[[v]]) != n || ncol(S_list[[v]]) != n)
      stop(sprintf("er_multiplex: matrix %d has dimensions %dx%d, expected %dx%d.",
                   v, nrow(S_list[[v]]), ncol(S_list[[v]]), n, n))
    S_list[[v]] <- methods::as(S_list[[v]], "dgCMatrix")
  }

  if (verbose)
    message(sprintf("[er_multiplex] p=%d layers, n=%d records, gamma=%.2f, omega=%.2f",
                    p, n, gamma, omega))

  if (n <= supra_max_n) {
    .multiplex_supra(S_list, n = n, p = p, gamma = gamma, omega = omega,
                     method = method, seed = seed, verbose = verbose)
  } else {
    if (verbose)
      message(sprintf("[er_multiplex] n=%d > supra_max_n=%d; using aggregate approach.",
                      n, supra_max_n))
    .multiplex_aggregate(S_list, n = n, p = p, gamma = gamma,
                         method = method, seed = seed, verbose = verbose)
  }
}

# ── Supra-adjacency approach ───────────────────────────────────────────────────

# Builds a (p*n) x (p*n) supra-adjacency graph:
#   - Diagonal p blocks: within-layer similarity edges (weighted by S^(k))
#   - Off-diagonal coupling: omega * I_n between every pair of adjacent layers
# Runs Louvain/Leiden on the full supra-graph, then majority-votes community
# of the p copies of each record back to a length-n label vector.
.multiplex_supra <- function(S_list, n, p, gamma, omega, method, seed,
                              verbose) {

  # Collect all edges as (from, to, weight) triples in supra-graph coordinates.
  # Layer k occupies node indices [(k-1)*n + 1, k*n].
  edges_i <- integer(0L)
  edges_j <- integer(0L)
  edges_x <- numeric(0L)

  # Within-layer edges
  for (k in seq_len(p)) {
    tr   <- Matrix::summary(S_list[[k]])
    # Keep upper triangle of positive entries
    keep <- tr$i < tr$j & is.finite(tr$x) & tr$x > 0
    tr   <- tr[keep, , drop = FALSE]
    if (nrow(tr)) {
      offset   <- (k - 1L) * n
      edges_i  <- c(edges_i, tr$i + offset)
      edges_j  <- c(edges_j, tr$j + offset)
      edges_x  <- c(edges_x, tr$x)
    }
  }

  # Between-layer coupling edges (adjacent layers only: k <-> k+1)
  if (omega > 0 && p > 1L) {
    for (k in seq_len(p - 1L)) {
      off_a <- (k - 1L) * n
      off_b <- k * n
      # Diagonal (same record across layers)
      edges_i <- c(edges_i, seq_len(n) + off_a)
      edges_j <- c(edges_j, seq_len(n) + off_b)
      edges_x <- c(edges_x, rep(omega, n))
    }
  }

  N_supra <- p * n

  if (!length(edges_i)) {
    if (verbose) message("[er_multiplex] supra graph has no edges; returning singletons.")
    return(seq_len(n))
  }

  # Build supra-adjacency graph (undirected; igraph symmetrizes automatically)
  g_supra <- igraph::graph_from_data_frame(
    data.frame(from = edges_i, to = edges_j, weight = edges_x),
    directed  = FALSE,
    vertices  = data.frame(name = seq_len(N_supra))
  )
  igraph::E(g_supra)$weight <- edges_x

  # BUG-19 (2026-06-12): preserve the caller's global RNG state. The same
  # set.seed(seed) still runs, so all sampling inside this function is
  # byte-identical to previous behaviour; only the side effect on the
  # caller's RNG stream is removed.
  .rng_old <- if (exists(".Random.seed", envir = globalenv(), inherits = FALSE))
    get(".Random.seed", envir = globalenv(), inherits = FALSE) else NULL
  on.exit({
    if (!is.null(.rng_old)) {
      assign(".Random.seed", .rng_old, envir = globalenv())
    } else if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
      rm(".Random.seed", envir = globalenv())
    }
  }, add = TRUE)
  set.seed(seed)
  cl <- if (method == "leiden") {
    tryCatch(
      igraph::cluster_leiden(g_supra,
                              weights              = igraph::E(g_supra)$weight,
                              resolution_parameter = gamma),
      error = function(e) {
        if (verbose) message("[er_multiplex] leiden failed, falling back to louvain: ",
                             conditionMessage(e))
        igraph::cluster_louvain(g_supra,
                                weights    = igraph::E(g_supra)$weight,
                                resolution = gamma)
      }
    )
  } else {
    igraph::cluster_louvain(g_supra,
                            weights    = igraph::E(g_supra)$weight,
                            resolution = gamma)
  }

  supra_memb <- as.integer(igraph::membership(cl))

  # Map supra-node communities back to records by majority vote across layers
  labels <- integer(n)
  for (i in seq_len(n)) {
    # Indices of the p copies of record i in the supra-graph
    copies      <- i + (seq_len(p) - 1L) * n
    copy_comms  <- supra_memb[copies]
    tbl         <- sort(table(copy_comms), decreasing = TRUE)
    labels[i]   <- as.integer(names(tbl)[1L])
  }

  # Re-index to consecutive 1..K integers
  as.integer(factor(labels))
}

# ── Aggregate approach (large n) ───────────────────────────────────────────────

# Forms a single weighted graph by layer-averaging all similarity matrices,
# then runs Louvain/Leiden with resolution gamma.
.multiplex_aggregate <- function(S_list, n, p, gamma, method, seed, verbose) {

  S_agg <- Reduce("+", S_list) / p

  tr   <- Matrix::summary(S_agg)
  keep <- tr$i < tr$j & is.finite(tr$x) & tr$x > 0
  tr   <- tr[keep, , drop = FALSE]

  if (!nrow(tr)) {
    if (verbose) message("[er_multiplex] aggregate graph has no edges; returning singletons.")
    return(seq_len(n))
  }

  g <- igraph::graph_from_data_frame(
    data.frame(from = tr$i, to = tr$j, weight = tr$x),
    directed = FALSE,
    vertices = data.frame(name = seq_len(n))
  )
  igraph::E(g)$weight <- tr$x

  # BUG-19 (2026-06-12): preserve the caller's global RNG state. The same
  # set.seed(seed) still runs, so all sampling inside this function is
  # byte-identical to previous behaviour; only the side effect on the
  # caller's RNG stream is removed.
  .rng_old <- if (exists(".Random.seed", envir = globalenv(), inherits = FALSE))
    get(".Random.seed", envir = globalenv(), inherits = FALSE) else NULL
  on.exit({
    if (!is.null(.rng_old)) {
      assign(".Random.seed", .rng_old, envir = globalenv())
    } else if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
      rm(".Random.seed", envir = globalenv())
    }
  }, add = TRUE)
  set.seed(seed)
  cl <- if (method == "leiden") {
    tryCatch(
      igraph::cluster_leiden(g,
                              weights              = igraph::E(g)$weight,
                              resolution_parameter = gamma),
      error = function(e) {
        if (verbose) message("[er_multiplex] leiden failed, falling back to louvain: ",
                             conditionMessage(e))
        igraph::cluster_louvain(g,
                                weights    = igraph::E(g)$weight,
                                resolution = gamma)
      }
    )
  } else {
    igraph::cluster_louvain(g,
                            weights    = igraph::E(g)$weight,
                            resolution = gamma)
  }

  memb <- rep(NA_integer_, n)
  memb[as.integer(igraph::V(g)$name)] <- as.integer(igraph::membership(cl))
  # Singletons (no edges) stay as unique labels
  n_assigned <- max(memb, na.rm = TRUE)
  na_idx     <- which(is.na(memb))
  if (length(na_idx))
    memb[na_idx] <- n_assigned + seq_along(na_idx)

  as.integer(factor(memb))
}
