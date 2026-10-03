########################################
# File: R/15-snf.R
# Similarity Network Fusion (Wang et al. 2014) for multi-view ER.
#
# er_snf()        -- fuse a list of per-field sparse similarity matrices
# .snf_knn_sparse -- internal: retain top-K neighbours per row
########################################

#' Similarity Network Fusion (SNF)
#'
#' Fuses \eqn{p} per-field similarity matrices via iterative graph diffusion
#' (Wang et al.\ 2014).  Each matrix is converted to a row-stochastic
#' \emph{status} matrix; views iteratively exchange neighbourhood information;
#' the averaged final matrices form the fused graph.
#'
#' \strong{Typical usage (Strategy C in \code{er_compare_paradigms()}):}
#' \enumerate{
#'   \item Build one sparse matrix per field: \code{er_pairs_to_sparse()}.
#'   \item Row-normalize each: \code{er_normalize(S_list, "row_stochastic")}.
#'   \item Fuse: \code{er_snf(S_list)}.
#'   \item Classify: \code{er_classify(S_fused)}.
#'   \item Cluster: \code{er_cluster(M, "threshold_cc")}.
#' }
#'
#' @param S_list Named list of \eqn{p \ge 2} sparse \code{dgCMatrix} objects,
#'   each \eqn{n \times n}.  Built by \code{er_pairs_to_sparse()}, optionally
#'   row-normalized by \code{er_normalize(..., "row_stochastic")}.
#' @param K Integer.  Nearest-neighbour count used to sparsify the status
#'   matrices.  Records with fewer than \code{K} candidates retain all
#'   neighbours.  Default \code{20L}.
#' @param t Integer.  Diffusion iterations.  Default \code{20L}.
#' @param warn_large Integer.  Warn when \eqn{n > \code{warn\_large}} because
#'   matrix multiplications scale as \eqn{O(n^2)}.  Default \code{5000L}.
#'
#' @return A symmetric, row-stochastic \code{dgCMatrix} (\eqn{n \times n}).
#'   Pass directly to \code{er_classify()}.
#'
#' @references
#' Wang, B., Mezlini, A. M., Demir, F., Fiume, M., Tu, Z., Brusic, V.,
#' Goldenberg, A. (2014).  Similarity network fusion for aggregating data
#' types on a genomic scale.  \emph{Nature Methods}, \strong{11}(3), 333--337.
#'
#' @seealso \code{\link{er_normalize}}, \code{\link{er_classify}},
#'   \code{\link{er_compare_paradigms}}
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
#' S_list <- er_normalize(S_list, "row_stochastic")
#' S_fused <- er_snf(S_list, K = 20, t = 20)
#' M       <- er_classify(S_fused, threshold = 0.5)
#' labs    <- er_cluster(M, method = "threshold_cc", threshold = 0.5)
#' }
#' @export
er_snf <- function(S_list, K = 20L, t = 20L, warn_large = 5000L) {
  if (!is.list(S_list) || length(S_list) < 2L)
    stop("er_snf: S_list must be a named list of at least 2 sparse matrices.")

  K <- as.integer(K)
  t <- as.integer(t)
  p <- length(S_list)
  n <- nrow(S_list[[1]])

  if (n > warn_large)
    warning(sprintf(
      "er_snf: n = %d > %d. Matrix multiplications are O(n^2); may be slow.",
      n, warn_large))

  # Coerce to dgCMatrix and validate dimensions
  S_list <- lapply(seq_len(p), function(v) {
    S <- S_list[[v]]
    if (!inherits(S, "sparseMatrix"))
      S <- Matrix::Matrix(S, sparse = TRUE)
    if (nrow(S) != n || ncol(S) != n)
      stop(sprintf("er_snf: matrix %d has wrong dimensions (%d x %d), expected %d x %d.",
                   v, nrow(S), ncol(S), n, n))
    methods::as(S, "dgCMatrix")
  })

  # Step 1: Row-normalize each matrix (W0 = fixed full status matrix)
  W0_list <- lapply(S_list, .norm_row_stochastic)

  # Step 2: KNN-sparse version of W0 (P = diffusing status matrix)
  P_list <- lapply(W0_list, function(W) .snf_knn_sparse(W, K = K))

  # Step 3: Diffusion iterations
  for (iter in seq_len(t)) {
    P_sum <- Reduce("+", P_list)
    P_new <- vector("list", p)
    for (v in seq_len(p)) {
      # Cross-view mean: (sum of all P) minus current view, divided by (p-1)
      cross  <- (P_sum - P_list[[v]]) / (p - 1L)
      # Update rule: W0^(v) x cross x W0^(v)^T, then row-normalize
      upd    <- W0_list[[v]] %*% cross %*% Matrix::t(W0_list[[v]])
      P_new[[v]] <- .norm_row_stochastic(upd)
    }
    P_list <- P_new
  }

  # Step 4: Average final P matrices, row-normalize, symmetrize
  fused <- Reduce("+", P_list) / p
  fused <- .norm_row_stochastic(fused)
  fused <- (fused + Matrix::t(fused)) / 2
  methods::as(fused, "dgCMatrix")
}

# ── KNN sparsification ─────────────────────────────────────────────────────────

# Keep only the top-K similarity values per row; zero out the rest.
# Operates on the sparse triplet representation to avoid dense intermediates.
.snf_knn_sparse <- function(W, K) {
  n <- nrow(W)
  if (K >= n) return(W)

  tr <- Matrix::summary(W)   # data.frame(i, j, x)
  if (!nrow(tr)) return(W)

  # Rank within each row (descending similarity); keep rank <= K
  tr$rnk <- stats::ave(tr$x, tr$i,
                        FUN = function(x) rank(-x, ties.method = "first"))
  tr <- tr[tr$rnk <= K, , drop = FALSE]
  if (!nrow(tr)) return(W)

  S_knn <- Matrix::sparseMatrix(i    = tr$i,
                                 j    = tr$j,
                                 x    = tr$x,
                                 dims = c(n, n))
  .norm_row_stochastic(S_knn)
}
