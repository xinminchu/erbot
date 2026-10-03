########################################
# File: R/13-normalize.R
# Normalize per-field similarity vectors or sparse matrices before fusion.
#
# er_normalize()  -- main entry point; dispatches on input type
#
# Internal helpers (not exported):
#   .norm_vec_minmax, .norm_vec_zscore
#   .norm_mat_minmax, .norm_mat_zscore
#   .norm_row_stochastic, .norm_laplacian
########################################

#' Normalize similarity vectors or matrices
#'
#' Scales per-field similarity objects so that values are comparable across
#' fields before combination or fusion.  Accepts a pair-level numeric vector
#' (output of \code{er_similarity()}), a sparse \code{dgCMatrix}, or a named
#' list of either.
#'
#' \strong{Methods available for numeric vectors:}
#' \describe{
#'   \item{\code{"minmax"}}{Linearly scale non-NA values to \eqn{[0,1]}.}
#'   \item{\code{"zscore"}}{Standardize (subtract mean, divide by SD), then
#'     clip to \eqn{[0,1]}.}
#' }
#'
#' \strong{Additional methods available for sparse matrices:}
#' \describe{
#'   \item{\code{"row_stochastic"}}{Row-normalize so each row sums to 1.
#'     Required by \code{er_snf()}.}
#'   \item{\code{"laplacian"}}{Symmetric normalized Laplacian
#'     \eqn{L = D^{-1/2}(D - S)D^{-1/2}}.  Use before spectral clustering
#'     (Method 2).}
#' }
#'
#' @param S Numeric vector, sparse \code{dgCMatrix}, or named list of either.
#'   Lists are normalized element-wise.
#' @param method Character.  One of \code{"minmax"} (default),
#'   \code{"zscore"}, \code{"row_stochastic"}, \code{"laplacian"}.
#' @param symmetric Logical.  For matrix inputs, symmetrize the result via
#'   \eqn{(S + S^T)/2} after normalizing.  Ignored for
#'   \code{method = "laplacian"}.  Default \code{TRUE}.
#'
#' @return Normalized object of the same class as \code{S}.
#'
#' @examples
#' \dontrun{
#' sim      <- er_similarity(df, pairs)
#' sim_norm <- er_normalize(sim, method = "minmax")   # normalize per field
#' S        <- er_pairs_to_sparse(pairs, er_combine(sim_norm), n)
#' S_rs     <- er_normalize(S, method = "row_stochastic")  # for SNF input
#' }
#' @export
er_normalize <- function(S,
                         method    = c("minmax", "zscore",
                                       "row_stochastic", "laplacian"),
                         symmetric = TRUE) {
  method <- match.arg(method)

  # Named list: recurse element-wise
  if (is.list(S))
    return(lapply(S, er_normalize, method = method, symmetric = symmetric))

  # Pair-level numeric vector
  if (is.numeric(S) && !inherits(S, "Matrix")) {
    if (method %in% c("row_stochastic", "laplacian"))
      stop("er_normalize: method '", method, "' requires a sparse matrix. ",
           "Call er_pairs_to_sparse() first.")
    return(switch(method,
      minmax = .norm_vec_minmax(S),
      zscore = .norm_vec_zscore(S)
    ))
  }

  # Sparse / dense matrix
  if (!inherits(S, "sparseMatrix"))
    S <- Matrix::Matrix(S, sparse = TRUE)

  out <- switch(method,
    minmax         = .norm_mat_minmax(S),
    zscore         = .norm_mat_zscore(S),
    row_stochastic = .norm_row_stochastic(S),
    laplacian      = .norm_laplacian(S)
  )

  if (symmetric && method != "laplacian")
    out <- (out + Matrix::t(out)) / 2

  out
}

# ── Numeric-vector normalizers ─────────────────────────────────────────────────

.norm_vec_minmax <- function(v) {
  ok <- !is.na(v)
  if (!any(ok)) return(v)
  mn <- min(v[ok]); mx <- max(v[ok])
  if (mx == mn) return(v)
  v[ok] <- (v[ok] - mn) / (mx - mn)
  v
}

.norm_vec_zscore <- function(v) {
  ok <- !is.na(v)
  if (sum(ok) < 2L) return(v)
  m <- mean(v[ok]); s <- stats::sd(v[ok])
  if (s == 0) return(v)
  v[ok] <- pmax(0, pmin(1, (v[ok] - m) / s))
  v
}

# ── Sparse-matrix normalizers ──────────────────────────────────────────────────

.norm_mat_minmax <- function(S) {
  if (!length(S@x)) return(S)
  mn <- min(S@x); mx <- max(S@x)
  if (mx == mn) return(S)
  S@x <- (S@x - mn) / (mx - mn)
  S
}

.norm_mat_zscore <- function(S) {
  if (length(S@x) < 2L) return(S)
  m <- mean(S@x); s <- stats::sd(S@x)
  if (s == 0) return(S)
  S@x <- pmax(0, pmin(1, (S@x - m) / s))
  S
}

.norm_row_stochastic <- function(S) {
  rs <- Matrix::rowSums(S)
  rs[rs == 0] <- 1   # all-zero rows stay zero after multiplication
  Matrix::Diagonal(x = 1 / rs) %*% S
}

.norm_laplacian <- function(S) {
  n  <- nrow(S)
  d  <- Matrix::rowSums(S)
  di <- ifelse(d > 0, 1 / sqrt(d), 0)
  Di <- Matrix::Diagonal(x = di)
  D  <- Matrix::Diagonal(x = d)
  Di %*% (D - S) %*% Di   # symmetric normalized Laplacian
}
