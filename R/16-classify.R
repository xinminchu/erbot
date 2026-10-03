########################################
# File: R/16-classify.R
# Binary classification of record pairs: similarity -> match/non-match.
#
# er_classify()           -- main entry point
# .classify_gmm_threshold -- internal: 2-component Gaussian EM boundary
########################################

#' Classify record pairs as match or non-match
#'
#' Converts a combined similarity matrix \eqn{S} into a binary match graph
#' \eqn{M} (sparse 0/1 matrix) by thresholding or by fitting a 2-component
#' Gaussian mixture model (GMM) via EM.
#'
#' This step sits explicitly between fusion and clustering:
#' \deqn{\{S^{(k)}\}
#'   \;\xrightarrow{\text{fuse}}\; S
#'   \;\xrightarrow{\text{er\_classify}}\; M_{ij}\in\{0,1\}
#'   \;\xrightarrow{\text{er\_cluster}}\; \hat{\mathcal{C}}}
#'
#' \strong{Why an explicit classification step?}
#' Canonical Fellegi-Sunter ER always classifies pairs as match / non-match
#' before grouping.  Clustering on raw similarity \eqn{S} directly lacks a
#' principled decision boundary and makes over-merge vs.\ over-split errors
#' invisible.  Reporting separate precision and recall (via
#' \code{er_pairwise_f()}) requires this binary output.
#'
#' \strong{Methods:}
#' \describe{
#'   \item{\code{"threshold"}}{Classify pair \eqn{(i,j)} as a match iff
#'     \eqn{S_{ij} \ge \tau}.  Fast and interpretable.}
#'   \item{\code{"gmm"}}{Fit a 2-component Gaussian mixture to the non-zero
#'     similarity values via EM; use the decision boundary (density crossing)
#'     as \eqn{\tau}.  Falls back to \code{threshold} if EM fails or if
#'     fewer than 20 pairs are available.}
#' }
#'
#' @param S A symmetric sparse \code{dgCMatrix} (\eqn{n \times n}), such as
#'   the output of \code{er_pairs_to_sparse()} + \code{er_combine()}, or
#'   \code{er_snf()}.
#' @param method Classification method.  Default \code{"threshold"}.
#' @param threshold Numeric in \eqn{[0,1]}.  Decision boundary for
#'   \code{"threshold"}; also used as the fallback for \code{"gmm"}.
#'   Default \code{0.5}.
#' @param verbose Logical.  Print the threshold used and the match-pair count.
#'   Default \code{FALSE}.
#'
#' @return A symmetric sparse \code{dgCMatrix} with values in \eqn{\{0,1\}}.
#'   Pass to \code{er_cluster(M, method = "threshold_cc", threshold = 0.5)}.
#'
#' @seealso \code{\link{er_combine}}, \code{\link{er_snf}},
#'   \code{\link{er_cluster}}, \code{\link{er_compare_paradigms}}
#'
#' @examples
#' \dontrun{
#' sim  <- er_similarity(df, pairs)
#' wt   <- er_weights(sim)
#' S    <- er_pairs_to_sparse(pairs, er_combine(sim, wt), n)
#' M    <- er_classify(S, method = "threshold", threshold = 0.5)
#' labs <- er_cluster(M, method = "threshold_cc", threshold = 0.5)
#'
#' # GMM-based adaptive threshold
#' M_gmm <- er_classify(S, method = "gmm", verbose = TRUE)
#' }
#' @export
er_classify <- function(S,
                        method    = c("threshold", "gmm"),
                        threshold = 0.5,
                        verbose   = FALSE) {
  method <- match.arg(method)

  if (!inherits(S, "sparseMatrix"))
    stop("er_classify: S must be a sparse matrix (dgCMatrix). ",
         "Use er_pairs_to_sparse() or er_snf() to build it.")

  tau <- threshold

  if (method == "gmm") {
    vals <- S@x[is.finite(S@x) & S@x > 0 & S@x < 1]
    if (length(vals) >= 20L) {
      tau <- tryCatch(
        .classify_gmm_threshold(vals, fallback = threshold),
        error = function(e) {
          warning("er_classify: GMM failed (", conditionMessage(e),
                  "). Falling back to threshold = ", threshold, ".")
          threshold
        }
      )
    } else {
      if (verbose)
        message("er_classify: fewer than 20 non-trivial pairs; using threshold.")
    }
  }

  # Build binary match matrix: entries >= tau become 1, rest 0
  tr   <- Matrix::summary(S)
  keep <- tr$x >= tau
  if (!any(keep)) {
    if (verbose) message("er_classify: no pairs exceed threshold ", tau,
                         "; all pairs classified as non-match.")
    return(Matrix::sparseMatrix(i = integer(0), j = integer(0),
                                x = numeric(0), dims = dim(S)))
  }

  M <- Matrix::sparseMatrix(
    i    = tr$i[keep],
    j    = tr$j[keep],
    x    = rep(1, sum(keep)),
    dims = dim(S)
  )
  # Symmetrize and remove diagonal
  M <- Matrix::forceSymmetric(M)
  diag(M) <- 0
  M <- Matrix::drop0(M)
  M <- as(as(M, "generalMatrix"), "dgCMatrix")

  if (verbose)
    message(sprintf("er_classify: tau = %.4f; %d match pairs (upper triangle).",
                    tau, sum(Matrix::summary(M)$i < Matrix::summary(M)$j)))
  M
}

# ── 2-component Gaussian EM ────────────────────────────────────────────────────

# Fits a 2-component Gaussian mixture to 'vals' via EM and returns the
# decision boundary (x-value where match density = non-match density).
# Falls back to 'fallback' if components don't separate or EM fails.
.classify_gmm_threshold <- function(vals, fallback = 0.5, max_iter = 50L) {
  med  <- stats::median(vals)
  lo   <- vals[vals <  med]
  hi   <- vals[vals >= med]

  mu  <- c(if (length(lo)) mean(lo) else med * 0.5,
            if (length(hi)) mean(hi) else med * 1.5)
  sg  <- c(max(if (length(lo) > 1L) stats::sd(lo) else 0.1, 1e-4),
            max(if (length(hi) > 1L) stats::sd(hi) else 0.1, 1e-4))
  pi_vec <- c(length(lo), length(hi)) / length(vals)
  pi_vec <- pmax(pi_vec, 0.01)
  pi_vec <- pi_vec / sum(pi_vec)

  for (iter in seq_len(max_iter)) {
    d1  <- pi_vec[1] * stats::dnorm(vals, mu[1], sg[1])
    d2  <- pi_vec[2] * stats::dnorm(vals, mu[2], sg[2])
    tot <- d1 + d2
    tot[tot <= 0] <- 1e-300
    r1  <- d1 / tot
    r2  <- 1 - r1

    pi_new <- c(mean(r1), mean(r2))
    pi_new <- pmax(pi_new, 0.01)
    pi_new <- pi_new / sum(pi_new)

    s1     <- max(sum(r1), 1e-10)
    s2     <- max(sum(r2), 1e-10)
    mu_new <- c(sum(r1 * vals) / s1, sum(r2 * vals) / s2)
    sg_new <- c(
      max(sqrt(sum(r1 * (vals - mu_new[1])^2) / s1), 1e-4),
      max(sqrt(sum(r2 * (vals - mu_new[2])^2) / s2), 1e-4)
    )

    if (max(abs(mu_new - mu)) < 1e-6) break
    mu     <- mu_new
    sg     <- sg_new
    pi_vec <- pi_new
  }

  # Components must be ordered (low = non-match, high = match)
  if (mu[1] > mu[2]) {
    mu     <- rev(mu)
    sg     <- rev(sg)
    pi_vec <- rev(pi_vec)
  }

  # Components must be separated; otherwise threshold is unreliable
  if (mu[2] - mu[1] < 0.02) return(fallback)

  # Find crossing point on a dense grid between the two means
  lo_g  <- mu[1] - 3 * sg[1]
  hi_g  <- mu[2] + 3 * sg[2]
  grid  <- seq(max(lo_g, 0), min(hi_g, 1), length.out = 2000L)
  d_nm  <- pi_vec[1] * stats::dnorm(grid, mu[1], sg[1])
  d_m   <- pi_vec[2] * stats::dnorm(grid, mu[2], sg[2])
  sign_diff <- sign(d_m - d_nm)
  crosses   <- which(diff(sign_diff) > 0)             # upward crossings

  if (!length(crosses)) return(fallback)
  grid[crosses[1]]
}
