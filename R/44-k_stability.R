#' K-theory inspired stability penalty
#'
#' This function computes a surrogate stability loss that penalizes
#' differences between cluster projections and graph Laplacians
#' before and after perturbation.
#'
#' It combines:
#' - rank difference of P and P_delta (approx. cluster count change)
#' - spectral differences of P L P and P_delta L_delta P_delta
#'
#' @param P Projection matrix \code{c([n, n])} from the original clustering.
#' @param L Laplacian matrix \code{c([n, n])} of the original graph.
#' @param P_delta Projection matrix \code{c([n, n])} from the perturbed clustering.
#' @param L_delta Laplacian matrix \code{c([n, n])} of the perturbed graph.
#' @param top_m Number of smallest-magnitude eigenvalues to compare.
#'
#' @return Numeric scalar stability loss.
#' @export
stability_penalty <- function(P,
                              L,
                              P_delta,
                              L_delta,
                              top_m = 10) {
  # Rank term (proxy for number of clusters)
  rank_P      <- Matrix::rankMatrix(P)[1]
  rank_Pdelta <- Matrix::rankMatrix(P_delta)[1]
  loss_rank <- abs(rank_P - rank_Pdelta)

  # Spectral term
  A1 <- as.matrix(P %*% L %*% P)
  A2 <- as.matrix(P_delta %*% L_delta %*% P_delta)

  m1 <- min(top_m, nrow(A1) - 1)
  m2 <- min(top_m, nrow(A2) - 1)
  m  <- min(m1, m2)

  if (m <= 0) {
    loss_spec <- 0
  } else {
    eig1 <- RSpectra::eigs(A1, k = m, which = "SM")$values
    eig2 <- RSpectra::eigs(A2, k = m, which = "SM")$values
    loss_spec <- sum(abs(Re(eig1) - Re(eig2)))
  }

  loss_rank + loss_spec
}
