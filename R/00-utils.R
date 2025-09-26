#' Internal utility: Null-coalescing
#' @keywords internal
`%||%` <- function(a, b) if (!is.null(a)) a else b

#' Cosine distance on rows of a matrix
#' @param X numeric matrix
#' @return full cosine distance matrix with diag 0
#' @export
er_cosine_dist <- function(X){
  X <- as.matrix(X)
  nr <- sqrt(rowSums(X^2)); nr[nr == 0] <- 1
  X <- X / nr
  D <- 1 - (X %*% t(X))
  pmax(D, 0)
}
