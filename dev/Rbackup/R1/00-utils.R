#' @importFrom magrittr %>%
NULL


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

safe_set_vertex_attr <- function(g, name, values, index = V(g)) {
  # If values is length 1, it’s fine for any index
  if (length(values) == 1L) {
    return(igraph::set_vertex_attr(g, name, index = index, value = values))
  }
  # If values is named by global IDs, align by V(g)$name
  if (!is.null(names(values))) {
    idx_names <- igraph::as_ids(index)
    aligned   <- values[idx_names]
    return(igraph::set_vertex_attr(g, name, index = index, value = aligned))
  }
  # If values already matches length(index), accept as-is
  if (length(values) == length(index)) {
    return(igraph::set_vertex_attr(g, name, index = index, value = values))
  }
  stop(sprintf(
    "safe_set_vertex_attr: length(values)=%d doesn't match length(index)=%d and values are not named.",
    length(values), length(index)))
}
