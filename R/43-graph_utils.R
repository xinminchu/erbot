#' Build a kNN similarity graph from embeddings
#'
#' @param Z Numeric matrix \code{c([n, d])} of L2-normalized embeddings.
#' @param k Number of nearest neighbours per node.
#' @param mutual Whether to enforce mutual kNN (not implemented yet).
#'
#' @return An igraph object with edge weights.
#' @export
build_knn_graph <- function(Z,
                            k = 10,
                            mutual = FALSE) {
  n <- nrow(Z)

  # Cosine distance -> similarity
  D <- proxy::dist(Z, method = "cosine")
  D <- as.matrix(D)
  S <- 1 - D

  edges <- list()
  weights <- numeric(0)

  for (i in seq_len(n)) {
    sims <- S[i, ]
    sims[i] <- -Inf
    idx <- order(sims, decreasing = TRUE)[1:k]

    for (j in idx) {
      # Mutual kNN could be handled here if needed
      edges[[length(edges) + 1]] <- c(i, j)
      weights <- c(weights, S[i, j])
    }
  }

  edges_mat <- do.call(rbind, edges)
  g <- igraph::graph_from_edgelist(edges_mat, directed = FALSE)
  igraph::E(g)$weight <- weights

  g
}

#' Perturb a similarity graph
#'
#' Adds Gaussian noise to edge weights and randomly drops edges
#' to generate a perturbed graph G^δ.
#'
#' @param g An igraph similarity graph with edge attribute 'weight'.
#' @param noise_sd Standard deviation of added Gaussian noise.
#' @param p_dropout Probability of dropping an edge.
#' @param min_weight Minimum allowed edge weight after perturbation.
#'
#' @return A perturbed igraph object.
#' @export
perturb_graph <- function(g,
                          noise_sd   = 0.0,
                          p_dropout  = 0.0,
                          min_weight = 0.0) {
  g2 <- g

  w <- igraph::E(g2)$weight

  if (noise_sd > 0) {
    w <- w + stats::rnorm(length(w), sd = noise_sd)
  }

  if (p_dropout > 0) {
    keep <- stats::runif(length(w)) > p_dropout
    g2 <- igraph::delete_edges(g2, igraph::E(g2)[!keep])
    w  <- w[keep]
  }

  w[w < min_weight] <- 0
  igraph::E(g2)$weight <- w

  g2
}

#' Run graph clustering (Leiden or Louvain)
#'
#' @param g An igraph similarity graph with 'weight' edge attribute.
#' @param method "leiden" (via leidenbase) or "louvain" (via igraph).
#' @param resolution Resolution parameter for Leiden (ignored for Louvain).
#'
#' @return Integer membership vector of cluster labels (1..k).
#' @export
run_graph_clustering <- function(g,
                                 method     = c("leiden", "louvain"),
                                 resolution = 1.0) {
  method <- match.arg(method)

  if (method == "leiden") {
    if (!requireNamespace("leidenbase", quietly = TRUE)) {
      stop("Please install 'leidenbase' or use method = 'louvain'.")
    }
    part <- leidenbase::leiden_find_partition(
      g,
      resolution_parameter = resolution,
      weights = igraph::E(g)$weight
    )
    membership <- part$membership
  } else {
    cl <- igraph::cluster_louvain(g, weights = igraph::E(g)$weight)
    membership <- igraph::membership(cl)
  }

  membership
}

#' Build projection matrix from cluster membership
#'
#' Given a clustering, construct P = H H^T
#' where H has columns normalized by cluster size.
#'
#' @param membership Integer membership vector, length n.
#'
#' @return Numeric matrix P with shape of \code{c([n, n])}.
#' @export
projection_from_membership <- function(membership) {
  n <- length(membership)
  clusters <- sort(unique(membership))
  k <- length(clusters)

  H <- matrix(0, nrow = n, ncol = k)

  for (idx in seq_along(clusters)) {
    c <- clusters[idx]
    members <- which(membership == c)
    H[members, idx] <- 1 / sqrt(length(members))
  }

  P <- H %*% t(H)
  P
}

#' Graph Laplacian matrix
#'
#' Compute the unnormalized Laplacian L = D - W
#' where W is the weighted adjacency matrix.
#'
#' @param g An igraph graph with 'weight' on edges.
#'
#' @return A sparse Matrix object representing the Laplacian.
#' @export
graph_laplacian_matrix <- function(g) {
  A <- igraph::as_adjacency_matrix(g, attr = "weight", sparse = TRUE)
  d <- Matrix::rowSums(A)
  D <- Matrix::Diagonal(x = d)
  L <- D - A
  L
}

#' Graph smoothness loss
#'
#' Compute Tr(Z^T L Z) as a scalar regularizer that encourages
#' embedding smoothness over the graph structure.
#'
#' @param Z Numeric matrix \code{c([n, d])} of embeddings.
#' @param L Sparse Laplacian matrix \code{c([n, n])}.
#'
#' @return Numeric scalar (smoothness loss).
#' @export
graph_smoothness_loss <- function(Z, L) {
  Zt <- t(Z)
  # Tr(Z^T L Z) = sum over all entries of Z^T (L Z)
  val <- sum(Zt * (Zt %*% L))
  val
}
