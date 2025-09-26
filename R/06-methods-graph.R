#' Louvain clustering from a precomputed similarity matrix
#'
#' Build a graph from a symmetric sparse similarity matrix and run Louvain
#' community detection after dropping edges below a similarity threshold.
#'
#' @param S A symmetric sparse similarity matrix (class `dgCMatrix`) with
#'   diagonal set to 1. Larger values mean more similar.
#' @param min_sim Numeric threshold in `[0, 1]`. Edges with similarity
#'   < `min_sim` are removed before clustering.
#' @return An integer vector of cluster labels (length `nrow(S)`).
#' @export
er_louvain_from_S <- function(S, min_sim = 0.0) {
  stopifnot(inherits(S, "sparseMatrix"))
  E <- Matrix::summary(S)
  E <- E[E$i != E$j & is.finite(E$x) & !is.na(E$x) & E$x >= min_sim, , drop = FALSE]
  if (!nrow(E)) return(rep(1L, nrow(S)))
  E <- E[E$i < E$j, , drop = FALSE]
  G <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE,
                                     vertices = data.frame(name = seq_len(nrow(S))))
  igraph::E(G)$weight <- E$x
  cl <- igraph::cluster_louvain(G, weights = igraph::E(G)$weight)
  memb <- rep(NA_integer_, nrow(S))
  v_ids <- as.integer(igraph::V(G)$name)
  memb[v_ids] <- igraph::membership(cl)
  if (anyNA(memb)) {
    maxlab <- if (all(is.na(memb))) 0L else max(memb, na.rm = TRUE)
    na_idx <- which(is.na(memb))
    memb[na_idx] <- maxlab + seq_along(na_idx)
  }
  memb
}

#' Louvain on a kNN graph (cosine over features)
#'
#' Builds a k-nearest-neighbor graph from a feature matrix using cosine
#' similarity, optionally prunes edges below a threshold, then runs Louvain
#' community detection.
#'
#' @param X Numeric feature matrix (rows = items).
#' @param knn Integer, number of neighbors per node (>= 1).
#' @param min_sim Numeric in `[0, 1]`; edges with similarity below this
#'   are dropped before clustering.
#' @return A list with:
#'   \itemize{
#'     \item `labels`: integer cluster labels (length `nrow(X)`).
#'     \item `graph`: the constructed `igraph` object.
#'   }
#' @export
er_louvain_knn <- function(X, knn = 10, min_sim = 0.0) {
  rs <- sqrt(rowSums(X^2)); rs[rs==0] <- 1; Xn <- X/rs
  knn_use <- max(1L, min(knn, nrow(Xn)-1L))
  nn <- FNN::get.knn(Xn, k=knn_use)
  sims <- vapply(seq_len(nrow(Xn)), function(i) as.numeric(Xn[i,,drop=FALSE] %*% t(Xn[nn$nn.index[i,],,drop=FALSE])), numeric(knn_use))
  keep <- sims >= min_sim
  edf <- cbind(from = rep(seq_len(nrow(Xn)), each=knn_use)[keep], to = as.vector(nn$nn.index)[keep])
  if (!length(edf)) return(list(labels=rep(1L, nrow(Xn)), graph=igraph::make_empty_graph(nrow(Xn))))
  g <- igraph::graph_from_edgelist(matrix(edf, ncol=2), directed=FALSE) |> igraph::simplify()
  list(labels = igraph::membership(igraph::cluster_louvain(g)), graph = g)
}


#' kNN clustering over embeddings with cosine threshold
#'
#' Runs a kNN graph on embedding vectors using cosine similarity and keeps
#' only edges above `cos_thresh`, then returns connected components as labels.
#'
#' @param emb_mat Numeric matrix of embeddings (rows = items).
#' @param k Integer, neighbors per node (>= 1).
#' @param cos_thresh Numeric in `[0, 1]`; keep edges with cosine >= this value.
#' @return Integer vector of cluster labels (length `nrow(emb_mat)`).
#' @export
er_embed_knn <- function(emb_mat, k=15, cos_thresh=0.88){
  if (!is.matrix(emb_mat)) emb_mat <- as.matrix(emb_mat); storage.mode(emb_mat) <- "double"
  n <- nrow(emb_mat); if (n < 2) return(rep(1L, n))
  nr <- sqrt(rowSums(emb_mat^2, na.rm=TRUE)); valid <- is.finite(nr) & nr > 0
  out <- seq_len(n); if (sum(valid) < 2) return(out)
  X <- emb_mat[valid,,drop=FALSE]; X <- X / nr[valid]; k_eff <- max(1L, min(k, nrow(X)-1L))
  knn <- FNN::get.knn(X, k=k_eff)
  edges <- vector("list", nrow(X))
  for (i in seq_len(nrow(X))) {
    idx <- knn$nn.index[i,]; sims_i <- as.numeric(X[i,,drop=FALSE] %*% t(X[idx,,drop=FALSE]))
    keep <- is.finite(sims_i) & sims_i >= cos_thresh
    if (any(keep)) edges[[i]] <- cbind(i, idx[keep])
  }
  edges <- do.call(rbind, edges); verts <- data.frame(name = as.character(seq_len(nrow(X))))
  if (!is.null(edges) && nrow(edges) > 0) {
    edf <- data.frame(from = as.character(edges[,1]), to = as.character(edges[,2]))
    g <- igraph::graph_from_data_frame(edf, directed=FALSE, vertices=verts) |> igraph::simplify()
  } else { g <- igraph::make_empty_graph(n = nrow(X)); igraph::V(g)$name <- as.character(seq_len(nrow(X))) }
  memb_valid <- igraph::components(g)$membership; out[valid] <- as.integer(memb_valid); out
}
