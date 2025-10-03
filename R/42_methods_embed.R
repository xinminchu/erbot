########################################

#' kNN clustering over embeddings with cosine threshold
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


########################################
