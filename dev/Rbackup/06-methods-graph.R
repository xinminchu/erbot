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

# Imports you’ll need in DESCRIPTION:
# Imports:
#   igraph,
#   FNN,
#   mclust

suppressPackageStartupMessages({
  library(igraph)
  library(FNN)     # fast kNN
  library(mclust)  # ARI
})

# --- Cosine helper: normalize rows so Euclidean ≡ Cosine distance ---
row_normalize <- function(X) {
  nrm <- sqrt(rowSums(X^2))
  nrm[nrm == 0] <- 1
  X / nrm
}

# --- Build (symmetrized) kNN graph from an embedding matrix Z (n x r) ---
# metric: "cosine" (default) or "euclidean"
# mutual: keep only mutual kNN edges (stricter) vs union (symmetric)
# weights: optional; if TRUE, use similarity weights on edges
build_knn_graph <- function(Z, k = 50, metric = c("cosine","euclidean"),
                            mutual = TRUE, weights = TRUE, seed = 1) {
  set.seed(seed)
  metric <- match.arg(metric)
  X <- if (metric == "cosine") row_normalize(as.matrix(Z)) else as.matrix(Z)

  # kNN indices and distances (Euclidean on X; for cosine, X is normalized)
  nn <- FNN::get.knn(X, k = k, algorithm = "cover_tree")
  n <- nrow(X)

  # Build edge list: i -> nn$nn.index[i, j]
  irep <- rep(seq_len(n), each = k)
  jvec <- as.vector(nn$nn.index)
  dvec <- as.vector(nn$nn.dist)

  # Symmetrize
  edges <- data.frame(from = irep, to = jvec, dist = dvec)
  edges <- edges[edges$from != edges$to, ]

  if (mutual) {
    # keep only mutual edges
    key <- paste(edges$from, edges$to, sep = "_")
    key_rev <- paste(edges$to, edges$from, sep = "_")
    keep <- key %in% key_rev
    edges <- edges[keep, , drop = FALSE]
  }

  # Convert distance to similarity (for weights)
  if (weights) {
    # Cosine similarity if metric == cosine; otherwise use exp(-dist)
    if (metric == "cosine") {
      # since X is normalized, dist^2 = 2(1 - cos); ⇒ cos = 1 - dist^2/2
      sim <- pmax(0, 1 - (edges$dist^2) / 2)
    } else {
      sim <- exp(-edges$dist / (median(edges$dist) + 1e-8))
    }
  } else {
    sim <- NULL
  }

  g <- igraph::graph_from_data_frame(
    d = data.frame(from = edges$from, to = edges$to, weight = sim),
    directed = FALSE, vertices = data.frame(name = seq_len(n))
  )

  # ensure simple and connected components handled (keep giant component if wanted)
  g <- igraph::simplify(g, remove.multiple = TRUE, remove.loops = TRUE)
  g
}

# Louvain clustering + modularity for a *single* graph
graph_louvain_with_modularity <- function(g) {
  # if weighted, Louvain will use edge attribute 'weight'
  cw <- igraph::cluster_louvain(g, weights = E(g)$weight)
  memb <- igraph::membership(cw)
  Q <- igraph::modularity(g, memb, weights = E(g)$weight)
  list(membership = memb, modularity = unname(Q), n_clusters = length(unique(memb)))
}

# Run over a grid of k (neighbors), return a tidy data.frame of Q vs k
er_graph_knn_modularity_grid <- function(
    Z,
    knn_grid = seq(10, 500, by = 10),
    metric = "cosine",
    mutual = TRUE,
    weights = TRUE,
    seed = 1
){
  rows <- vector("list", length(knn_grid))
  for (i in seq_along(knn_grid)) {
    k <- knn_grid[i]
    g <- build_knn_graph(Z, k = k, metric = metric, mutual = mutual, weights = weights, seed = seed)
    fit <- graph_louvain_with_modularity(g)
    rows[[i]] <- data.frame(
      method = "louvain_knn",
      knn = k,
      modularity = fit$modularity,
      n_clusters = fit$n_clusters,
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, rows)
}
