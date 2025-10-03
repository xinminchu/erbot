# Generic helper: average ARI against a reference clustering on full data
# get_fit: a function that takes (X, ...) and returns a vector of labels (length nrow(X))
stability_via_resamples <- function(
    X,
    get_fit,             # function(X, ...) -> labels (length nrow(X))
    B = 20,              # #resamples
    sample_frac = 0.8,   # fraction of rows per resample
    seed = 1,            # RNG
    ...
){
  set.seed(seed)
  n <- nrow(X)
  # Reference labels on full data
  ref_labels <- get_fit(X, ...)

  ari_vals <- numeric(B)
  for (b in seq_len(B)) {
    idx <- sort(sample.int(n, size = max(2L, floor(sample_frac * n)), replace = FALSE))
    Xb <- X[idx, , drop = FALSE]
    lab_b <- get_fit(Xb, ...)

    # Compare with reference restricted to idx
    ari_vals[b] <- mclust::adjustedRandIndex(ref_labels[idx], lab_b)
  }
  mean(ari_vals, na.rm = TRUE)
}

# --- K-means stability across k_grid ---
er_kmeans_stability_grid <- function(
    Z,
    k_grid = seq(2, 30, by = 1),
    nstart = 10,
    scale_input = FALSE,
    B = 20,
    sample_frac = 0.8,
    seed = 1
){
  if (scale_input) Z <- scale(Z)

  get_fit_kmeans <- function(X, k, nstart) {
    stats::kmeans(X, centers = k, nstart = nstart)$cluster
  }

  rows <- vector("list", length(k_grid))
  for (i in seq_along(k_grid)) {
    k <- min(k_grid[i], nrow(Z))
    stab <- stability_via_resamples(
      X = Z, get_fit = get_fit_kmeans,
      B = B, sample_frac = sample_frac, seed = seed,
      k = k, nstart = nstart
    )
    rows[[i]] <- data.frame(
      method = "kmeans",
      k = k,
      stability_ari = stab,
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, rows)
}

# --- Graph (Louvain) stability across knn_grid ---
er_graph_louvain_stability_grid <- function(
    Z,
    knn_grid = seq(10, 300, by = 10),
    metric = "cosine",
    mutual = TRUE,
    weights = TRUE,
    B = 20,
    sample_frac = 0.8,
    seed = 1
){
  get_fit_graph <- function(X, k, metric, mutual, weights, seed) {
    g <- build_knn_graph(X, k = k, metric = metric, mutual = mutual, weights = weights, seed = seed)
    igraph::membership(igraph::cluster_louvain(g, weights = E(g)$weight))
  }

  rows <- vector("list", length(knn_grid))
  for (i in seq_along(knn_grid)) {
    k <- knn_grid[i]
    stab <- stability_via_resamples(
      X = Z, get_fit = get_fit_graph,
      B = B, sample_frac = sample_frac, seed = seed,
      k = k, metric = metric, mutual = mutual, weights = weights, seed = seed
    )
    rows[[i]] <- data.frame(
      method = "louvain_knn",
      knn = k,
      stability_ari = stab,
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, rows)
}
