########################################

#' K-means clustering on features
#' @export
er_kmeans_from_X <- function(X, k=10, seed=123){ set.seed(seed); stats::kmeans(X, centers=k, nstart=20)$cluster }

#' Hierarchical clustering (Ward.D2) with cosine distance
#' @export
er_hclust_from_X <- function(X, k=10, method="ward.D2"){
  D <- er_cosine_dist(X); hc <- stats::hclust(stats::as.dist(D), method=method); stats::cutree(hc, k=k)
}

#' PAM clustering with cosine distance
#' @export
er_pam_from_X <- function(X, k=10){
  D <- er_cosine_dist(X); cluster::pam(stats::as.dist(D), k=k)$clustering
}

#' KMeans with named cluster vector (aligned to global IDs)
#' @export
run_kmeans_svd <- function(Z, k, ids, nstart = 10, iter.max = 100, seed = NULL) {
  stopifnot(nrow(Z) == length(ids))
  if (!is.null(seed)) set.seed(seed)
  km <- stats::kmeans(Z, centers = k, nstart = nstart, iter.max = iter.max)
  cl <- km$cluster
  names(cl) <- ids
  list(kmeans = km, clusters_named = cl)
}


########################################
