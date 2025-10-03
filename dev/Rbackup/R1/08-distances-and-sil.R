#' Average silhouette from distance
#' @param labels vector
#' @param D matrix or dist
#' @return numeric
#' @export
er_silhouette_avg <- function(labels, D){
  labs <- as.integer(factor(labels))
  if (length(unique(labs)) < 2) return(NA_real_)
  sil <- tryCatch(cluster::silhouette(as.integer(factor(labels)), dist = stats::as.dist(D)), error=function(e) NULL)
  if (is.null(sil)) return(NA_real_)
  mean(sil[,3], na.rm=TRUE)
}

#' Pairwise string distances in blocks (with optional progress)
#' @export
er_pairwise_stringdist <- function(text_vec, method="jw", block=4000, progress=NULL){
  n <- length(text_vec)
  if (n <= 1) return(matrix(0, n, n))
  if (inherits(progress, "er_progress")) {
    total_pairs <- n * (n - 1) / 2
    cat(sprintf("\nComputing pairwise distances [%s] for n=%d (~%.1fM pairs)\n",
                method, n, total_pairs / 1e6))
  }
  D <- matrix(0, n, n)
  total_pairs <- n * (n - 1) / 2
  done_pairs <- 0
  for (i in seq(1, n, by=block)) {
    i2 <- min(i+block-1, n); xi <- text_vec[i:i2]
    for (j in seq(i, n, by=block)) {
      j2 <- min(j+block-1, n); xj <- text_vec[j:j2]
      sub <- as.matrix(stringdist::stringdistmatrix(xi, xj, method=method))
      D[i:i2, j:j2] <- sub
      if (j > i) D[j:j2, i:i2] <- t(sub)
      new_pairs <- if (i==j) (i2 - i + 1) * (i2 - i) / 2 else (i2 - i + 1) * (j2 - j + 1)
      done_pairs <- done_pairs + new_pairs
      if (inherits(progress, "er_progress")) {
        pct <- 100 * done_pairs / max(1, total_pairs)
        cat(sprintf("\r Distance progress: %5.1f%%", pct)); flush.console()
      }
    }
  }
  if (inherits(progress, "er_progress")) cat("\n")
  D
}
