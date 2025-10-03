########################################

#' Louvain clustering from a precomputed similarity matrix
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

#' MST or Sorted-Neighborhood (SN) + edit-distance clustering
#' @export
er_mst_or_sn_edit <- function(text_vec,
                              mst_cut_ratio = 5,
                              mst_k = NULL,
                              sn_window = 40,
                              sn_method = "jw",
                              sn_thresh = 0.12) {
  n <- length(text_vec)
  if (n <= 1) return(rep(1L, n))

  txt <- as.character(text_vec)
  txt[is.na(txt)] <- ""
  txt <- stringi::stri_trans_nfkc(txt)
  txt <- tolower(txt)
  txt <- stringr::str_squish(txt)

  ord <- order(txt, method = "radix")

  sn_i <- integer(0); sn_j <- integer(0)
  if (sn_window >= 1) {
    win <- max(1L, as.integer(sn_window))
    for (a in seq_len(n - 1L)) {
      bmax <- min(n, a + win)
      if (a < bmax) {
        i <- ord[a]
        js <- ord[(a + 1L):bmax]
        dists <- vapply(js, function(j) { stringdist::stringdist(txt[i], txt[j], method = sn_method) }, numeric(1))
        keep <- which(is.finite(dists) & !is.na(dists) & dists <= sn_thresh)
        if (length(keep)) {
          sn_i <- c(sn_i, rep(i, length(keep)))
          sn_j <- c(sn_j, js[keep])
        }
      }
    }
  }

  mst_i <- integer(0); mst_j <- integer(0); mst_w <- numeric(0)
  if (!is.null(mst_k) && is.finite(mst_k) && mst_k >= 1) {
    k <- max(1L, as.integer(mst_k))
    for (a in seq_len(n - 1L)) {
      bmax <- min(n, a + k)
      if (a < bmax) {
        i <- ord[a]
        js <- ord[(a + 1L):bmax]
        if (length(js)) {
          dists <- vapply(js, function(j) stringdist::stringdist(txt[i], txt[j], method = sn_method), numeric(1))
          good <- is.finite(dists) & !is.na(dists)
          if (any(good)) {
            mst_i <- c(mst_i, rep(i, sum(good)))
            mst_j <- c(mst_j, js[good])
            mst_w <- c(mst_w, dists[good])
          }
        }
      }
    }
    if (length(mst_w)) {
      g0 <- igraph::graph_from_edgelist(cbind(mst_i, mst_j), directed = FALSE)
      igraph::E(g0)$weight <- mst_w
      Tm <- igraph::mst(g0, weights = igraph::E(g0)$weight)
      w_mst <- igraph::E(Tm)$weight
      med <- stats::median(w_mst, na.rm = TRUE)
      madv <- stats::mad(w_mst, center = med, constant = 1, na.rm = TRUE)
      thr <- med + max(0, mst_cut_ratio) * (if (is.finite(madv)) madv else 0)
      keep_e <- which(is.finite(w_mst) & w_mst <= thr)
      if (length(keep_e)) {
        ep <- igraph::ends(Tm, es = igraph::E(Tm)[keep_e], names = FALSE)
        mst_i <- as.integer(ep[, 1]); mst_j <- as.integer(ep[, 2])
      } else {
        mst_i <- mst_j <- integer(0)
      }
    }
  }

  ei <- c(sn_i, mst_i); ej <- c(sn_j, mst_j)
  if (!length(ei)) {
    return(seq_len(n))
  }

  g <- igraph::graph_from_edgelist(cbind(ei, ej), directed = FALSE)
  if (igraph::vcount(g) < n) {
    g <- igraph::add_vertices(g, n - igraph::vcount(g), name = as.character(setdiff(seq_len(n), as.integer(igraph::V(g)$name))))
  }
  memb <- igraph::components(g)$membership
  out <- integer(n)
  vn <- as.integer(igraph::V(g)$name)
  out[vn] <- as.integer(memb)
  out
}


########################################
