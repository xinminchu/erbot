########################################

#' MST or Sorted-Neighborhood (SN) + edit-distance clustering
#'
#' Builds links between near-duplicate strings using a Sorted-Neighborhood
#' pass with an edit-distance threshold. Optionally, also builds a
#' k-nearest-neighbor graph in the sorted order and prunes a Minimum
#' Spanning Tree (MST) using a robust cut rule.
#'
#' The final graph is the union of:
#' * SN edges where \code{stringdist(text[i], text[j], method = sn_method) <= sn_thresh}
#'   for pairs within a sliding window of size \code{sn_window} on the
#'   lexicographically sorted normalized text, and
#' * (optional) MST edges whose distances are \emph{not} outliers:
#'   \code{d <= median(d) + mst_cut_ratio * MAD(d)}.
#'
#' Connected components of this union graph define clusters.
#'
#' @param text_vec Character vector of strings to cluster.
#' @param mst_cut_ratio Non-negative numeric controlling how aggressively to cut
#'   long MST edges; larger values keep more edges. Default \code{5}.
#' @param mst_k Optional integer. If provided, also build a k-NN graph in the
#'   sorted order (each node connects to the next \code{mst_k} items) with
#'   weights equal to edit distance, then compute its MST and prune with the
#'   robust rule above.
#' @param sn_window Integer sliding-window size (>= 1) for sorted-neighborhood
#'   candidate pairs. Default \code{40}.
#' @param sn_method String distance method for \code{stringdist::stringdist};
#'   e.g., \code{"jw"} (Jaro–Winkler), \code{"lv"} (Levenshtein). Default \code{"jw"}.
#' @param sn_thresh Numeric in `[0, 1]` for methods returning distances in that
#'   range (e.g., Jaro–Winkler). Create an SN edge if distance <= \code{sn_thresh}.
#'   Default \code{0.12}.
#'
#' @return Integer vector of cluster labels (length = \code{length(text_vec)}).
#' @export
er_mst_or_sn_edit <- function(text_vec,
                              mst_cut_ratio = 5,
                              mst_k = NULL,
                              sn_window = 40,
                              sn_method = "jw",
                              sn_thresh = 0.12) {
  n <- length(text_vec)
  if (n <= 1) return(rep(1L, n))

  # Normalize text lightly for sorting
  txt <- as.character(text_vec)
  txt[is.na(txt)] <- ""
  txt <- stringi::stri_trans_nfkc(txt)
  txt <- tolower(txt)
  txt <- stringr::str_squish(txt)

  # Sorted-Neighborhood ordering
  ord <- order(txt, method = "radix")
  inv <- integer(n); inv[ord] <- seq_len(n)

  # --- SN edges (threshold on edit distance) ---
  sn_i <- integer(0); sn_j <- integer(0)
  if (sn_window >= 1) {
    win <- max(1L, as.integer(sn_window))
    for (a in seq_len(n - 1L)) {
      bmax <- min(n, a + win)
      if (a < bmax) {
        i <- ord[a]
        js <- ord[(a + 1L):bmax]
        # compute distances to those candidates
        dists <- vapply(js, function(j) {
          stringdist::stringdist(txt[i], txt[j], method = sn_method)
        }, numeric(1))
        keep <- which(is.finite(dists) & !is.na(dists) & dists <= sn_thresh)
        if (length(keep)) {
          sn_i <- c(sn_i, rep(i, length(keep)))
          sn_j <- c(sn_j, js[keep])
        }
      }
    }
  }

  # --- Optional MST from sorted k-NN in order space ---
  mst_i <- integer(0); mst_j <- integer(0); mst_w <- numeric(0)
  if (!is.null(mst_k) && is.finite(mst_k) && mst_k >= 1) {
    k <- max(1L, as.integer(mst_k))
    # Build k "forward" neighbors per position in sorted order
    for (a in seq_len(n - 1L)) {
      bmax <- min(n, a + k)
      if (a < bmax) {
        i <- ord[a]
        js <- ord[(a + 1L):bmax]
        if (length(js)) {
          dists <- vapply(js, function(j) {
            stringdist::stringdist(txt[i], txt[j], method = sn_method)
          }, numeric(1))
          good <- is.finite(dists) & !is.na(dists)
          if (any(good)) {
            mst_i <- c(mst_i, rep(i, sum(good)))
            mst_j <- c(mst_j, js[good])
            mst_w <- c(mst_w, dists[good])  # distances as weights
          }
        }
      }
    }
    if (length(mst_w)) {
      # Create graph and compute MST
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

  # --- Union of edges (SN ∪ pruned MST) ---
  ei <- c(sn_i, mst_i); ej <- c(sn_j, mst_j)
  if (!length(ei)) {
    return(seq_len(n))  # all singletons
  }

  g <- igraph::graph_from_edgelist(cbind(ei, ej), directed = FALSE)
  # ensure isolated vertices exist
  if (igraph::vcount(g) < n) {
    g <- igraph::add_vertices(g, n - igraph::vcount(g), name = as.character(setdiff(seq_len(n), as.integer(igraph::V(g)$name))))
  }
  memb <- igraph::components(g)$membership
  # membership is aligned to vertex names; map to 1..n
  out <- integer(n)
  vn <- as.integer(igraph::V(g)$name)
  out[vn] <- as.integer(memb)
  out
}


########################################
