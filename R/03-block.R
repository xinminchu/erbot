########################################
# File: R/03-block.R
# Blocking: generate candidate pairs by restricting comparison space.
# Four methods: "none" (all pairs), "standard" (exact key match),
# "prefix" (first-p-chars of key), "sn" (sorted-neighborhood window).
# er_block() is the main entry point; er_block_stats() measures quality.
########################################

# ── Internal helpers ───────────────────────────────────────────────────────────

#' Build all within-block pairs from a block assignment vector
#'
#' @param block_vec Character or factor vector of block keys (length n).
#' @param max_pairs Numeric. Pair budget; triggers a warning when exceeded.
#' @return data.frame with columns \code{idx1}, \code{idx2} (integer, 1-based).
#' @keywords internal
.pairs_from_blocks <- function(block_vec, max_pairs = Inf) {
  blocks <- split(seq_along(block_vec), block_vec)
  pairs  <- vector("list", length(blocks))
  total  <- 0L
  for (i in seq_along(blocks)) {
    idx <- blocks[[i]]
    m   <- length(idx)
    if (m < 2L) next
    # combn() directly generates the upper-triangle pairs (C(m,2) rows)
    # without the intermediate m^2 matrix that expand.grid() would create.
    comb <- combn(m, 2L)
    pi   <- idx[comb[1L, ]]; pj <- idx[comb[2L, ]]
    total <- total + length(pi)
    if (total > max_pairs) {
      warning(sprintf(
        "er_block: pair budget exceeded (%s > %s). Truncating.",
        format(total, big.mark = ","),
        format(max_pairs, big.mark = ",")
      ))
      pairs[[i]] <- data.frame(idx1 = pi, idx2 = pj)
      break
    }
    pairs[[i]] <- data.frame(idx1 = pi, idx2 = pj)
  }
  out <- do.call(rbind, pairs)
  if (is.null(out)) return(tibble::tibble(idx1 = integer(), idx2 = integer()))
  tibble::as_tibble(out)
}

#' Generate sorted-neighborhood candidate pairs
#'
#' Sorts records by \code{sort_key} and returns all pairs within a sliding
#' window of size \code{window}.
#'
#' @param sort_key Character vector of sorting keys (length n).
#' @param window Integer. Sliding-window size.
#' @param max_pairs Numeric. Pair budget; triggers a warning when exceeded.
#' @return tibble(idx1, idx2) (integer, 1-based).
#' @keywords internal
.pairs_sn <- function(sort_key, window, max_pairs = Inf) {
  ord <- order(sort_key, method = "radix", na.last = TRUE)
  n   <- length(ord)
  win <- max(1L, as.integer(window))

  # Pre-allocate output vectors to avoid O(n^2) memory copies from c() in loop.
  # Each position pairs with at most `win` subsequent positions.
  # Cap pre-allocation at 20M entries to prevent runaway allocation on huge inputs.
  est  <- max(1L, as.integer(min(as.numeric(n - 1L) * win, 2e7)))
  is   <- integer(est)
  js   <- integer(est)
  k    <- 0L

  for (pos in seq_len(n - 1L)) {
    i    <- ord[pos]
    jmax <- min(n, pos + win)
    for (q in seq.int(pos + 1L, jmax)) {
      k     <- k + 1L
      is[k] <- i
      js[k] <- ord[q]
    }
    if (k > max_pairs) {
      warning(sprintf(
        "er_block (sn): pair budget exceeded. Truncating at %s pairs.",
        format(max_pairs, big.mark = ",")
      ))
      break
    }
  }
  tibble::tibble(idx1 = is[seq_len(k)], idx2 = js[seq_len(k)])
}

# ── er_block() ────────────────────────────────────────────────────────────────

#' Generate candidate pairs via blocking
#'
#' Restricts pairwise comparison to within-block pairs using one of four
#' strategies. The output is consumed by \code{er_similarity()}.
#'
#' \describe{
#'   \item{\code{"none"}}{All \eqn{\binom{n}{2}} pairs. Only suitable for
#'     small \eqn{n} (\eqn{\le 5000}).}
#'   \item{\code{"standard"}}{Exact match on \code{block_key} column.
#'     Records with \code{NA} in the key are placed in a single \code{""} block
#'     (structural fallback; they are \emph{not} imputed).}
#'   \item{\code{"prefix"}}{First \code{prefix_len} characters of
#'     \code{block_key} (after lower-casing and normalisation).}
#'   \item{\code{"sn"}}{Sorted-neighborhood on \code{block_key}: sort records
#'     by key then compare all pairs within a sliding window of
#'     \code{sn_window} positions.}
#'   \item{\code{"auto"}}{Selects a method from the \code{diag} object
#'     produced by \code{er_diagnose()}: \code{"none"} if no blocking is
#'     needed, otherwise the recommended method.}
#' }
#'
#' In \strong{linkage mode} (when \code{source_col} is set or detected from
#' \code{diag}), only cross-source pairs are generated.
#'
#' @param data A \code{data.frame} of records (output of \code{er_load()}).
#' @param diag Named list from \code{er_diagnose()}, or \code{NULL}.
#' @param method Character. Blocking method: one of \code{"auto"},
#'   \code{"none"}, \code{"standard"}, \code{"prefix"}, \code{"sn"}.
#' @param block_key Character. Column name to use as the blocking key.
#'   If \code{NULL}, taken from \code{diag$recommended_block_key}.
#' @param prefix_len Integer. Prefix length for method \code{"prefix"}.
#'   Default \code{3}.
#' @param sn_window Integer. Sliding-window size for method \code{"sn"}.
#'   Default \code{40}.
#' @param source_col Character. Name of the source-ID column. If non-\code{NULL},
#'   only cross-source pairs are retained. If \code{NULL}, taken from
#'   \code{diag$source_col}.
#' @param max_pairs Numeric. Hard upper limit on generated pairs.
#'   Default \code{2e6}.
#'
#' @return A \code{tibble} with columns \code{idx1} and \code{idx2} (integer
#'   row indices into \code{data}, 1-based). Always satisfies
#'   \code{idx1 < idx2}.
#' @export
er_block <- function(data,
                     diag       = NULL,
                     method     = "auto",
                     block_key  = NULL,
                     prefix_len = 3L,
                     sn_window  = 40L,
                     source_col = NULL,
                     max_pairs  = 2e6) {

  `%||%` <- get("%||%", asNamespace("erbot"))

  df <- tibble::as_tibble(data)
  names(df) <- tolower(names(df))
  n <- nrow(df)

  # Resolve method and blocking key from diag
  if (method == "auto") {
    if (!is.null(diag)) {
      method    <- diag$recommended_block_method %||% "none"
      block_key <- block_key %||% diag$recommended_block_key
    } else {
      method <- if (n <= 5000L) "none" else "prefix"
    }
  }
  method <- match.arg(method, c("none", "standard", "prefix", "sn"))

  # Source column: linkage mode
  if (is.null(source_col) && !is.null(diag)) source_col <- diag$source_col
  if (!is.null(source_col)) source_col <- tolower(source_col)
  linkage_mode <- !is.null(source_col) && source_col %in% names(df)

  # ── Generate pairs ─────────────────────────────────────────────────────────
  pairs <- switch(method,

    "none" = {
      if (n > 20000L)
        warning("er_block method='none' with n=", n,
                " generates ", format(choose(n, 2), big.mark = ","),
                " pairs. Consider using blocking.")
      ii <- rep(seq_len(n - 1L), times = rev(seq_len(n - 1L)))
      jj <- unlist(lapply(seq_len(n - 1L), function(i) (i + 1L):n))
      tibble::tibble(idx1 = as.integer(ii), idx2 = as.integer(jj))
    },

    "standard" = {
      if (is.null(block_key) || !block_key %in% names(df))
        stop("er_block: 'block_key' must be a valid column name for method='standard'.")
      key_vec <- as.character(df[[block_key]])
      key_vec[is.na(key_vec) | key_vec == ""] <- ""  # structural fallback
      .pairs_from_blocks(key_vec, max_pairs)
    },

    "prefix" = {
      if (is.null(block_key) || !block_key %in% names(df))
        stop("er_block: 'block_key' must be a valid column name for method='prefix'.")
      key_vec <- as.character(df[[block_key]])
      key_vec[is.na(key_vec) | key_vec == ""] <- ""
      key_vec <- tolower(stringi::stri_trans_nfkc(key_vec))
      p   <- max(1L, as.integer(prefix_len))
      pfx <- substr(key_vec, 1L, p)
      pfx[pfx == ""] <- "__MISSING__"   # separate group for missing
      .pairs_from_blocks(pfx, max_pairs)
    },

    "sn" = {
      sort_key <- if (!is.null(block_key) && block_key %in% names(df)) {
        key_vec <- as.character(df[[block_key]])
        key_vec[is.na(key_vec)] <- ""
        tolower(stringi::stri_trans_nfkc(key_vec))
      } else {
        # use row index order as fallback
        as.character(seq_len(n))
      }
      .pairs_sn(sort_key, sn_window, max_pairs)
    }
  )

  # ── Ensure idx1 < idx2 ────────────────────────────────────────────────────
  swap <- pairs$idx1 > pairs$idx2
  if (any(swap)) {
    tmp            <- pairs$idx1[swap]
    pairs$idx1[swap] <- pairs$idx2[swap]
    pairs$idx2[swap] <- tmp
  }
  pairs <- dplyr::distinct(pairs)

  # ── Linkage mode: keep only cross-source pairs ─────────────────────────────
  if (linkage_mode) {
    src <- as.character(df[[source_col]])
    src1 <- src[pairs$idx1]; src2 <- src[pairs$idx2]
    pairs <- pairs[src1 != src2, , drop = FALSE]
  }

  pairs
}

# ── er_block_stats() ──────────────────────────────────────────────────────────

#' Compute blocking quality statistics
#'
#' Measures how well a set of candidate pairs covers the true match pairs.
#'
#' \describe{
#'   \item{Reduction Ratio (RR)}{
#'     \eqn{1 - |\text{candidate pairs}| / \binom{n}{2}}.
#'     Close to 1 means most non-match pairs were discarded.}
#'   \item{Pair Completeness (PC)}{
#'     \eqn{|\text{candidate} \cap \text{true matches}| / |\text{true matches}|}.
#'     Close to 1 means no true matches were lost. Only computed when
#'     \code{truth} is provided.}
#' }
#'
#' @param pairs tibble(idx1, idx2) from \code{er_block()}.
#' @param n Integer. Total number of records.
#' @param truth Optional named integer vector (names = record IDs, values =
#'   cluster IDs), or a two-column data.frame with columns \code{id} and
#'   \code{cluster_id}.  When provided, Pair Completeness is computed.
#' @param id_vec Optional character vector of record IDs aligned to row indices
#'   (length \code{n}). Required if \code{truth} is a named vector.
#'
#' @return Named list with \code{n_candidate_pairs}, \code{reduction_ratio},
#'   \code{pair_completeness} (NA when truth absent).
#' @export
er_block_stats <- function(pairs, n, truth = NULL, id_vec = NULL) {
  n_cand <- nrow(pairs)
  n_all  <- choose(n, 2L)
  rr     <- if (n_all > 0) 1 - n_cand / n_all else 0

  pc <- NA_real_
  if (!is.null(truth)) {
    # Build match pairs from truth
    if (is.vector(truth) && !is.null(names(truth))) {
      truth <- tibble::tibble(id = as.character(names(truth)),
                              cluster_id = as.integer(truth))
    }
    if (is.data.frame(truth)) {
      names(truth) <- tolower(names(truth))
      truth <- truth[, c("id", "cluster_id")]
    }
    # Map IDs to indices
    if (!is.null(id_vec)) {
      id_to_idx <- setNames(seq_along(id_vec), as.character(id_vec))
      truth_idx <- truth[truth$id %in% names(id_to_idx), , drop = FALSE]
      truth_idx$idx <- id_to_idx[truth_idx$id]
    } else {
      truth_idx <- truth
      truth_idx$idx <- suppressWarnings(as.integer(truth_idx$id))
    }
    # True match pairs: all pairs within a cluster
    cl_groups <- split(truth_idx$idx, truth_idx$cluster_id)
    true_pairs <- do.call(rbind, lapply(cl_groups, function(idx) {
      if (length(idx) < 2L) return(NULL)
      gr <- expand.grid(a = idx, b = idx)
      gr <- gr[gr$a < gr$b, , drop = FALSE]
      data.frame(idx1 = gr$a, idx2 = gr$b)
    }))
    if (!is.null(true_pairs) && nrow(true_pairs)) {
      cand_key <- paste(pairs$idx1, pairs$idx2, sep = "_")
      true_key <- paste(true_pairs$idx1, true_pairs$idx2, sep = "_")
      n_covered <- sum(true_key %in% cand_key)
      pc <- n_covered / nrow(true_pairs)
    } else {
      pc <- NA_real_
    }
  }

  list(
    n_candidate_pairs = n_cand,
    n_all_pairs       = n_all,
    reduction_ratio   = round(rr, 4),
    pair_completeness = if (is.na(pc)) NA_real_ else round(pc, 4)
  )
}
