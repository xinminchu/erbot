########################################
# File: R/06-combine.R
# Combine per-field similarities into a single combined similarity.
# Key design: NA-aware adaptive weighted average (Eq. 1 in erbot_plan_v2).
#
# er_combine()  -- main entry point
# er_weights()  -- weight learning (auto / supervised / unsupervised)
########################################

# ── er_combine() ──────────────────────────────────────────────────────────────

#' NA-aware adaptive weighted similarity combination
#'
#' Computes the combined similarity for each candidate pair as:
#' \deqn{S(i,j) = \frac{\sum_{k:\,S_k(i,j) \neq \texttt{NA}} w_k S_k(i,j)}
#'                     {\sum_{k:\,S_k(i,j) \neq \texttt{NA}} w_k}}
#' Pairs for which \emph{all} fields are \code{NA} receive the value
#' \code{na_fill} (default 0).
#'
#' This is the reference implementation given by Prof.\ Degras-Valabregue
#' (\texttt{impute\_dist\_weights.R}), adapted to the pair-list representation.
#'
#' @param sim_list Named list of numeric vectors (output of
#'   \code{er_similarity()}), one per field.
#' @param weights Named numeric vector of field weights. Names must match
#'   names of \code{sim_list}. If \code{NULL}, equal weights are used.
#' @param na_fill Numeric. Substitute for pairs where all fields are missing.
#'   Default \code{0}.
#'
#' @return Numeric vector of combined similarities (length = length of each
#'   element of \code{sim_list}).
#' @export
er_combine <- function(sim_list, weights = NULL, na_fill = 0) {
  if (!length(sim_list)) stop("er_combine: sim_list is empty.")

  n_pairs  <- length(sim_list[[1]])
  fnames   <- names(sim_list)

  # Default equal weights
  if (is.null(weights)) {
    weights <- setNames(rep(1 / length(fnames), length(fnames)), fnames)
  }
  # Align weights to sim_list fields; ignore extra weight names
  common <- intersect(fnames, names(weights))
  if (!length(common)) stop("er_combine: no overlap between sim_list names and weights names.")
  sim_list <- sim_list[common]
  weights  <- weights[common]
  weights  <- weights / sum(weights)   # normalise

  s_sum <- numeric(n_pairs)    # numerator
  w_sum <- numeric(n_pairs)    # denominator (available weight)

  for (fname in common) {
    sv  <- sim_list[[fname]]
    wk  <- weights[[fname]]
    ok  <- !is.na(sv)
    s_sum[ok] <- s_sum[ok] + wk * sv[ok]
    w_sum[ok] <- w_sum[ok] + wk
  }

  result <- ifelse(w_sum > 0, s_sum / w_sum, na_fill)
  result
}

# ── er_weights() ──────────────────────────────────────────────────────────────

#' Learn or select field weights
#'
#' Provides several weight strategies for \code{er_combine()}:
#' \describe{
#'   \item{\code{"auto"}}{Uses \code{"ari"} if \code{truth} is provided,
#'     \code{"fellegi_sunter"} otherwise.}
#'   \item{\code{"equal"}}{All weights = \eqn{1/K}.}
#'   \item{\code{"ari"}}{Weight_k proportional to ARI between threshold-CC
#'     clusters from \eqn{S_k} and the truth. Requires \code{truth}.}
#'   \item{\code{"fellegi_sunter"}}{EM mixture model: weight_k \eqn{\propto}
#'     \eqn{\log(m_k / u_k)} where \eqn{m_k} = P(S_k | match) and
#'     \eqn{u_k} = P(S_k | non-match).}
#'   \item{\code{"bimodal"}}{Weight_k \eqn{\propto} bimodality coefficient of
#'     \eqn{S_k} distribution.}
#'   \item{\code{"variance"}}{Weight_k \eqn{\propto} Var(\eqn{S_k}).}
#'   \item{Numeric vector}{User-specified weights; normalised to sum to 1.}
#' }
#'
#' @param sim_list Named list from \code{er_similarity()}.
#' @param pairs tibble(idx1, idx2) from \code{er_block()}.
#' @param truth Optional named integer vector or two-column data.frame
#'   (id, cluster_id) of ground truth.
#' @param id_vec Character vector of record IDs (aligned to row indices).
#' @param method Character or numeric vector. Default \code{"auto"}.
#'
#' @return Named numeric vector of weights summing to 1.
#' @export
er_weights <- function(sim_list, pairs = NULL, truth = NULL, id_vec = NULL,
                       method = "auto") {

  fnames <- names(sim_list)
  K      <- length(fnames)
  if (K == 0L) stop("er_weights: sim_list is empty.")

  # ── User-supplied numeric vector ────────────────────────────────────────────
  if (is.numeric(method)) {
    if (length(method) != K)
      stop("er_weights: numeric 'method' must have length == number of fields (", K, ").")
    w <- method
    if (is.null(names(w))) names(w) <- fnames
    w <- w[fnames]; w[is.na(w)] <- 0
    if (sum(w) <= 0) stop("er_weights: all weights are zero.")
    return(w / sum(w))
  }

  if (method == "auto") {
    method <- if (!is.null(truth)) "ari" else "fellegi_sunter"
  }
  method <- match.arg(method, c("equal", "ari", "fellegi_sunter",
                                 "bimodal", "variance"))

  # ── Equal ────────────────────────────────────────────────────────────────────
  if (method == "equal") {
    return(setNames(rep(1 / K, K), fnames))
  }

  # ── Variance ─────────────────────────────────────────────────────────────────
  if (method == "variance") {
    w <- vapply(sim_list, function(sv) {
      v <- sv[!is.na(sv)]
      if (length(v) < 2L) 0 else stats::var(v)
    }, numeric(1L))
    s <- sum(w)
    if (s <= 0) return(setNames(rep(1 / K, K), fnames))
    return(w / s)
  }

  # ── Bimodality coefficient ────────────────────────────────────────────────────
  # BC = (skew^2 + 1) / (kurt + 3*(n-1)^2/((n-2)(n-3)))
  # Threshold 0.555 (Sarle's rule); weight = max(0, BC - 0.5)
  if (method == "bimodal") {
    w <- vapply(sim_list, function(sv) {
      v  <- sv[!is.na(sv)]
      nv <- length(v)
      if (nv < 4L) return(0)
      mu  <- mean(v); s <- stats::sd(v)
      if (s == 0) return(0)
      g1  <- mean(((v - mu) / s)^3)
      g2  <- mean(((v - mu) / s)^4) - 3
      bc  <- (g1^2 + 1) / (g2 + 3 * (nv - 1)^2 / ((nv - 2) * (nv - 3)))
      max(0, bc - 0.5)
    }, numeric(1L))
    s <- sum(w)
    if (s <= 0) return(setNames(rep(1 / K, K), fnames))
    return(w / s)
  }

  # ── Fellegi-Sunter (EM mixture) ───────────────────────────────────────────────
  if (method == "fellegi_sunter") {
    w <- vapply(sim_list, function(sv) {
      v <- sv[!is.na(sv)]
      if (length(v) < 10L) return(0)
      v <- pmax(0, pmin(1, v))
      # Simple 2-component EM: initialise by splitting at median
      med  <- stats::median(v)
      # m = mean of upper component; u = mean of lower component
      upper <- v[v >= med]; lower <- v[v < med]
      m_k <- if (length(upper)) mean(upper) else 0.9
      u_k <- if (length(lower)) mean(lower) else 0.1
      pi_m <- 0.1   # prior for match component (usually small fraction)
      # 5 EM iterations
      for (iter in seq_len(5L)) {
        # E-step: posterior probability of match component
        dm <- stats::dbeta(v, shape1 = max(0.1, m_k * 9), shape2 = max(0.1, (1 - m_k) * 9))
        du <- stats::dbeta(v, shape1 = max(0.1, u_k * 9), shape2 = max(0.1, (1 - u_k) * 9))
        denom  <- pi_m * dm + (1 - pi_m) * du
        denom[denom == 0] <- 1e-300
        r_m    <- pi_m * dm / denom
        # M-step
        pi_m   <- mean(r_m)
        m_k    <- sum(r_m * v) / max(sum(r_m), 1e-10)
        u_k    <- sum((1 - r_m) * v) / max(sum(1 - r_m), 1e-10)
      }
      m_k <- pmax(0.01, pmin(0.99, m_k))
      u_k <- pmax(0.01, pmin(0.99, u_k))
      max(0, log(m_k / u_k))
    }, numeric(1L))
    s <- sum(w)
    if (s <= 0) return(setNames(rep(1 / K, K), fnames))
    return(w / s)
  }

  # ── ARI-based (supervised) ───────────────────────────────────────────────────
  if (method == "ari") {
    if (is.null(truth)) stop("er_weights method='ari' requires 'truth'.")
    if (is.null(pairs)) stop("er_weights method='ari' requires 'pairs'.")

    # Parse truth into id -> cluster_id map
    if (is.vector(truth) && !is.null(names(truth))) {
      truth_tbl <- tibble::tibble(id = as.character(names(truth)),
                                  cluster_id = as.integer(truth))
    } else if (is.data.frame(truth)) {
      truth_tbl <- truth; names(truth_tbl) <- tolower(names(truth_tbl))
    } else {
      stop("er_weights: unsupported truth format.")
    }
    truth_map <- setNames(as.integer(truth_tbl$cluster_id),
                          as.character(truth_tbl$id))

    if (is.null(id_vec)) id_vec <- as.character(seq_len(max(pairs$idx2)))
    gold_vec <- truth_map[id_vec]  # NA for unlabelled records

    # Only evaluate on pairs where both records have a truth label
    has_truth <- !is.na(gold_vec)

    .ari_of_field <- function(sv) {
      if (!requireNamespace("GCMER", quietly = TRUE)) {
        # Fallback: variance proxy
        v <- sv[!is.na(sv)]
        return(if (length(v) < 2L) 0 else stats::var(v))
      }
      # Build threshold-CC clusters for this field on labelled pairs
      ok  <- !is.na(sv) & has_truth[pairs$idx1] & has_truth[pairs$idx2]
      if (sum(ok) < 2L) return(0)
      p1  <- pairs$idx1[ok]; p2 <- pairs$idx2[ok]; sv_ok <- sv[ok]
      thr <- stats::median(sv_ok, na.rm = TRUE)  # simple median threshold
      # connected components
      g_df <- data.frame(from = p1[sv_ok >= thr], to = p2[sv_ok >= thr])
      if (!nrow(g_df)) return(0)
      n_nodes <- max(pairs$idx1, pairs$idx2)
      g  <- igraph::graph_from_data_frame(g_df, directed = FALSE,
                                           vertices = data.frame(name = seq_len(n_nodes)))
      cl <- igraph::components(g)$membership
      labelled_idx <- which(has_truth)[seq_len(min(500L, sum(has_truth)))]
      pred  <- cl[labelled_idx]
      truth_sub <- gold_vec[labelled_idx]
      tryCatch(GCMER::adj_rand(pred, truth_sub), error = function(e) 0)
    }

    w <- vapply(sim_list, .ari_of_field, numeric(1L))
    w <- pmax(0, w)
    s <- sum(w)
    if (s <= 0) return(setNames(rep(1 / K, K), fnames))
    return(w / s)
  }

  setNames(rep(1 / K, K), fnames)  # fallback
}


# ── er_field_ensemble() ───────────────────────────────────────────────────────

#' Cluster ensemble across fields (Approach 2)
#'
#' An alternative to \code{er_combine()} + \code{er_cluster()}.
#' Instead of aggregating per-field similarities into one matrix first,
#' this function clusters each field's similarity matrix \emph{independently}
#' and then merges the resulting partitions via majority-vote co-membership
#' (\code{er_consensus()}).
#'
#' \strong{When to use:}
#' \itemize{
#'   \item Fields are very heterogeneous in scale or measurement type and a
#'     weighted average would distort the similarity structure.
#'   \item You suspect that different fields carry complementary rather than
#'     redundant signals.
#'   \item As a robustness check against Approach~1 (\code{er_combine}).
#' }
#'
#' \strong{How it works:}
#' \enumerate{
#'   \item For each field \eqn{k}, build a sparse \eqn{n \times n} similarity
#'     matrix \eqn{S_k} from the candidate pairs. Fields with fewer than
#'     \code{min_pairs} non-NA values are skipped.
#'   \item Run \code{er_cluster(S_k, method = cluster_method, threshold,
#'     ...)} to obtain label vector \eqn{\ell_k}.
#'   \item Collect all \eqn{\ell_k} into a list and call
#'     \code{er_consensus(cluster_list, n, alpha = merge_alpha)}: a pair is
#'     placed in the same cluster in the final partition only if at least
#'     fraction \code{merge_alpha} of fields agree.
#' }
#'
#' @param sim_list Named list of numeric vectors (output of
#'   \code{er_similarity()}), one entry per field.
#' @param pairs A tibble with columns \code{idx1} and \code{idx2} from
#'   \code{er_block()}.
#' @param n Integer. Total number of records.
#' @param cluster_method Character. Clustering method passed to
#'   \code{er_cluster()} for each field. Default \code{"threshold_cc"}.
#' @param merge_alpha Numeric in (0, 1]. Fraction of fields that must agree
#'   to merge a pair. \code{0.5} = majority vote (default).
#'   \code{1.0} = all fields must agree (intersection).
#' @param threshold Numeric. Similarity threshold forwarded to
#'   \code{er_cluster()}. Default \code{0.5}.
#' @param min_pairs Integer. Minimum number of non-NA pair similarities a
#'   field must have to be included. Fields below this are silently skipped.
#'   Default \code{10L}.
#' @param ... Additional arguments forwarded to \code{er_cluster()}.
#'
#' @return Integer vector of consensus cluster labels (length \code{n}).
#' @examples
#' \dontrun{
#' sim <- er_similarity(df, pairs)
#' # Approach 1 (default pipeline)
#' wt  <- er_weights(sim)
#' S   <- er_pairs_to_sparse(pairs, er_combine(sim, wt), n)
#' labs_a1 <- er_cluster(S, "louvain")
#'
#' # Approach 2: per-field clustering then consensus
#' labs_a2 <- er_field_ensemble(sim, pairs, n,
#'                              cluster_method = "threshold_cc",
#'                              merge_alpha    = 0.5)
#' }
#' @export
er_field_ensemble <- function(sim_list,
                               pairs,
                               n,
                               cluster_method = "threshold_cc",
                               merge_alpha    = 0.5,
                               threshold      = 0.5,
                               min_pairs      = 10L,
                               ...) {

  if (!length(sim_list)) stop("er_field_ensemble: sim_list is empty.")
  stopifnot(is.data.frame(pairs), all(c("idx1", "idx2") %in% names(pairs)))

  cluster_list <- list()

  for (fname in names(sim_list)) {
    sv   <- sim_list[[fname]]
    n_ok <- sum(!is.na(sv))
    if (n_ok < min_pairs) {
      message(sprintf("er_field_ensemble: skipping field '%s' (%d non-NA pairs < min_pairs=%d)",
                      fname, n_ok, min_pairs))
      next
    }
    # Build per-field sparse similarity matrix (NAs filled with 0)
    S_k <- er_pairs_to_sparse(pairs, sv, n, na_fill = 0)
    diag(S_k) <- 1

    labs <- tryCatch(
      er_cluster(S_k, method = cluster_method, threshold = threshold, ...),
      error = function(e) {
        message(sprintf("er_field_ensemble: clustering failed for field '%s': %s",
                        fname, conditionMessage(e)))
        NULL
      }
    )
    if (!is.null(labs)) cluster_list[[fname]] <- labs
  }

  if (!length(cluster_list)) {
    warning("er_field_ensemble: no field produced a valid clustering; ",
            "returning one cluster per record.")
    return(seq_len(n))
  }

  message(sprintf("er_field_ensemble: merging %d field partition(s) with alpha=%.2f",
                  length(cluster_list), merge_alpha))
  er_consensus(cluster_list, n = n, alpha = merge_alpha)
}
