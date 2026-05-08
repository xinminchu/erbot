########################################
# File: R/12-cv_protocols.R
# Redesigned evaluation engine: two protocols for honest ER benchmarking.
#
# Protocol A (er_protocol_a):
#   Full-data principled parameter selection.
#   Sweep (cluster_method, tau) on full data, pick best by ARI.
#   "Optimistic" estimate: tuned and evaluated on the same data.
#
# Protocol B (er_protocol_b):
#   CV5 tune -> consensus refit.
#   Each fold: sweep grid on training 80%, record best params.
#   Take consensus (tau=median, method=majority vote) across folds.
#   Refit on full data with consensus params, report that single ARI.
#   "Honest" estimate.
#
# er_delta_ari: ARI_A - ARI_B (the leakage/overfitting gap).
########################################

# ── Constants ──────────────────────────────────────────────────────────────────

#' Default method grid for er_grid_sweep
#' Excludes svm/gbm (supervised pairwise classifiers — truth leakage in sweep),
#' gc (optional heavy dep), hclust_ward/pam (dominated by hclust_avg in ER).
DEFAULT_METHOD_GRID <- c(
  "threshold_cc",
  "louvain",
  "leiden",
  "label_prop",
  "hclust_avg",
  "field_ensemble"
)

#' Default tau grid. Covers values from `0.2` to `0.8` in steps of `0.1`.
#' For tau-invariant methods (louvain, leiden, label_prop, hclust_avg),
#' the sweep produces identical results for all tau values; the first
#' occurrence is used by which.max().
DEFAULT_TAU_GRID <- c(0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)


# ── Internal helpers ───────────────────────────────────────────────────────────

#' Subset sim_list, pairs, id_vec, and truth_vec to a set of record indices.
#'
#' Filters pairs to those where both records are in idx, then remaps
#' idx1/idx2 from global 1..n to local 1..length(idx).
#'
#' @param sim_list Named list of numeric vectors (length = nrow(pairs)).
#' @param pairs tibble(idx1, idx2) from er_block().
#' @param id_vec Character vector of record IDs (length n).
#' @param truth_vec Integer vector of ground-truth labels (length n), or NULL.
#' @param idx Integer vector of record indices to keep (subset of 1..n).
#'
#' @return list(sim_list, pairs, n, id_vec, truth_vec) with remapped indices.
#' @keywords internal
.subset_to_fold <- function(sim_list, pairs, id_vec, truth_vec, idx) {
  # Build global->local index map
  idx_map          <- integer(max(idx))
  idx_map[idx]     <- seq_along(idx)

  # Keep only pairs where both records are in idx
  keep             <- pairs$idx1 %in% idx & pairs$idx2 %in% idx
  sub_pairs        <- pairs[keep, , drop = FALSE]
  sub_pairs$idx1   <- idx_map[sub_pairs$idx1]
  sub_pairs$idx2   <- idx_map[sub_pairs$idx2]

  sub_sim          <- lapply(sim_list, `[`, keep)
  sub_id_vec       <- id_vec[idx]
  sub_truth_vec    <- if (!is.null(truth_vec)) truth_vec[idx] else NULL

  list(
    sim_list  = sub_sim,
    pairs     = sub_pairs,
    n         = length(idx),
    id_vec    = sub_id_vec,
    truth_vec = sub_truth_vec
  )
}


#' Compute ARI for one (method, tau) cell.
#'
#' @param sim_list Named list of per-field similarity vectors.
#' @param pairs tibble(idx1, idx2).
#' @param n Integer. Number of records.
#' @param truth_tbl tibble(id, cluster_id). Ground truth.
#' @param id_vec Character vector of record IDs.
#' @param truth_vec Integer vector of ground-truth labels aligned to id_vec.
#' @param method Character. Clustering method or "field_ensemble".
#' @param tau Numeric. Threshold (or merge_alpha for field_ensemble).
#' @param weight_method Character. Passed to er_weights(). Default "ari".
#'
#' @return Named numeric: c(ARI, Bcubed_F, Vmeasure), or NAs on error.
#' @keywords internal
.one_cell <- function(sim_list, pairs, n, truth_tbl, id_vec, truth_vec,
                      method, tau, weight_method = "ari") {
  na_result <- c(ARI = NA_real_, Bcubed_F = NA_real_, Vmeasure = NA_real_)

  labels <- tryCatch({
    if (method == "field_ensemble") {
      er_field_ensemble(
        sim_list       = sim_list,
        pairs          = pairs,
        n              = n,
        cluster_method = "threshold_cc",
        merge_alpha    = tau,
        threshold      = 0.5,
        min_pairs      = 5L
      )
    } else {
      wt       <- er_weights(sim_list, pairs = pairs, truth = truth_tbl,
                             id_vec = id_vec, method = weight_method)
      s_comb   <- er_combine(sim_list, weights = wt)
      S        <- er_pairs_to_sparse(pairs, s_comb, n)
      diag(S)  <- 1
      er_cluster(S, method = method, threshold = tau)
    }
  }, error = function(e) {
    message(sprintf("  [.one_cell] method=%s tau=%.2f error: %s", method, tau, e$message))
    NULL
  })

  if (is.null(labels)) return(na_result)

  ev <- tryCatch(
    er_evaluate(list(m = labels), truth = truth_tbl, id_vec = id_vec,
                eval_mode = "labeled_only"),
    error = function(e) NULL
  )
  if (is.null(ev) || !nrow(ev)) return(na_result)

  c(
    ARI      = ev$ARI[1],
    Bcubed_F = ev$Bcubed_F[1],
    Vmeasure = ev$Vmeasure[1]
  )
}


# ── er_grid_sweep ──────────────────────────────────────────────────────────────

#' Sweep (cluster_method, tau) grid and return an ARI table.
#'
#' For tau-invariant methods (louvain, leiden, label_prop, hclust_avg) the
#' function runs each method only once (at the median tau) and replicates
#' the result across all tau rows to avoid redundant computation.
#'
#' @param sim_list Named list of per-field similarity vectors.
#' @param pairs tibble(idx1, idx2).
#' @param n Integer. Number of records.
#' @param truth_tbl tibble(id, cluster_id). Ground truth.
#' @param id_vec Character vector of record IDs (length n).
#' @param method_grid Character vector of methods to sweep.
#' @param tau_grid Numeric vector of tau / merge_alpha values.
#' @param weight_method Character. Weight learning method. Default "ari".
#' @param seed Integer. RNG seed for stochastic methods. Default 42L.
#' @param verbose Logical. Print progress. Default TRUE.
#'
#' @return data.frame with columns: method, tau, ARI, Bcubed_F, Vmeasure.
#' @export
er_grid_sweep <- function(sim_list, pairs, n, truth_tbl, id_vec,
                          method_grid  = DEFAULT_METHOD_GRID,
                          tau_grid     = DEFAULT_TAU_GRID,
                          weight_method = "ari",
                          seed         = 42L,
                          verbose      = TRUE) {

  # tau-invariant methods: run once, replicate
  TAU_INVARIANT <- c("louvain", "leiden", "label_prop", "hclust_avg",
                     "hclust_ward", "pam")

  # hclust requires a full distance matrix (O(n^2) memory); skip for large n
  HCLUST_MAX_N <- 5000L
  # field_ensemble builds one igraph per field × tau; expensive for large n nodes
  ENSEMBLE_MAX_N <- 15000L
  n_pairs <- nrow(pairs)

  if (n > HCLUST_MAX_N) {
    skipped <- c("hclust_avg", "hclust_ward", "pam")
    skipped_present <- intersect(method_grid, skipped)
    if (length(skipped_present) && verbose)
      message(sprintf("  [er_grid_sweep] n=%d > %d: skipping %s (memory limit)",
                      n, HCLUST_MAX_N, paste(skipped_present, collapse = ", ")))
    method_grid <- setdiff(method_grid, skipped)
  }
  if (n > ENSEMBLE_MAX_N && "field_ensemble" %in% method_grid) {
    if (verbose)
      message(sprintf("  [er_grid_sweep] n=%d > %d: skipping field_ensemble (speed limit)",
                      n, ENSEMBLE_MAX_N))
    method_grid <- setdiff(method_grid, "field_ensemble")
  }

  rows <- list()
  for (method in method_grid) {
    if (method %in% TAU_INVARIANT) {
      # Run once at median tau
      ref_tau <- tau_grid[ceiling(length(tau_grid) / 2)]
      set.seed(seed)
      if (verbose) message(sprintf("  sweep: method=%-15s tau=%.2f (invariant, run once)",
                                   method, ref_tau))
      metrics <- .one_cell(sim_list, pairs, n, truth_tbl, id_vec,
                           NULL,   # truth_vec not used here
                           method, ref_tau, weight_method)
      for (tau in tau_grid) {
        rows[[length(rows) + 1L]] <- data.frame(
          method = method, tau = tau,
          ARI = metrics["ARI"], Bcubed_F = metrics["Bcubed_F"],
          Vmeasure = metrics["Vmeasure"],
          stringsAsFactors = FALSE
        )
      }
    } else {
      for (tau in tau_grid) {
        set.seed(seed)
        if (verbose) message(sprintf("  sweep: method=%-15s tau=%.2f", method, tau))
        metrics <- .one_cell(sim_list, pairs, n, truth_tbl, id_vec,
                             NULL, method, tau, weight_method)
        rows[[length(rows) + 1L]] <- data.frame(
          method = method, tau = tau,
          ARI = metrics["ARI"], Bcubed_F = metrics["Bcubed_F"],
          Vmeasure = metrics["Vmeasure"],
          stringsAsFactors = FALSE
        )
      }
    }
  }

  do.call(rbind, rows)
}


# ── er_protocol_a ──────────────────────────────────────────────────────────────

#' Protocol A: full-data principled parameter selection.
#'
#' Sweeps (method, tau) on full data using gold labels, picks the combination
#' maximising ARI. This is the "optimistic" estimate: the same data used to
#' tune parameters is also used to report performance.
#'
#' @inheritParams er_grid_sweep
#'
#' @return A list:
#'   \describe{
#'     \item{sweep}{Full sweep data.frame (all method × tau cells).}
#'     \item{best_method}{Character. Best method.}
#'     \item{best_tau}{Numeric. Best tau.}
#'     \item{ARI}{Numeric. ARI at (best_method, best_tau) on full data.}
#'     \item{Bcubed_F}{Numeric.}
#'     \item{Vmeasure}{Numeric.}
#'     \item{labels}{Integer vector of cluster labels at best params.}
#'   }
#' @export
er_protocol_a <- function(sim_list, pairs, n, truth_tbl, id_vec,
                          method_grid   = DEFAULT_METHOD_GRID,
                          tau_grid      = DEFAULT_TAU_GRID,
                          weight_method = "ari",
                          seed          = 42L,
                          verbose       = TRUE) {

  if (verbose) message("[Protocol A] Full-data sweep...")
  sweep <- er_grid_sweep(sim_list, pairs, n, truth_tbl, id_vec,
                         method_grid, tau_grid, weight_method, seed, verbose)

  # Pick best by ARI (break ties by Bcubed_F)
  valid <- sweep[!is.na(sweep$ARI), ]
  if (!nrow(valid)) stop("er_protocol_a: all sweep cells returned NA.")
  best_idx    <- which.max(valid$ARI + 1e-9 * valid$Bcubed_F)
  best_method <- valid$method[best_idx]
  best_tau    <- valid$tau[best_idx]

  if (verbose) message(sprintf("[Protocol A] Best: method=%s tau=%.2f ARI=%.4f",
                               best_method, best_tau, valid$ARI[best_idx]))

  # Recompute labels at best params (already run but .one_cell doesn't return labels)
  set.seed(seed)
  labels <- tryCatch({
    if (best_method == "field_ensemble") {
      er_field_ensemble(sim_list, pairs, n,
                        cluster_method = "threshold_cc",
                        merge_alpha    = best_tau,
                        threshold      = 0.5,
                        min_pairs      = 5L)
    } else {
      wt      <- er_weights(sim_list, pairs = pairs, truth = truth_tbl,
                            id_vec = id_vec, method = weight_method)
      s_comb  <- er_combine(sim_list, weights = wt)
      S       <- er_pairs_to_sparse(pairs, s_comb, n)
      diag(S) <- 1
      er_cluster(S, method = best_method, threshold = best_tau)
    }
  }, error = function(e) {
    warning("er_protocol_a: could not recover labels: ", e$message)
    seq_len(n)
  })

  list(
    sweep       = sweep,
    best_method = best_method,
    best_tau    = best_tau,
    ARI         = valid$ARI[best_idx],
    Bcubed_F    = valid$Bcubed_F[best_idx],
    Vmeasure    = valid$Vmeasure[best_idx],
    labels      = as.integer(labels)
  )
}


# ── er_protocol_b ──────────────────────────────────────────────────────────────

#' Protocol B: CV5 tune -> consensus refit (honest estimate).
#'
#' For each of 5 folds: sweep (method, tau) grid on the training 80%, record
#' the best params by training ARI. Select consensus params (tau = median,
#' method = majority vote) across folds. Refit on full data with consensus
#' params. Report the single full-data ARI as the honest performance estimate.
#'
#' @inheritParams er_grid_sweep
#' @param n_folds Integer. Number of CV folds. Default 5L.
#' @param base_seed Integer. Base RNG seed; fold k uses base_seed + k. Default 42L.
#'
#' @return A list:
#'   \describe{
#'     \item{fold_best}{data.frame with columns fold, method, tau, train_ARI.}
#'     \item{consensus_method}{Character. Consensus method.}
#'     \item{consensus_tau}{Numeric. Consensus tau (nearest grid value to median).}
#'     \item{ARI}{Numeric. Full-data ARI at consensus params.}
#'     \item{Bcubed_F}{Numeric.}
#'     \item{Vmeasure}{Numeric.}
#'     \item{labels}{Integer vector of full-data cluster labels.}
#'   }
#' @export
er_protocol_b <- function(sim_list, pairs, n, truth_tbl, id_vec,
                          method_grid   = DEFAULT_METHOD_GRID,
                          tau_grid      = DEFAULT_TAU_GRID,
                          weight_method = "ari",
                          n_folds       = 5L,
                          base_seed     = 42L,
                          verbose       = TRUE) {

  set.seed(base_seed)
  fold_idx <- split(sample(n), cut(seq_len(n), n_folds, labels = FALSE))

  fold_best_rows <- list()

  for (k in seq_len(n_folds)) {
    if (verbose) message(sprintf("[Protocol B] Fold %d/%d: sweeping training set...", k, n_folds))

    test_idx  <- fold_idx[[k]]
    train_idx <- setdiff(seq_len(n), test_idx)

    sub <- .subset_to_fold(sim_list, pairs, id_vec,
                           truth_vec = NULL,  # not needed; truth_tbl used by .one_cell
                           idx = train_idx)

    # Build training truth_tbl (subset to training ids)
    train_ids      <- id_vec[train_idx]
    train_truth    <- truth_tbl[truth_tbl$id %in% train_ids, ]

    sweep_k <- er_grid_sweep(
      sim_list      = sub$sim_list,
      pairs         = sub$pairs,
      n             = sub$n,
      truth_tbl     = train_truth,
      id_vec        = sub$id_vec,
      method_grid   = method_grid,
      tau_grid      = tau_grid,
      weight_method = weight_method,
      seed          = base_seed + k,
      verbose       = FALSE
    )

    valid_k  <- sweep_k[!is.na(sweep_k$ARI), ]
    if (!nrow(valid_k)) {
      message(sprintf("  Fold %d: all cells NA, skipping.", k))
      next
    }
    best_k   <- valid_k[which.max(valid_k$ARI), ]
    fold_best_rows[[k]] <- data.frame(
      fold       = k,
      method     = best_k$method,
      tau        = best_k$tau,
      train_ARI  = best_k$ARI,
      stringsAsFactors = FALSE
    )
    if (verbose) message(sprintf("  Fold %d best: method=%s tau=%.2f train_ARI=%.4f",
                                 k, best_k$method, best_k$tau, best_k$ARI))
  }

  fold_best <- do.call(rbind, fold_best_rows)

  # Consensus: tau = median (snapped to nearest grid value), method = majority vote
  consensus_tau_raw <- stats::median(fold_best$tau)
  consensus_tau     <- tau_grid[which.min(abs(tau_grid - consensus_tau_raw))]

  method_votes    <- sort(table(fold_best$method), decreasing = TRUE)
  consensus_method <- names(method_votes)[1]
  # Tie-break: if top two methods share the highest vote count, pick the one
  # with lower SD of train_ARI among its winning folds (more stable).
  if (length(method_votes) > 1 && method_votes[1] == method_votes[2]) {
    top_methods <- names(method_votes[method_votes == method_votes[1]])
    sds <- vapply(top_methods, function(m) {
      rows <- fold_best[fold_best$method == m, ]
      if (nrow(rows) < 2L) Inf else stats::sd(rows$train_ARI)
    }, numeric(1L))
    consensus_method <- top_methods[which.min(sds)]
  }

  if (verbose) message(sprintf(
    "[Protocol B] Consensus: method=%s tau=%.2f (median raw=%.2f, votes: %s)",
    consensus_method, consensus_tau, consensus_tau_raw,
    paste(names(method_votes), method_votes, sep = "=", collapse = ", ")
  ))

  # Refit on full data with consensus params
  if (verbose) message("[Protocol B] Refitting on full data with consensus params...")
  set.seed(base_seed)
  labels <- tryCatch({
    if (consensus_method == "field_ensemble") {
      er_field_ensemble(sim_list, pairs, n,
                        cluster_method = "threshold_cc",
                        merge_alpha    = consensus_tau,
                        threshold      = 0.5,
                        min_pairs      = 5L)
    } else {
      wt      <- er_weights(sim_list, pairs = pairs, truth = truth_tbl,
                            id_vec = id_vec, method = weight_method)
      s_comb  <- er_combine(sim_list, weights = wt)
      S       <- er_pairs_to_sparse(pairs, s_comb, n)
      diag(S) <- 1
      er_cluster(S, method = consensus_method, threshold = consensus_tau)
    }
  }, error = function(e) {
    warning("er_protocol_b: refit failed: ", e$message)
    seq_len(n)
  })

  ev <- tryCatch(
    er_evaluate(list(m = labels), truth = truth_tbl, id_vec = id_vec,
                eval_mode = "labeled_only"),
    error = function(e) NULL
  )

  ari      <- if (!is.null(ev)) ev$ARI[1]      else NA_real_
  bcubed_f <- if (!is.null(ev)) ev$Bcubed_F[1] else NA_real_
  vmeasure <- if (!is.null(ev)) ev$Vmeasure[1] else NA_real_

  if (verbose) message(sprintf("[Protocol B] Full-data ARI at consensus params: %.4f", ari))

  list(
    fold_best        = fold_best,
    consensus_method = consensus_method,
    consensus_tau    = consensus_tau,
    ARI              = ari,
    Bcubed_F         = bcubed_f,
    Vmeasure         = vmeasure,
    labels           = as.integer(labels)
  )
}


# ── er_delta_ari ───────────────────────────────────────────────────────────────

#' Compute the ARI overfitting gap: Protocol A minus Protocol B.
#'
#' A positive delta means full-data tuning overstates performance relative
#' to the honest CV-consensus estimate.
#'
#' @param proto_a List returned by er_protocol_a().
#' @param proto_b List returned by er_protocol_b().
#'
#' @return Named numeric: c(delta_ARI, ARI_A, ARI_B).
#' @export
er_delta_ari <- function(proto_a, proto_b) {
  c(
    delta_ARI = proto_a$ARI - proto_b$ARI,
    ARI_A     = proto_a$ARI,
    ARI_B     = proto_b$ARI
  )
}
