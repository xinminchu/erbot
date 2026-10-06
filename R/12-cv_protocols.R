########################################
# File: R/12-cv_protocols.R
# Redesigned evaluation engine: two protocols for honest ER benchmarking.
#
# Protocol A (er_protocol_a):
#   Full-data principled parameter selection.
#   Sweep (cluster_method, tau) on full data, pick best by ARI.
#   "Optimistic" estimate: tuned and evaluated on the same data.
#
# Protocol B (er_protocol_b), rewritten 2026-10-06 (Phase 2b):
#   Entity-disjoint Selection/Test split (test = true held-out).
#   Inside Selection: entity-disjoint K-fold CV, each fold sweeps the grid
#   on its training part, best params by training ARI.
#   Consensus (tau=median, method=majority vote) across folds.
#   Weights learned on Selection; Test clustered ONCE with consensus params;
#   the single test ARI is evaluated ONCE. Genuinely honest estimate.
#
# er_delta_ari: ARI_A - ARI_B (the leakage/overfitting gap).
########################################

# ── er_split() ────────────────────────────────────────────────────────────────

#' Create K entity-disjoint (or record-level) CV folds
#'
#' Splits \code{n} records into \code{k} folds for cross-validation.
#' When \code{entity_disjoint = TRUE} (default), all records of the same
#' true entity are placed in the same fold, preventing information leakage
#' across folds.  Fold sizes are balanced by assigning entities greedily
#' (largest entities first, each assigned to the fold with fewest records).
#'
#' @param id_vec Character vector of record IDs (length n).
#' @param truth Ground truth accepted by \code{\link{er_truth_from_any}}:
#'   named integer vector, \code{tibble(id, cluster_id)}, file path, or
#'   \code{NULL}.  Required when \code{entity_disjoint = TRUE}.
#' @param k Integer. Number of folds. Default \code{5L}.
#' @param entity_disjoint Logical. If \code{TRUE} (default), entities are
#'   assigned as units; no entity spans two folds.  If \code{FALSE}, records
#'   are assigned independently at random.
#' @param seed Integer. RNG seed. Default \code{42L}.
#'
#' @return A list of \code{k} elements, each a list with:
#'   \describe{
#'     \item{fold}{Integer fold index (1..k).}
#'     \item{train_idx}{Integer vector: indices of training records in \code{id_vec}.}
#'     \item{test_idx}{Integer vector: indices of test records in \code{id_vec}.}
#'   }
#' @export
er_split <- function(id_vec, truth = NULL, k = 5L, entity_disjoint = TRUE,
                     seed = 42L) {
  k <- as.integer(k)
  n <- length(id_vec)
  if (k < 2L || k > n)
    stop(sprintf("er_split: k must be between 2 and n=%d.", n))

  # BUG-19 (2026-06-12): preserve the caller's global RNG state. The same
  # set.seed(seed) still runs, so all sampling inside this function is
  # byte-identical to previous behaviour; only the side effect on the
  # caller's RNG stream is removed.
  .rng_old <- if (exists(".Random.seed", envir = globalenv(), inherits = FALSE))
    get(".Random.seed", envir = globalenv(), inherits = FALSE) else NULL
  on.exit({
    if (!is.null(.rng_old)) {
      assign(".Random.seed", .rng_old, envir = globalenv())
    } else if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
      rm(".Random.seed", envir = globalenv())
    }
  }, add = TRUE)
  set.seed(seed)

  if (!entity_disjoint || is.null(truth)) {
    fold_assign <- sample(rep(seq_len(k), length.out = n))
    return(lapply(seq_len(k), function(f) {
      test_idx <- which(fold_assign == f)
      list(fold = f, train_idx = setdiff(seq_len(n), test_idx),
           test_idx = test_idx)
    }))
  }

  # Entity-disjoint: assign whole entities to folds ───────────────────────────
  truth_tbl        <- er_truth_from_any(truth)
  id_to_idx        <- setNames(seq_len(n), id_vec)
  labelled         <- truth_tbl[truth_tbl$id %in% id_vec, , drop = FALSE]
  labelled$rec_idx <- id_to_idx[as.character(labelled$id)]

  entity_groups <- split(labelled$rec_idx, labelled$cluster_id)
  sizes         <- lengths(entity_groups)
  ord           <- order(sizes, decreasing = TRUE)   # largest entities first
  entity_groups <- entity_groups[ord]
  sizes         <- sizes[ord]

  # Greedy: each entity goes to the fold with the fewest records so far
  fold_counts  <- integer(k)
  entity_folds <- integer(length(entity_groups))
  for (i in seq_along(entity_groups)) {
    f               <- which.min(fold_counts)
    entity_folds[i] <- f
    fold_counts[f]  <- fold_counts[f] + sizes[i]
  }

  fold_assign <- integer(n)
  for (i in seq_along(entity_groups))
    fold_assign[entity_groups[[i]]] <- entity_folds[i]

  # Unlabelled records (singletons not in truth): assign randomly
  unlab <- which(fold_assign == 0L)
  if (length(unlab))
    fold_assign[unlab] <- sample(rep(seq_len(k), length.out = length(unlab)))

  lapply(seq_len(k), function(f) {
    test_idx <- which(fold_assign == f)
    list(fold = f, train_idx = setdiff(seq_len(n), test_idx),
         test_idx = test_idx)
  })
}


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

#' Protocol B: entity-disjoint CV tune -> true held-out evaluation.
#'
#' Honest-estimate protocol, rewritten 2026-10-06 (Phase 2b). The old version
#' tuned on CV folds and then refit + evaluated on the FULL data, so the
#' reported ARI was computed on records that drove parameter selection --
#' not a held-out estimate at all. The rewrite is:
#'
#' 1. Split ENTITIES (never records) once into **Selection** and **Test**
#'    via [er_stratified_three_way_split()]. Test is the true held-out set.
#' 2. Inside Selection only: entity-disjoint K-fold CV (via [er_split()]);
#'    each fold sweeps (method, tau) on its training part and records the
#'    best params by training ARI.
#' 3. Consensus params (tau = median snapped to grid, method = majority vote
#'    with SD tie-break) across folds.
#' 4. Learn field weights on Selection, cluster the **Test** records once
#'    with the consensus params, and evaluate **once** against test truth.
#'
#' Test labels are never used for any selection or fitting decision.
#'
#' @inheritParams er_grid_sweep
#' @param n_folds Integer. Number of CV folds inside the selection set.
#'   Default 5L.
#' @param test_fraction Numeric in (0, 1). Fraction of records reserved for
#'   the held-out test set. Default 0.3.
#' @param base_seed Integer. Base RNG seed; the split uses `base_seed`, fold
#'   k uses `base_seed + k`. Default 42L.
#'
#' @return A list:
#'   \describe{
#'     \item{fold_best}{data.frame with columns fold, method, tau, train_ARI
#'       (from the selection-set CV).}
#'     \item{consensus_method}{Character. Consensus method.}
#'     \item{consensus_tau}{Numeric. Consensus tau (nearest grid value to median).}
#'     \item{ARI}{Numeric. **One-time held-out test ARI** at consensus params.}
#'     \item{Bcubed_F}{Numeric. One-time held-out B-cubed F.}
#'     \item{Vmeasure}{Numeric. One-time held-out V-measure.}
#'     \item{labels}{Integer vector of cluster labels **for the test records**,
#'       aligned to `test_id_vec`. (Changed 2026-10-06: previously full-data
#'       labels.)}
#'     \item{test_id_vec}{Character vector of held-out test record IDs.}
#'     \item{selection_idx, test_idx}{Integer index vectors into `id_vec`.}
#'     \item{n_selection, n_test}{Partition sizes.}
#'   }
#' @export
er_protocol_b <- function(sim_list, pairs, n, truth_tbl, id_vec,
                          method_grid   = DEFAULT_METHOD_GRID,
                          tau_grid      = DEFAULT_TAU_GRID,
                          weight_method = "ari",
                          n_folds       = 5L,
                          test_fraction = 0.3,
                          base_seed     = 42L,
                          verbose       = TRUE) {

  if (!is.numeric(test_fraction) || length(test_fraction) != 1L ||
      test_fraction <= 0 || test_fraction >= 1) {
    stop("er_protocol_b: 'test_fraction' must be a single number in (0, 1).")
  }

  # ── Step 1: one entity-disjoint Selection/Test split ──────────────────────
  if (verbose) message("[Protocol B] Splitting entities into selection/test ",
                       sprintf("(test_fraction=%.2f)...", test_fraction))
  sp <- er_stratified_three_way_split(
    id_vec, truth_tbl,
    fractions = c(selection = 1 - test_fraction, test = test_fraction),
    seed = base_seed
  )
  sel_idx  <- sp$indices$selection
  test_idx <- sp$indices$test
  n_sel <- length(sel_idx); n_tst <- length(test_idx)
  if (verbose) message(sprintf("[Protocol B] Selection: %d records | Test (held-out): %d records",
                               n_sel, n_tst))

  n_folds <- as.integer(n_folds)
  if (n_folds > n_sel) {
    message(sprintf("[Protocol B] n_folds=%d > selection n=%d; using n_folds=%d.",
                    n_folds, n_sel, max(2L, n_sel)))
    n_folds <- max(2L, n_sel)
  }
  if (n_folds < 2L) stop("er_protocol_b: selection set too small for CV.")

  # ── Step 2: entity-disjoint K-fold CV inside Selection ────────────────────
  sel_id_vec <- id_vec[sel_idx]
  sel_truth  <- truth_tbl[truth_tbl$id %in% sel_id_vec, , drop = FALSE]
  folds <- er_split(sel_id_vec, truth = sel_truth, k = n_folds,
                    entity_disjoint = TRUE, seed = base_seed + 1L)

  fold_best_rows <- list()

  for (k in seq_len(n_folds)) {
    if (verbose) message(sprintf("[Protocol B] Fold %d/%d: sweeping selection training set...",
                                 k, n_folds))

    # fold train/test are relative to the selection ordering; map to global
    fold_train_global <- sel_idx[folds[[k]]$train_idx]

    sub <- .subset_to_fold(sim_list, pairs, id_vec,
                           truth_vec = NULL,  # not needed; truth_tbl used by .one_cell
                           idx = fold_train_global)

    train_ids   <- id_vec[fold_train_global]
    train_truth <- truth_tbl[truth_tbl$id %in% train_ids, , drop = FALSE]

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
  if (is.null(fold_best) || !nrow(fold_best))
    stop("er_protocol_b: all selection folds failed; cannot form consensus.")

  # ── Step 3: consensus params ──────────────────────────────────────────────
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

  # ── Step 4: weights on Selection, cluster Test once, evaluate once ─────────
  if (verbose) message("[Protocol B] Learning weights on selection set...")
  sel_sub <- .subset_to_fold(sim_list, pairs, id_vec, truth_vec = NULL, idx = sel_idx)
  wt <- tryCatch(
    er_weights(sel_sub$sim_list, pairs = sel_sub$pairs, truth = sel_truth,
               id_vec = sel_sub$id_vec, method = weight_method),
    error = function(e) {
      warning("er_protocol_b: weight learning on selection failed (",
              e$message, "); using equal weights.")
      stats::setNames(rep(1 / length(sel_sub$sim_list), length(sel_sub$sim_list)),
                       names(sel_sub$sim_list))
    }
  )

  if (verbose) message("[Protocol B] Clustering held-out test records (once)...")
  tst_sub <- .subset_to_fold(sim_list, pairs, id_vec, truth_vec = NULL, idx = test_idx)
  set.seed(base_seed)
  test_labels <- tryCatch({
    if (consensus_method == "field_ensemble") {
      er_field_ensemble(tst_sub$sim_list, tst_sub$pairs, tst_sub$n,
                        cluster_method = "threshold_cc",
                        merge_alpha    = consensus_tau,
                        threshold      = 0.5,
                        min_pairs      = 5L)
    } else {
      s_comb   <- er_combine(tst_sub$sim_list, weights = wt)
      S        <- er_pairs_to_sparse(tst_sub$pairs, s_comb, tst_sub$n)
      diag(S)  <- 1
      er_cluster(S, method = consensus_method, threshold = consensus_tau)
    }
  }, error = function(e) {
    warning("er_protocol_b: test clustering failed: ", e$message)
    seq_len(tst_sub$n)
  })

  # ── Step 5: ONE-TIME evaluation on the held-out test set ───────────────────
  test_truth <- truth_tbl[truth_tbl$id %in% tst_sub$id_vec, , drop = FALSE]
  if (nrow(test_truth) < 2L) {
    warning("er_protocol_b: fewer than 2 labelled test records; metrics are NA.")
    ev <- NULL
  } else {
    ev <- tryCatch(
      er_evaluate(list(m = test_labels), truth = test_truth,
                  id_vec = tst_sub$id_vec, eval_mode = "labeled_only"),
      error = function(e) NULL
    )
  }

  ari      <- if (!is.null(ev) && nrow(ev)) ev$ARI[1]      else NA_real_
  bcubed_f <- if (!is.null(ev) && nrow(ev)) ev$Bcubed_F[1] else NA_real_
  vmeasure <- if (!is.null(ev) && nrow(ev)) ev$Vmeasure[1] else NA_real_

  if (verbose) message(sprintf(
    "[Protocol B] One-time held-out test ARI at consensus params: %.4f", ari))

  list(
    fold_best        = fold_best,
    consensus_method = consensus_method,
    consensus_tau    = consensus_tau,
    ARI              = ari,
    Bcubed_F         = bcubed_f,
    Vmeasure         = vmeasure,
    labels           = as.integer(test_labels),
    test_id_vec      = tst_sub$id_vec,
    selection_idx    = sel_idx,
    test_idx         = test_idx,
    n_selection      = n_sel,
    n_test           = n_tst
  )
}


# ── er_delta_ari ───────────────────────────────────────────────────────────────

#' Compute the ARI overfitting gap: Protocol A minus Protocol B.
#'
#' A positive delta means full-data tuning overstates performance relative
#' to the honest held-out estimate (Protocol B evaluates once on an
#' entity-disjoint test set since 2026-10-06).
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
