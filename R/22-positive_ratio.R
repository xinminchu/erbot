########################################
# File: R/22-positive_ratio.R
# Pre-registered split design: raise the positive ratio without breaking the
# split discipline.
#
# Design (pre-registered):
#   1. Split ENTITIES, never records or pairs, into Fit / Validation / Test.
#      No entity may span two partitions.
#   2. Gold labels may be used ONLY to design the split (balance entity-size
#      strata and positive-pair quotas). They never enter similarity features
#      (the BUG-22 leakage guard in er_diagnose() still applies).
#   3. One fixed seed. If the quotas are infeasible because entities are
#      indivisible, the function errors with the achieved counts.
#      It NEVER redraws the seed to make the numbers look nicer.
#   4. Positive augmentation (oversampling / appending positives) happens in
#      the Fit partition ONLY. Validation and Test keep natural prevalence.
#   5. Natural prevalence and simulated (augmented) prevalence are reported
#      separately and are never averaged together.
########################################

.er_partition_names <- function(fractions) {
  nms <- names(fractions)
  if (is.null(nms) || any(!nzchar(nms))) {
    nms <- c("fit", "validation", "test")[seq_along(fractions)]
  }
  nms
}

.er_validate_fractions <- function(fractions) {
  if (!is.numeric(fractions) || length(fractions) < 2L) {
    stop("er_stratified_three_way_split: 'fractions' must be a named numeric vector with at least two partitions.")
  }
  if (any(!is.finite(fractions)) || any(fractions <= 0)) {
    stop("er_stratified_three_way_split: all fractions must be positive and finite.")
  }
  if (abs(sum(fractions) - 1) > 1e-8) {
    stop(sprintf(
      "er_stratified_three_way_split: fractions must sum to 1 (got %.6f). Fix the pre-registered fractions; do not renormalise silently.",
      sum(fractions)
    ))
  }
  fractions
}

#' Entity-disjoint stratified three-way split with positive-pair quotas
#'
#' Assigns whole entities (gold clusters) to Fit / Validation / Test so that
#' record fractions and, where requested, positive-pair quotas are met as
#' closely as entity indivisibility allows. Entities are processed largest
#' (by positive-pair contribution) first and greedily assigned to the
#' partition with the greatest remaining deficit.
#'
#' Gold labels are used only to design this split. They are not returned as
#' model features and must not be passed to \code{er_similarity()}.
#'
#' @param id_vec Character vector of record IDs (length n).
#' @param truth Ground truth accepted by \code{\link{er_truth_from_any}}.
#'   Records missing from \code{truth} are treated as singleton entities.
#' @param fractions Named numeric vector of target record fractions summing
#'   to 1, e.g. \code{c(fit = 0.35, validation = 0.35, test = 0.30)}.
#' @param positive_pair_targets Optional named numeric vector of target
#'   positive-pair counts per partition. If \code{NULL}, targets are the
#'   total positive pairs distributed in proportion to \code{fractions}
#'   (soft targets used only for balancing).
#' @param min_positive_pairs Optional named numeric vector of hard minima.
#'   If a minimum cannot be met, the function errors with the achieved
#'   counts instead of redrawing.
#' @param seed Integer. The single pre-registered seed.
#'
#' @return A list with:
#' \describe{
#'   \item{\code{assignment}}{tibble(id, partition).}
#'   \item{\code{indices}}{Named list of integer index vectors into \code{id_vec}.}
#'   \item{\code{report}}{Per-partition diagnostics: records, entities,
#'     positive pairs, candidate pairs, natural positive-pair ratio.}
#'   \item{\code{seed}, \code{fractions}}{The pre-registered design.}
#' }
#' @export
er_stratified_three_way_split <- function(id_vec,
                                          truth,
                                          fractions = c(fit = 0.35,
                                                        validation = 0.35,
                                                        test = 0.30),
                                          positive_pair_targets = NULL,
                                          min_positive_pairs = NULL,
                                          seed = 42L) {
  fractions <- .er_validate_fractions(fractions)
  parts <- .er_partition_names(fractions)
  names(fractions) <- parts
  n <- length(id_vec)
  if (n < length(parts)) {
    stop("er_stratified_three_way_split: fewer records than partitions.")
  }

  truth_tbl <- er_truth_from_any(truth)
  if (is.null(truth_tbl) || !nrow(truth_tbl)) {
    stop("er_stratified_three_way_split: could not parse 'truth'. A label-aware split design needs gold clusters; without them, use er_split() instead.")
  }

  # Align gold clusters to id_vec; records absent from truth are singletons.
  cluster_vec <- setNames(truth_tbl$cluster_id, as.character(truth_tbl$id))
  gold <- unname(cluster_vec[as.character(id_vec)])
  missing <- is.na(gold)
  if (any(missing)) {
    gold[missing] <- max(c(gold[!missing], 0L), na.rm = TRUE) +
      seq_len(sum(missing))
  }
  gold <- as.integer(gold)

  entities <- split(seq_len(n), gold)
  entity_sizes <- lengths(entities)
  entity_pairs <- choose(entity_sizes, 2)
  total_positive_pairs <- sum(entity_pairs)

  target_records <- fractions * n
  if (is.null(positive_pair_targets)) {
    target_pairs <- fractions * total_positive_pairs
    hard_targets <- FALSE
  } else {
    if (is.null(names(positive_pair_targets)) ||
        !all(parts %in% names(positive_pair_targets))) {
      stop("er_stratified_three_way_split: 'positive_pair_targets' must be named with one entry per partition.")
    }
    target_pairs <- as.numeric(positive_pair_targets[parts])
    hard_targets <- TRUE
  }

  if (!is.null(min_positive_pairs)) {
    if (is.null(names(min_positive_pairs)) ||
        !all(parts %in% names(min_positive_pairs))) {
      stop("er_stratified_three_way_split: 'min_positive_pairs' must be named with one entry per partition.")
    }
  }

  assignment_partition <- er_with_seed(seed, {
    # Largest positive-pair contributors first; random tie-break under the
    # single pre-registered seed (restored afterwards by er_with_seed()).
    ord <- order(-entity_pairs, -entity_sizes, stats::runif(length(entities)))
    entities_o <- entities[ord]
    pairs_o <- entity_pairs[ord]
    sizes_o <- entity_sizes[ord]

    rec_counts <- setNames(numeric(length(parts)), parts)
    pair_counts <- setNames(numeric(length(parts)), parts)
    part_of_entity <- character(length(entities_o))

    for (i in seq_along(entities_o)) {
      rec_deficit <- (target_records - rec_counts) /
        pmax(target_records, 1)
      pair_deficit <- (target_pairs - pair_counts) /
        pmax(target_pairs, 1)
      # Once a partition is over its record target, penalise it so a giant
      # entity does not keep being dumped into the same partition.
      overshoot <- pmax(0, (rec_counts - target_records) /
                          pmax(target_records, 1))
      score <- rec_deficit + pair_deficit - 2 * overshoot
      chosen <- parts[which.max(score)]
      part_of_entity[i] <- chosen
      rec_counts[chosen] <- rec_counts[chosen] + sizes_o[i]
      pair_counts[chosen] <- pair_counts[chosen] + pairs_o[i]
    }

    out <- character(n)
    for (i in seq_along(entities_o)) {
      out[entities_o[[i]]] <- part_of_entity[i]
    }
    out
  })

  indices <- lapply(parts, function(p) which(assignment_partition == p))
  names(indices) <- parts

  report <- do.call(rbind, lapply(parts, function(p) {
    idx <- indices[[p]]
    cand <- choose(length(idx), 2)
    pos <- sum(choose(table(gold[idx]), 2))
    data.frame(
      partition = p,
      n_records = length(idx),
      record_fraction = length(idx) / n,
      target_record_fraction = unname(fractions[p]),
      n_entities = length(unique(gold[idx])),
      positive_pairs = pos,
      target_positive_pairs = unname(target_pairs[p]),
      candidate_pairs = cand,
      natural_positive_pair_ratio = if (cand > 0) pos / cand else NA_real_,
      stringsAsFactors = FALSE
    )
  }))

  if (!is.null(min_positive_pairs)) {
    need <- as.numeric(min_positive_pairs[parts])
    got <- report$positive_pairs[match(parts, report$partition)]
    if (any(got < need)) {
      stop(sprintf(
        paste0(
          "er_stratified_three_way_split: positive-pair quota infeasible under entity indivisibility. ",
          "Achieved %s; required at least %s. ",
          "Do NOT redraw the seed. Relax the quota, change the pre-registered fractions, or report the achieved counts."
        ),
        paste(sprintf("%s=%d", parts, got), collapse = ", "),
        paste(sprintf("%s=%d", parts, need), collapse = ", ")
      ))
    }
  }
  if (hard_targets) {
    # Hard targets are design targets, not a licence to redraw: report the
    # shortfall loudly but return the split so it can be discussed.
    short <- report$positive_pairs < report$target_positive_pairs
    if (any(short)) {
      warning(sprintf(
        "er_stratified_three_way_split: positive-pair targets not fully met (achieved %s vs target %s). This is reported, not repaired by redrawing.",
        paste(sprintf("%s=%d", report$partition, report$positive_pairs), collapse = ", "),
        paste(sprintf("%s=%d", report$partition, report$target_positive_pairs), collapse = ", ")
      ))
    }
  }

  list(
    assignment = tibble::tibble(
      id = as.character(id_vec),
      partition = assignment_partition
    ),
    indices = indices,
    report = report,
    seed = as.integer(seed),
    fractions = fractions,
    positive_pair_targets = if (hard_targets) target_pairs else NULL,
    id_vec = as.character(id_vec)
  )
}

#' Per-partition natural-prevalence report
#'
#' Summarises a split (from \code{\link{er_stratified_three_way_split}} or any
#' assignment with the same shape) using natural prevalence only. Augmented /
#' simulated Fit prevalence is reported by
#' \code{\link{er_augment_fit_pairs}} and must never be merged into this table.
#'
#' @param split A list returned by \code{er_stratified_three_way_split()}.
#' @return The split's per-partition report data.frame.
#' @export
er_positive_ratio_report <- function(split) {
  if (!is.list(split) || is.null(split$report)) {
    stop("er_positive_ratio_report: 'split' must be the list returned by er_stratified_three_way_split().")
  }
  split$report
}

#' Augment positive pairs inside the Fit partition only
#'
#' Resamples positive candidate pairs (both records in Fit, same gold cluster)
#' with replacement until the Fit positive-pair ratio reaches
#' \code{target_ratio}. Validation and Test pairs are never touched. The
#' returned pairs are explicitly marked as simulated prevalence; report them
#' separately from every natural-prevalence number.
#'
#' @param pairs Candidate pairs tibble with columns \code{idx1}, \code{idx2}
#'   (indices into the full record set).
#' @param truth_vec Integer gold cluster labels aligned to the full record
#'   set (length n). Used only to label Fit pairs; never a model feature.
#' @param fit_idx Integer indices of the Fit records (from the split).
#' @param target_ratio Target positive-pair ratio within Fit candidate pairs,
#'   in (0, 1).
#' @param seed Integer seed for resampling duplicates.
#'
#' @return A list with \code{pairs} (tibble: idx1, idx2, y, weight, copy_id,
#'   source_partition) and \code{report} (natural vs simulated Fit counts).
#' @export
er_augment_fit_pairs <- function(pairs,
                                 truth_vec,
                                 fit_idx,
                                 target_ratio = 0.10,
                                 seed = 42L) {
  if (!all(c("idx1", "idx2") %in% names(pairs))) {
    stop("er_augment_fit_pairs: 'pairs' must have columns idx1 and idx2.")
  }
  if (target_ratio <= 0 || target_ratio >= 1) {
    stop("er_augment_fit_pairs: 'target_ratio' must be in (0, 1).")
  }
  truth_vec <- as.integer(truth_vec)

  in_fit <- pairs$idx1 %in% fit_idx & pairs$idx2 %in% fit_idx
  fit_pairs <- pairs[in_fit, , drop = FALSE]
  if (!nrow(fit_pairs)) {
    stop(paste0(
      "er_augment_fit_pairs: no candidate pairs lie entirely inside the Fit partition. ",
      "Reason: blocking produced no within-Fit pairs, or fit_idx does not match the pair indices. ",
      "Next step: check the blocking step and the split indices before changing any prevalence target."
    ))
  }

  y <- as.integer(truth_vec[fit_pairs$idx1] == truth_vec[fit_pairs$idx2])
  n_pos <- sum(y == 1L)
  n_neg <- sum(y == 0L)
  if (n_pos == 0L) {
    stop(paste0(
      "er_augment_fit_pairs: the Fit partition contains zero positive candidate pairs, so there is nothing to resample. ",
      "Reason: the split design or the blocking step left Fit without a single gold pair. ",
      "Next step: revisit the pre-registered split quotas (er_stratified_three_way_split) -- do not manufacture positives in Validation/Test."
    ))
  }

  natural_ratio <- n_pos / (n_pos + n_neg)
  n_add <- 0L
  if (target_ratio > natural_ratio) {
    # Solve (P + x) / (P + N + x) = target for x. Subtract a tiny epsilon
    # before ceiling(): a mathematically exact solution (e.g. 2) can come
    # out as 2.0000000000000004 in floating point and ceiling would add one
    # duplicate too many (caught by tests on 2026-10-01: 0.833 vs 0.8).
    n_add <- as.integer(ceiling(
      (target_ratio * (n_pos + n_neg) - n_pos) / (1 - target_ratio) - 1e-9
    ))
    n_add <- max(n_add, 0L)
  }

  base <- tibble::tibble(
    idx1 = fit_pairs$idx1,
    idx2 = fit_pairs$idx2,
    y = y,
    weight = 1,
    copy_id = 0L,
    source_partition = "fit"
  )

  if (n_add > 0L) {
    pos_rows <- which(y == 1L)
    dup_local <- er_with_seed(seed, {
      sample(pos_rows, size = n_add, replace = TRUE)
    })
    dup <- tibble::tibble(
      idx1 = fit_pairs$idx1[dup_local],
      idx2 = fit_pairs$idx2[dup_local],
      y = 1L,
      weight = 1,
      copy_id = seq_len(n_add),
      source_partition = "fit"
    )
    out_pairs <- rbind(base, dup)
  } else {
    out_pairs <- base
  }

  simulated_ratio <- sum(out_pairs$y == 1L) / nrow(out_pairs)
  attr(out_pairs, "simulated_prevalence") <- TRUE
  attr(out_pairs, "natural_positive_pair_ratio") <- natural_ratio

  list(
    pairs = out_pairs,
    report = data.frame(
      partition = "fit",
      natural_positive_pairs = n_pos,
      negative_pairs = n_neg,
      natural_positive_pair_ratio = natural_ratio,
      duplicated_positive_pairs = n_add,
      simulated_positive_pairs = sum(out_pairs$y == 1L),
      simulated_positive_pair_ratio = simulated_ratio,
      target_ratio = target_ratio,
      simulated_prevalence = TRUE,
      stringsAsFactors = FALSE
    )
  )
}
