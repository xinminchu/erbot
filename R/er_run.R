########################################
# File: R/er_run.R
# Main entry point for the ERBOT v2 pipeline.
# "Data In, Entity Out" -- one call gets you results.
#
# Pipeline stages:
#  1. er_load()       -- ingest
#  2. er_diagnose()   -- field types, MV rates, recommendations
#  3. er_block()      -- candidate pairs
#  4. er_similarity() -- per-field similarities (NA-aware)
#  5. er_weights()    -- field weights
#  6. er_combine()    -- NA-aware weighted combination -> S
#  7. er_cluster_all()-- all clustering methods
#  8. er_merge()      -- consensus / transitivity post-processing
#  9. er_evaluate()   -- ARI, B-cubed, H/C/V (if truth given)
# 10. er_report()     -- CSV + console summary
########################################

#' Run the full ERBOT entity resolution pipeline
#'
#' The single-call interface to \pkg{erbot}.  Given any tabular dataset,
#' identifies which records refer to the same real-world entity.
#'
#' @param data A \code{data.frame}, file path, or benchmark keyword
#'   (\code{"cora"}, \code{"affiliation"}, \code{"d10k"}).
#' @param truth Optional ground truth: named integer vector, data.frame
#'   (id, cluster_id), or file path.  If provided, supervised metrics are
#'   computed and supervised weight learning is used.
#' @param id_col Character. ID column name.  Auto-detected if \code{NULL}.
#' @param text_cols Character vector.  Columns to use.  Auto-detected from
#'   \code{er_diagnose()} if \code{NULL}.
#' @param mode Character. \code{"auto"}, \code{"dedup"}, or \code{"link"}.
#'   Auto-detection checks for a \code{source_id} column.
#' @param block Character or list. Blocking method: \code{"auto"},
#'   \code{"none"}, \code{"standard"}, \code{"prefix"}, \code{"sn"}.
#'   Or a named list \code{list(method=..., key=..., prefix_len=...,
#'   sn_window=...)}.
#' @param similarity List of field specs, or \code{"auto"}.  If
#'   \code{"auto"}, spec is built from \code{er_diagnose()}.
#' @param weights Field weight specification.  One of \code{"auto"},
#'   \code{"equal"}, \code{"ari"}, \code{"fellegi_sunter"}, \code{"bimodal"},
#'   \code{"variance"}, or a named numeric vector.
#' @param cluster_methods Character vector of clustering methods, or
#'   \code{"all"}.  See \code{er_cluster_all()}.
#' @param k Integer. Cluster count for centroidal methods.  Tuned
#'   automatically if \code{NULL}.
#' @param k_grid Integer vector. k values swept during auto-tuning.
#' @param threshold Numeric. Similarity threshold for \code{threshold_cc} and
#'   graph coloring.
#' @param merge Character. Post-processing: \code{"transitivity"},
#'   \code{"consensus"}, \code{"best"}, or \code{"none"}.
#' @param consensus_alpha Numeric. Fraction for consensus merge.
#' @param eval_mode Character. \code{"labeled_only"} or
#'   \code{"singleton_fill"}.
#' @param out_dir Character. If set, write CSV and PDF report here.
#' @param verbose Logical. Print stage-level messages.
#'
#' @return A named list (class \code{"er_result"}) with elements:
#' \describe{
#'   \item{\code{clusters}}{Integer vector of final cluster labels (length n).}
#'   \item{\code{all_clusters}}{Named list of label vectors per method.}
#'   \item{\code{performance}}{tibble of evaluation metrics (NULL if no truth).}
#'   \item{\code{unsupervised}}{Named list of unsupervised quality scores.}
#'   \item{\code{predictions}}{tibble(id, cluster_id) for CSV output.}
#'   \item{\code{diag}}{Output of \code{er_diagnose()}.}
#'   \item{\code{blocking}}{List: pairs tibble and stats.}
#'   \item{\code{weights}}{Named numeric weight vector used.}
#'   \item{\code{params}}{List of all parameters used.}
#' }
#' @export
er_run <- function(
  data,
  truth            = NULL,
  id_col           = NULL,
  text_cols        = NULL,
  mode             = "auto",
  block            = "auto",
  similarity       = "auto",
  weights          = "auto",
  cluster_methods  = "all",
  k                = NULL,
  k_grid           = c(5L, 10L, 15L, 20L, 30L, 50L),
  threshold        = 0.5,
  merge            = "best",
  consensus_alpha  = 0.5,
  eval_mode        = "labeled_only",
  out_dir          = NULL,
  verbose          = TRUE
) {

  `%||%` <- get("%||%", asNamespace("erbot"))
  .msg <- function(...) if (verbose) message(...)
  t0 <- Sys.time()

  # ── Stage 1: Load ──────────────────────────────────────────────────────────
  .msg("[er_run] Stage 1: loading data...")
  df <- er_load(data)
  n  <- nrow(df)
  .msg(sprintf("  n = %d records, %d columns", n, ncol(df)))

  # ── Stage 2: Diagnose ─────────────────────────────────────────────────────
  .msg("[er_run] Stage 2: diagnosing fields...")
  diag <- er_diagnose(df, id_col = id_col, text_cols = text_cols)
  id_col_used <- diag$id_col
  id_vec      <- as.character(df[[id_col_used]])
  .msg(sprintf("  mode=%s, blocking_needed=%s, recommended=%s on '%s'",
               diag$mode, diag$blocking_needed,
               diag$recommended_block_method,
               diag$recommended_block_key %||% "none"))

  # ── Stage 3: Block ────────────────────────────────────────────────────────
  .msg("[er_run] Stage 3: blocking...")
  block_method <- if (is.list(block)) block$method %||% "auto" else block
  block_key    <- if (is.list(block)) block$key    %||% NULL   else NULL
  prefix_len   <- if (is.list(block)) block$prefix_len %||% 3L else 3L
  sn_window    <- if (is.list(block)) block$sn_window  %||% 40L else 40L

  pairs <- er_block(df, diag = diag, method = block_method,
                    block_key = block_key, prefix_len = prefix_len,
                    sn_window = sn_window)
  .msg(sprintf("  %s candidate pairs generated.",
               format(nrow(pairs), big.mark = ",")))

  block_stats <- er_block_stats(pairs, n,
                                truth = if (!is.null(truth)) truth else NULL,
                                id_vec = id_vec)

  # ── Stage 4: Similarity ───────────────────────────────────────────────────
  .msg("[er_run] Stage 4: computing per-field similarities...")
  spec_use <- if (identical(similarity, "auto")) NULL else similarity
  sim_list <- er_similarity(df, pairs, spec = spec_use, diag = diag)
  .msg(sprintf("  %d field(s) computed.", length(sim_list)))

  # ── Stage 5: Weights ──────────────────────────────────────────────────────
  .msg("[er_run] Stage 5: computing field weights...")
  truth_parsed <- if (!is.null(truth)) er_truth_from_any(truth) else NULL
  truth_vec_full <- if (!is.null(truth_parsed)) {
    tm <- setNames(as.integer(truth_parsed$cluster_id),
                   as.character(truth_parsed$id))
    tv <- tm[id_vec]
    tv
  } else NULL

  wt <- if (is.numeric(weights)) {
    weights
  } else {
    er_weights(sim_list, pairs = pairs, truth = truth_parsed,
               id_vec = id_vec, method = weights)
  }
  .msg(sprintf("  weights: %s",
               paste(sprintf("%s=%.3f", names(wt), wt), collapse = ", ")))

  # ── Stage 6: Combine ──────────────────────────────────────────────────────
  .msg("[er_run] Stage 6: combining similarities...")
  sim_combined <- er_combine(sim_list, weights = wt)
  S <- er_pairs_to_sparse(pairs, sim_combined, n)
  .msg(sprintf("  S: %d x %d sparse matrix, %d non-zero entries.",
               n, n, Matrix::nnzero(S) - n))

  # ── Stage 7: Cluster ──────────────────────────────────────────────────────
  .msg("[er_run] Stage 7: clustering...")
  all_clusters <- er_cluster_all(
    S,
    methods      = cluster_methods,
    k            = k,
    k_grid       = k_grid,
    threshold    = threshold,
    resolution   = 1,
    truth_vec    = truth_vec_full,
    verbose      = verbose
  )
  .msg(sprintf("  %d method(s) ran.", length(all_clusters)))

  # ── Stage 8: Merge ────────────────────────────────────────────────────────
  .msg("[er_run] Stage 8: merging / post-processing...")
  final_labels <- er_merge(
    all_clusters, S,
    method      = merge,
    threshold   = threshold,
    alpha       = consensus_alpha,
    truth_vec   = truth_vec_full
  )
  .msg(sprintf("  final: %d clusters.", length(unique(final_labels))))

  # ── Stage 9: Evaluate ─────────────────────────────────────────────────────
  perf_tbl <- NULL
  unsup    <- NULL
  if (!is.null(truth_parsed) && nrow(truth_parsed)) {
    .msg("[er_run] Stage 9a: evaluating (supervised)...")
    all_clusters_eval        <- all_clusters
    all_clusters_eval$final  <- final_labels
    perf_tbl <- tryCatch(
      er_evaluate(all_clusters_eval, truth = truth_parsed, id_vec = id_vec,
                  eval_mode = eval_mode),
      error = function(e) { message("  evaluation error: ", e$message); NULL }
    )
  } else {
    .msg("[er_run] Stage 9b: computing unsupervised quality scores...")
    unsup <- tryCatch(
      er_unsupervised_quality(final_labels, S),
      error = function(e) NULL
    )
  }

  # ── Stage 10: Output ──────────────────────────────────────────────────────
  predictions <- tibble::tibble(
    id         = id_vec,
    cluster_id = as.integer(final_labels)
  )

  if (!is.null(out_dir)) {
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    pred_path <- file.path(out_dir, "er_predictions.csv")
    readr::write_csv(predictions, pred_path)
    .msg(sprintf("  predictions written to %s", pred_path))
    if (!is.null(perf_tbl)) {
      perf_path <- file.path(out_dir, "er_performance.csv")
      readr::write_csv(perf_tbl, perf_path)
      .msg(sprintf("  performance table written to %s", perf_path))
    }
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  .msg(sprintf("[er_run] Done. Elapsed: %.1fs", elapsed))

  # ── Console summary ───────────────────────────────────────────────────────
  if (verbose) {
    cat(sprintf(
      "\n=== ERBOT result: n=%d, clusters=%d, elapsed=%.1fs ===\n",
      n, length(unique(final_labels)), elapsed
    ))
    cat(sprintf("  Blocking: %s pairs (RR=%.3f)\n",
                format(nrow(pairs), big.mark = ","),
                block_stats$reduction_ratio))
    if (!is.null(perf_tbl)) {
      cat("\n  Performance (primary metrics):\n")
      show_cols <- intersect(c("method", "ARI", "Bcubed_F", "Homogeneity",
                               "Completeness"), names(perf_tbl))
      print(perf_tbl[, show_cols, drop = FALSE], n = Inf)
    }
    if (!is.null(unsup)) {
      cat(sprintf("\n  Unsupervised quality: within=%.3f, between=%.3f, gap=%.3f, sil=%.3f\n",
                  unsup$within_sim %||% NA, unsup$between_sim %||% NA,
                  unsup$sim_gap %||% NA, unsup$silhouette %||% NA))
    }
  }

  result <- list(
    clusters      = final_labels,
    all_clusters  = all_clusters,
    performance   = perf_tbl,
    unsupervised  = unsup,
    predictions   = predictions,
    diag          = diag,
    blocking      = list(pairs = pairs, stats = block_stats),
    weights       = wt,
    params        = list(
      mode             = mode,
      block            = block,
      weights          = weights,
      cluster_methods  = cluster_methods,
      k                = k,
      k_grid           = k_grid,
      threshold        = threshold,
      merge            = merge,
      consensus_alpha  = consensus_alpha,
      eval_mode        = eval_mode
    )
  )
  class(result) <- c("er_result", "list")
  invisible(result)
}

# ── print method ──────────────────────────────────────────────────────────────

#' Print an \code{er_result} object
#'
#' Displays a compact summary: record count, final cluster count, performance
#' table (if truth was supplied), and unsupervised quality scores (otherwise).
#'
#' @param x An \code{er_result} list returned by \code{\link{er_run}()}.
#' @param ... Currently unused; for S3 method consistency.
#' @return \code{x} invisibly.
#' @export
print.er_result <- function(x, ...) {
  n  <- nrow(x$predictions)
  nk <- length(unique(x$clusters))
  cat(sprintf("er_result: n=%d records, %d final clusters\n", n, nk))
  if (!is.null(x$performance)) {
    cat("Performance:\n")
    show_cols <- intersect(c("method","ARI","Bcubed_F","Vmeasure"),
                           names(x$performance))
    print(x$performance[, show_cols, drop = FALSE], n = Inf)
  }
  if (!is.null(x$unsupervised)) {
    cat(sprintf("Unsupervised: gap=%.3f, sil=%.3f\n",
                x$unsupervised$sim_gap %||% NA,
                x$unsupervised$silhouette %||% NA))
  }
  invisible(x)
}
