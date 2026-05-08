########################################
# File: R/11-ablation.R
#
# Ablation study runner for ERBOT.
#
# Implements the required comparison matrix:
#
#   Weight conditions:
#     w_uniform   -- er_weights(method = "equal")
#     w_fs_em     -- er_weights(method = "fellegi_sunter")    [unsupervised]
#     w_full_ari  -- er_weights(method = "ari"), full-data truth used for
#                    both learning AND evaluation  [optimistic, for reference]
#     w_cv_ari    -- ARI weights learned on 80% train, evaluated on 20% test
#                    [the honest CV condition]
#
#   Blocking conditions:
#     no_block    -- er_block(method = "none")
#     block       -- er_block(method = block_spec)
#
#   Full ablation matrix: 2 blocking × 4 weight = 8 single-run conditions
#   plus 2 CV conditions (no_block+cv, block+cv).
#
# Public API
# ----------
#   er_cv()             -- generic K-fold CV runner (block + weight params)
#   er_ablation_table() -- non-CV ablation conditions (8 single-run rows)
#   er_ablation_all()   -- full ablation + CV + baselines, one unified table
########################################

# ── er_cv() ───────────────────────────────────────────────────────────────────

#' Generic K-fold cross-validation for the ERBOT pipeline
#'
#' Partitions records into K random folds, learns field weights on each
#' 80\% train fold, and evaluates on the held-out 20\% test fold.
#' This is the honest out-of-sample evaluation protocol used in the paper.
#'
#' Weight learning uses \code{method = weights} on the training fold only;
#' the resulting \code{learned_weights} vector is passed directly to
#' \code{er_run()} for the test fold (no further fitting on test data).
#'
#' @param data data.frame of records.
#' @param truth Ground truth (required for ARI weight learning and evaluation).
#' @param id_col Character. ID column. Auto-detected if NULL.
#' @param K Integer. Number of folds (default 5).
#' @param seed Integer. Random seed for fold assignment (default 42).
#' @param block Blocking specification: \code{"none"}, \code{"auto"}, a
#'   keyword string, or a named list with \code{method}, \code{key},
#'   \code{prefix_len}, etc.
#' @param weights Character. Weight learning method for the train fold.
#'   One of \code{"ari"} (default), \code{"equal"}, \code{"fellegi_sunter"}.
#' @param cluster_methods Character vector of clustering methods.
#' @param merge Character. Post-processing merge strategy (default \code{"best"}).
#' @param out_dir Character. If set, write per-fold CSVs here.
#' @param verbose Logical.
#' @return A named list:
#'   \item{\code{cv_summary}}{data.frame: mean+SD per method across test folds.}
#'   \item{\code{all_folds}}{data.frame: per-fold per-method test metrics.}
#'   \item{\code{fold_weights}}{List of learned weight vectors per fold.}
#'   \item{\code{n_folds_ok}}{Integer: number of folds that completed.}
#' @export
er_cv <- function(data, truth, id_col = NULL,
                  K = 5L, seed = 42L,
                  block = "auto",
                  weights = "ari",
                  cluster_methods = c("hclust_avg", "hclust_ward", "gc",
                                      "leiden", "louvain", "label_prop",
                                      "threshold_cc"),
                  merge = "best",
                  out_dir = NULL,
                  verbose = TRUE) {

  `%||%` <- get("%||%", asNamespace("erbot"))
  ts <- function(...) if (verbose) message(sprintf("[%s] %s",
    format(Sys.time(), "%H:%M:%S"), paste0(...)))

  # ── Load & fold assignment ──────────────────────────────────────────────────
  df          <- er_load(data)
  n           <- nrow(df)
  diag        <- er_diagnose(df, id_col = id_col)
  id_col_used <- diag$id_col
  id_vec      <- as.character(df[[id_col_used]])

  truth_parsed <- er_truth_from_any(truth)

  .filter_gold <- function(gp, id_subset) {
    gp[gp$id %in% id_subset, ]
  }

  set.seed(seed)
  fold_idx <- sample(rep(seq_len(K), length.out = n))
  ts(sprintf("Records assigned to %d folds (%s).",
             K, paste(tabulate(fold_idx), collapse = "/")))

  if (!is.null(out_dir) && !dir.exists(out_dir))
    dir.create(out_dir, recursive = TRUE)

  fold_results  <- vector("list", K)
  fold_weights  <- vector("list", K)

  for (fold in seq_len(K)) {
    ts(sprintf("── Fold %d / %d ──", fold, K))

    is_test   <- fold_idx == fold
    train_df  <- df[!is_test, , drop = FALSE]
    test_df   <- df[ is_test, , drop = FALSE]
    train_ids <- id_vec[!is_test]
    test_ids  <- id_vec[ is_test]

    train_truth <- truth_parsed[truth_parsed$id %in% train_ids, ]
    test_truth  <- truth_parsed[truth_parsed$id %in% test_ids,  ]

    ts(sprintf("  Train: %d records, %d gold clusters | Test: %d records, %d gold clusters",
               nrow(train_df), length(unique(train_truth$cluster_id)),
               nrow(test_df),  length(unique(test_truth$cluster_id))))

    # ── TRAIN ─────────────────────────────────────────────────────────────────
    ts("  [TRAIN] Running er_run...")
    res_train <- tryCatch(
      er_run(train_df, truth = train_truth, id_col = id_col_used,
             block = block, weights = weights,
             cluster_methods = cluster_methods, merge = merge,
             verbose = FALSE),
      error = function(e) {
        ts(sprintf("  TRAIN ERROR fold %d: %s", fold, e$message))
        NULL
      }
    )
    if (is.null(res_train)) next

    learned_w <- res_train$weights
    fold_weights[[fold]] <- learned_w
    ts(sprintf("  Learned weights: %s",
               paste(sprintf("%s=%.3f", names(learned_w), learned_w),
                     collapse = ", ")))

    if (!is.null(out_dir) && !is.null(res_train$performance)) {
      fold_dir <- file.path(out_dir, sprintf("fold_%d", fold))
      if (!dir.exists(fold_dir)) dir.create(fold_dir)
      readr::write_csv(res_train$performance,
                       file.path(fold_dir, "train_performance.csv"))
    }

    # ── TEST ──────────────────────────────────────────────────────────────────
    ts("  [TEST] Running er_run...")
    res_test <- tryCatch(
      er_run(test_df, truth = test_truth, id_col = id_col_used,
             block = block, weights = learned_w,    # use train weights
             cluster_methods = cluster_methods, merge = merge,
             verbose = FALSE),
      error = function(e) {
        ts(sprintf("  TEST ERROR fold %d: %s", fold, e$message))
        NULL
      }
    )
    if (is.null(res_test)) next

    if (!is.null(out_dir) && !is.null(res_test$performance)) {
      fold_dir <- file.path(out_dir, sprintf("fold_%d", fold))
      if (!dir.exists(fold_dir)) dir.create(fold_dir)
      readr::write_csv(res_test$performance,
                       file.path(fold_dir, "test_performance.csv"))
      readr::write_csv(res_test$predictions,
                       file.path(fold_dir, "test_predictions.csv"))
    }

    train_perf <- if (!is.null(res_train$performance))
      cbind(fold = fold, split = "train", res_train$performance) else NULL
    test_perf  <- if (!is.null(res_test$performance))
      cbind(fold = fold, split = "test",  res_test$performance)  else NULL

    fold_results[[fold]] <- list(
      fold         = fold,
      n_train      = nrow(train_df),
      n_test       = nrow(test_df),
      weights      = learned_w,
      train_perf   = train_perf,
      test_perf    = test_perf
    )
    ts(sprintf("  Fold %d done.", fold))

    # Release memory
    rm(res_train, res_test, train_df, test_df, train_truth, test_truth)
    gc(verbose = FALSE)
  }

  # ── Aggregate ───────────────────────────────────────────────────────────────
  valid <- Filter(Negate(is.null), fold_results)
  if (!length(valid)) {
    warning("er_cv: all folds failed.")
    return(list(cv_summary = NULL, all_folds = NULL,
                fold_weights = fold_weights, n_folds_ok = 0L))
  }

  all_test <- do.call(rbind, lapply(valid, `[[`, "test_perf"))
  all_folds <- do.call(rbind, lapply(valid, function(x)
    rbind(x$train_perf, x$test_perf)))

  PRIMARY <- c("ARI", "Bcubed_F", "Vmeasure", "Homogeneity", "Completeness")
  avail    <- intersect(PRIMARY, names(all_test))
  methods  <- unique(all_test$method)

  cv_summary <- do.call(rbind, lapply(methods, function(m) {
    sub  <- all_test[all_test$method == m, avail, drop = FALSE]
    sub  <- apply(sub, 2L, as.numeric)
    means <- colMeans(sub, na.rm = TRUE)
    sds   <- apply(sub, 2L, sd, na.rm = TRUE)
    row   <- data.frame(method = m, n_folds = nrow(sub),
                        stringsAsFactors = FALSE)
    for (met in avail) {
      row[[paste0(met, "_mean")]] <- round(means[met], 4L)
      row[[paste0(met, "_sd")]]   <- round(sds[met],   4L)
    }
    row
  }))
  cv_summary <- cv_summary[order(-cv_summary$ARI_mean), ]

  if (!is.null(out_dir)) {
    readr::write_csv(all_folds,  file.path(out_dir, "cv_all_folds.csv"))
    readr::write_csv(cv_summary, file.path(out_dir, "cv_summary.csv"))
  }

  list(
    cv_summary   = cv_summary,
    all_folds    = all_folds,
    fold_weights = fold_weights,
    n_folds_ok   = length(valid)
  )
}

# ── er_ablation_table() ───────────────────────────────────────────────────────

#' Run ablation conditions (single-run, not CV)
#'
#' Evaluates the ERBOT pipeline under all combinations of weight strategy
#' (\code{uniform}, \code{fs_em}, \code{full_ari}) and blocking
#' (\code{no_block}, \code{block}).  \strong{Note}: the \code{full_ari}
#' condition uses truth for both weight learning and evaluation and is
#' therefore an optimistic upper bound — it is included for reference only.
#'
#' The CV conditions (\code{w=CV-learned}) are handled by \code{\link{er_cv}}.
#'
#' @param data data.frame of records.
#' @param truth Optional ground truth (required for \code{full_ari} condition).
#' @param id_col Character. ID column.
#' @param block_spec Blocking specification for the \code{blocking} conditions.
#' @param cluster_methods Character vector.
#' @param merge Character. Post-processing strategy.
#' @param conditions Named list of conditions to run.  Default: all 6.
#' @param verbose Logical.
#' @return data.frame with one row per (condition × method) combination and
#'   columns \code{condition, weight_method, blocking, method, ARI, Bcubed_F,
#'   Vmeasure, Homogeneity, Completeness}.
#' @export
er_ablation_table <- function(data, truth = NULL, id_col = NULL,
                               block_spec = "auto",
                               cluster_methods = c("hclust_avg", "hclust_ward",
                                                   "gc", "leiden", "louvain",
                                                   "label_prop", "threshold_cc"),
                               merge = "best",
                               conditions = NULL,
                               verbose = TRUE) {

  ts <- function(lbl, ...) if (verbose) message(sprintf("[ablation|%s] %s", lbl, paste0(...)))

  # Default condition matrix
  default_conditions <- list(
    list(label = "no_block+uniform",  block = "none",      weights = "equal"),
    list(label = "no_block+fs_em",    block = "none",      weights = "fellegi_sunter"),
    list(label = "no_block+full_ari", block = "none",      weights = "ari"),
    list(label = "block+uniform",     block = block_spec,  weights = "equal"),
    list(label = "block+fs_em",       block = block_spec,  weights = "fellegi_sunter"),
    list(label = "block+full_ari",    block = block_spec,  weights = "ari")
  )
  if (!is.null(conditions)) default_conditions <- conditions

  # Drop full_ari conditions if no truth provided
  if (is.null(truth)) {
    default_conditions <- Filter(
      function(c) !grepl("full_ari", c$label), default_conditions)
    if (verbose) message("[ablation] No truth provided: skipping full_ari conditions.")
  }

  rows <- list()
  for (cond in default_conditions) {
    ts(cond$label, sprintf("w=%s, block=%s",
                            cond$weights,
                            if (is.list(cond$block)) cond$block$method
                            else cond$block))
    res <- tryCatch(
      er_run(data, truth = truth, id_col = id_col,
             block = cond$block, weights = cond$weights,
             cluster_methods = cluster_methods, merge = merge,
             verbose = FALSE),
      error = function(e) {
        warning(sprintf("[ablation] condition '%s' failed: %s",
                        cond$label, e$message))
        NULL
      }
    )
    if (is.null(res) || is.null(res$performance)) next

    perf <- res$performance
    perf$condition     <- cond$label
    perf$weight_method <- cond$weights
    perf$blocking      <- if (is.list(cond$block))
      cond$block$method else cond$block
    rows[[cond$label]] <- perf
  }

  if (!length(rows)) {
    warning("[ablation] No conditions completed successfully.")
    return(data.frame())
  }

  out <- do.call(rbind, rows)
  # Reorder columns: condition meta first
  meta_cols  <- c("condition", "weight_method", "blocking", "method")
  metric_cols <- intersect(c("ARI","Bcubed_F","Vmeasure","Homogeneity",
                              "Completeness","NMI","VI","PairF_F"),
                            names(out))
  out[, c(meta_cols, metric_cols), drop = FALSE]
}

# ── er_ablation_all() ─────────────────────────────────────────────────────────

#' Full ablation study: single-run conditions + CV + baselines
#'
#' Combines:
#' \enumerate{
#'   \item \strong{Ablation table} (6 single-run conditions from
#'         \code{\link{er_ablation_table}}).
#'   \item \strong{CV conditions}: honest 5-fold CV with
#'         blocking+cv_ari and no_block+cv_ari (via \code{\link{er_cv}}).
#'   \item \strong{External baselines}: Fellegi–Sunter EM, Splink,
#'         DeepMatcher, Ditto (via \code{\link{er_run_baselines}}).
#' }
#'
#' Any component that fails (Python not installed, timeout, etc.) is
#' represented as NAs in the output table with a warning.
#'
#' @param data data.frame of records.
#' @param truth Optional ground truth.
#' @param id_col Character. ID column.
#' @param block_spec Blocking spec for the blocking conditions.
#' @param cluster_methods Character vector.
#' @param K Integer. Number of CV folds (default 5).
#' @param seed Integer. CV fold seed (default 42).
#' @param run_cv Logical. Whether to run the (expensive) CV conditions.
#' @param run_baselines Logical. Whether to run external Python baselines.
#' @param baselines Character vector. Which baselines to run.
#' @param cv_out_dir Character. Optional directory for CV fold CSV output.
#' @param out_dir Character. Optional directory for full ablation CSV output.
#' @param verbose Logical.
#' @return A named list:
#'   \item{\code{ablation_table}}{Single-run condition results.}
#'   \item{\code{cv_block}}{CV results with blocking.}
#'   \item{\code{cv_no_block}}{CV results without blocking.}
#'   \item{\code{baselines}}{External baseline results.}
#'   \item{\code{combined}}{Single flat data.frame merging all above.}
#' @export
er_ablation_all <- function(data, truth = NULL, id_col = NULL,
                             block_spec = "auto",
                             cluster_methods = c("hclust_avg", "hclust_ward",
                                                 "gc", "leiden", "louvain",
                                                 "label_prop", "threshold_cc"),
                             K = 5L, seed = 42L,
                             run_cv        = TRUE,
                             run_baselines = TRUE,
                             baselines     = c("fs_em", "splink",
                                               "deepmatcher", "ditto"),
                             cv_out_dir    = NULL,
                             out_dir       = NULL,
                             conditions    = NULL,
                             verbose       = TRUE) {

  ts <- function(...) if (verbose) message(sprintf("[%s] %s",
    format(Sys.time(), "%H:%M:%S"), paste0(...)))

  results <- list(ablation_table = NULL, cv_block = NULL,
                  cv_no_block = NULL, baselines = NULL, combined = NULL)

  # ── Part 1: Ablation table (single-run) ─────────────────────────────────────
  ts("Part 1: single-run ablation conditions...")
  results$ablation_table <- tryCatch(
    er_ablation_table(data, truth = truth, id_col = id_col,
                      block_spec = block_spec,
                      cluster_methods = cluster_methods,
                      conditions = conditions,
                      verbose = verbose),
    error = function(e) { warning("Ablation table failed: ", e$message); NULL }
  )

  # ── Part 2: CV conditions ────────────────────────────────────────────────────
  if (run_cv && !is.null(truth)) {
    # CV with blocking
    ts(sprintf("Part 2a: CV%d with blocking...", K))
    cv_block_dir <- if (!is.null(cv_out_dir)) file.path(cv_out_dir, "cv_block") else NULL
    results$cv_block <- tryCatch(
      er_cv(data, truth, id_col = id_col, K = K, seed = seed,
             block = block_spec, weights = "ari",
             cluster_methods = cluster_methods,
             out_dir = cv_block_dir, verbose = verbose),
      error = function(e) { warning("CV (blocking) failed: ", e$message); NULL }
    )

    # CV without blocking
    ts(sprintf("Part 2b: CV%d without blocking...", K))
    cv_noblock_dir <- if (!is.null(cv_out_dir)) file.path(cv_out_dir, "cv_no_block") else NULL
    results$cv_no_block <- tryCatch(
      er_cv(data, truth, id_col = id_col, K = K, seed = seed,
             block = "none", weights = "ari",
             cluster_methods = cluster_methods,
             out_dir = cv_noblock_dir, verbose = verbose),
      error = function(e) { warning("CV (no-block) failed: ", e$message); NULL }
    )
  } else if (run_cv && is.null(truth)) {
    message("[ablation] Skipping CV conditions: truth required for ARI weight learning.")
  }

  # ── Part 3: External baselines ───────────────────────────────────────────────
  if (run_baselines) {
    ts("Part 3: external baselines...")
    results$baselines <- tryCatch(
      er_run_baselines(data, truth = truth, id_col = id_col,
                        block = block_spec, baselines = baselines),
      error = function(e) { warning("Baselines failed: ", e$message); NULL }
    )
  }

  # ── Part 4: Combine into one flat table ──────────────────────────────────────
  ts("Part 4: assembling combined comparison table...")
  rows <- list()

  # Single-run ablation rows
  if (!is.null(results$ablation_table) && nrow(results$ablation_table)) {
    # Summarise to best method per condition (highest ARI)
    abl <- results$ablation_table
    abl_best <- do.call(rbind, lapply(unique(abl$condition), function(cond) {
      sub <- abl[abl$condition == cond, ]
      sub[which.max(sub$ARI), , drop = FALSE]
    }))
    abl_best$source <- "ablation_single_run"
    rows[["ablation"]] <- abl_best[, c("condition", "source", "method",
                                        "ARI", "Bcubed_F", "Vmeasure"), drop = FALSE]
  }

  # CV summary rows
  .cv_to_rows <- function(cv_res, label_prefix) {
    if (is.null(cv_res) || is.null(cv_res$cv_summary)) return(NULL)
    s <- cv_res$cv_summary
    out <- data.frame(
      condition = paste0(label_prefix, "+", s$method),
      source    = "cv5",
      method    = s$method,
      ARI       = s$ARI_mean,
      Bcubed_F  = s$Bcubed_F_mean,
      Vmeasure  = s$Vmeasure_mean,
      stringsAsFactors = FALSE
    )
    out
  }
  cv_block_rows   <- .cv_to_rows(results$cv_block,    "block+cv_ari")
  cv_noblock_rows <- .cv_to_rows(results$cv_no_block, "no_block+cv_ari")
  if (!is.null(cv_block_rows))   rows[["cv_block"]]   <- cv_block_rows
  if (!is.null(cv_noblock_rows)) rows[["cv_noblock"]] <- cv_noblock_rows

  # Baseline rows
  if (!is.null(results$baselines) && nrow(results$baselines)) {
    bl_rows <- data.frame(
      condition = results$baselines$method,
      source    = "baseline",
      method    = results$baselines$method,
      ARI       = results$baselines$ARI,
      Bcubed_F  = results$baselines$Bcubed_F,
      Vmeasure  = results$baselines$Vmeasure,
      stringsAsFactors = FALSE
    )
    rows[["baselines"]] <- bl_rows
  }

  if (length(rows)) {
    combined <- do.call(rbind, rows)
    combined <- combined[order(-combined$ARI, na.last = TRUE), ]
    results$combined <- combined

    if (!is.null(out_dir)) {
      if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
      readr::write_csv(combined, file.path(out_dir, "ablation_combined.csv"))
      if (!is.null(results$ablation_table))
        readr::write_csv(results$ablation_table,
                         file.path(out_dir, "ablation_table.csv"))
      ts(sprintf("Saved to %s/ablation_*.csv", out_dir))
    }
  }

  ts("Done.")
  results
}

# ── Printing helper ───────────────────────────────────────────────────────────

#' Print an ablation result in a compact table
#'
#' @param x Output of \code{\link{er_ablation_all}}.
#' @param ... Unused.
#' @return \code{x} invisibly.
#' @export
print_ablation <- function(x, ...) {
  cat("\n=== ERBOT Ablation Study ===\n\n")

  if (!is.null(x$combined)) {
    cat("── Combined comparison (best method per condition, sorted by ARI) ──\n")
    cols <- intersect(c("condition", "source", "method", "ARI",
                        "Bcubed_F", "Vmeasure"), names(x$combined))
    print(x$combined[, cols, drop = FALSE], row.names = FALSE, digits = 4L)
    cat("\n")
  }

  if (!is.null(x$ablation_table) && nrow(x$ablation_table)) {
    cat("── Ablation table (all methods × conditions) ──\n")
    cols <- intersect(c("condition","method","ARI","Bcubed_F","Vmeasure"),
                      names(x$ablation_table))
    print(x$ablation_table[, cols, drop = FALSE], row.names = FALSE, digits = 4L)
    cat("\n")
  }

  if (!is.null(x$cv_block) && !is.null(x$cv_block$cv_summary)) {
    cat(sprintf("── CV%d (with blocking) ──\n", x$cv_block$n_folds_ok + 0L))
    cols <- intersect(c("method","ARI_mean","ARI_sd","Bcubed_F_mean","Vmeasure_mean"),
                      names(x$cv_block$cv_summary))
    print(x$cv_block$cv_summary[, cols, drop = FALSE], row.names = FALSE, digits = 4L)
    cat("\n")
  }

  if (!is.null(x$baselines) && nrow(x$baselines)) {
    cat("── External baselines ──\n")
    print(x$baselines, row.names = FALSE, digits = 4L)
    cat("\n")
  }

  invisible(x)
}
