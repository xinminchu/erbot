########################################
# File: R/run_datasets.R
# High-level dataset runners.
# Each function is a thin, opinionated wrapper over er_run() that sets
# sensible defaults for a specific benchmark dataset.
#
# Functions:
#   run_cora()               - CORA bibliographic deduplication
#   run_affiliation()        - Affiliation-string deduplication
#   run_erbot_dataset()      - General runner for any supported dataset
#   run_erbot_tuned_dataset()- Same but with er_tune() hyperparameter search
#   run_stable_er()          - Stability-assessed ER via bootstrap
#   run_with_weights()       - er_run() with a user-supplied weight vector
#   er_main()                - Alias for er_run() (back-compat)
#   er_main_simple()         - Minimal er_run() alias (back-compat)
#   er_unified_pipeline()    - Alias for er_run() (back-compat)
#   er_general_pipeline()    - Alias for er_run() (back-compat)
########################################

# ── CORA ──────────────────────────────────────────────────────────────────────

#' Run ERBOT on the CORA bibliographic deduplication benchmark
#'
#' Loads the CORA dataset from the \pkg{cora} package (1,879 records) and
#' runs the full 9-stage ERBOT pipeline with CORA-appropriate defaults:
#' \itemize{
#'   \item Fields: \code{title}, \code{authors}, \code{address},
#'         \code{booktitle}, \code{journal}, \code{year}
#'   \item Blocking: \code{"none"} (n is small enough for full comparison)
#'   \item Truth: \code{cora::cora_gold} (used automatically if available)
#' }
#'
#' @param fields Character vector of field names to use.  Defaults to the six
#'   most informative CORA fields.
#' @param block Blocking method.  Default \code{"none"} (n = 1,879 is
#'   manageable without blocking).
#' @param truth Ground truth.  Defaults to \code{cora::cora_gold}.  Pass
#'   \code{NULL} to run unsupervised.
#' @param out_dir Optional output directory for CSV / PDF reports.
#' @param ... Additional arguments forwarded to \code{\link{er_run}()}.
#'
#' @return An \code{er_result} object.
#' @export
run_cora <- function(
    fields  = c("title", "authors", "address", "booktitle", "journal", "year"),
    block   = "none",
    truth   = NULL,
    out_dir = NULL,
    ...
) {
  if (!requireNamespace("cora", quietly = TRUE))
    stop("Package 'cora' is required. Install with: install.packages('cora')")

  # Load ground truth from cora package if not supplied
  if (is.null(truth)) {
    truth <- tryCatch(
      get("cora_gold", envir = asNamespace("cora")),
      error = function(e) NULL
    )
    if (is.null(truth))
      message("run_cora: cora_gold not found in cora package; running unsupervised.")
  }

  er_run(
    data            = "cora",
    truth           = truth,
    text_cols       = fields,
    block           = block,
    out_dir         = out_dir,
    ...
  )
}


# ── Affiliation ───────────────────────────────────────────────────────────────

#' Run ERBOT on the affiliation-string deduplication benchmark
#'
#' Loads \code{affiliationstrings_ids.csv} (2,260 raw affiliation strings) and
#' runs the pipeline with affiliation-appropriate defaults:
#' \itemize{
#'   \item Field: \code{affiliation} (single free-text field)
#'   \item Blocking: \code{"sn"} with window 10 (strings are short; alphabetical
#'     sort works well)
#'   \item Truth: \code{affiliationstrings_mapping.csv} (32,815 match pairs)
#' }
#'
#' @param data_path Path to the affiliation IDs CSV.  Defaults to the bundled
#'   \code{data/affiliationstrings_ids.csv}.
#' @param truth_path Path to the affiliation mapping CSV (id1 / id2 pairs).
#'   Defaults to the bundled \code{data/affiliationstrings_mapping.csv}.
#' @param block Blocking specification.  Default: SN with window 10.
#' @param out_dir Optional output directory.
#' @param ... Additional arguments forwarded to \code{\link{er_run}()}.
#'
#' @return An \code{er_result} object.
#' @export
run_affiliation <- function(
    data_path  = NULL,
    truth_path = NULL,
    block      = list(method = "sn", key = "affiliation", sn_window = 10L),
    out_dir    = NULL,
    ...
) {
  # Locate data file
  if (is.null(data_path)) {
    here <- .pkg_root()
    candidates <- c(
      file.path(here, "data", "affiliationstrings_ids.csv"),
      system.file("extdata", "affiliation.csv", package = "erbot",
                  mustWork = FALSE)
    )
    data_path <- candidates[file.exists(candidates)][1]
    if (is.na(data_path))
      stop("Affiliation data not found. Supply 'data_path' explicitly.")
  }

  # Locate truth file
  if (is.null(truth_path)) {
    here <- .pkg_root()
    tp <- file.path(here, "data", "affiliationstrings_mapping.csv")
    truth_path <- if (file.exists(tp)) tp else NULL
    if (is.null(truth_path))
      message("run_affiliation: truth file not found; running unsupervised.")
  }

  er_run(
    data      = data_path,
    truth     = truth_path,
    text_cols = "affiliation",
    block     = block,
    out_dir   = out_dir,
    ...
  )
}


# ── General dataset runner ────────────────────────────────────────────────────

#' Run ERBOT on a supported benchmark dataset
#'
#' A unified entry point for the four benchmark datasets.
#'
#' @param dataset Character.  One of \code{"cora"}, \code{"affiliation"},
#'   \code{"d10k"}, or \code{"ncvr"}.
#' @param ncvr_root Directory containing NC voter CSV partitions (required when
#'   \code{dataset = "ncvr"}).
#' @param ncvr_which Partition set for NCVR: \code{"5"}, \code{"10"}, or
#'   \code{"all"}.  Default \code{"5"}.
#' @param truth Optional ground truth override.  When \code{NULL}, the function
#'   tries to locate the bundled truth file for the dataset.
#' @param out_dir Optional output directory.
#' @param ... Additional arguments forwarded to \code{\link{er_run}()}.
#'
#' @return An \code{er_result} object.
#' @export
run_erbot_dataset <- function(
    dataset    = c("cora", "affiliation", "d10k", "ncvr"),
    ncvr_root  = NULL,
    ncvr_which = "5",
    truth      = NULL,
    out_dir    = NULL,
    ...
) {
  `%||%` <- get("%||%", asNamespace("erbot"))
  dataset <- match.arg(dataset)

  switch(dataset,

    "cora" = run_cora(truth = truth, out_dir = out_dir, ...),

    "affiliation" = run_affiliation(truth_path = truth, out_dir = out_dir, ...),

    "d10k" = {
      here <- .pkg_root()
      # Resolve truth: 10Kduplicates.csv is pipe-delimited with Entity1|Entity2
      # columns; pre-read it so er_truth_from_any receives a proper id1/id2 df.
      truth_obj <- if (!is.null(truth)) {
        truth
      } else {
        tp <- file.path(here, "data", "10Kduplicates.csv")
        if (file.exists(tp)) {
          pairs_df <- data.table::fread(tp, showProgress = FALSE)
          names(pairs_df) <- tolower(names(pairs_df))
          nms <- names(pairs_df)
          if (length(nms) >= 2L && !all(c("id1", "id2") %in% nms))
            names(pairs_df)[1:2] <- c("id1", "id2")
          tibble::as_tibble(pairs_df)
        } else {
          message("run_erbot_dataset (d10k): truth file not found; running unsupervised.")
          NULL
        }
      }
      er_run(
        data      = "d10k",
        truth     = truth_obj,
        text_cols = "text",
        block     = list(method = "sn", key = "text", sn_window = 40L),
        out_dir   = out_dir,
        ...
      )
    },

    "ncvr" = {
      if (is.null(ncvr_root)) {
        here <- .pkg_root()
        ncvr_root <- file.path(here, "data")
      }
      df <- ncvr_read(ncvr_root, which = ncvr_which)
      fields <- ncvr_guess_fields(df)
      if (!length(fields))
        stop("Could not detect any ER fields in the NCVR data. ",
             "Check column names: ", paste(names(df), collapse = ", "))
      message("run_erbot_dataset (ncvr): using fields: ",
              paste(fields, collapse = ", "))
      er_run(
        data      = df,
        truth     = truth,
        id_col    = "recid",
        text_cols = fields,
        block     = list(method = "sn", key = fields[1], sn_window = 40L),
        out_dir   = out_dir,
        ...
      )
    }
  )
}


# ── Tuned dataset runner ──────────────────────────────────────────────────────

#' Run ERBOT with hyperparameter tuning on a benchmark dataset
#'
#' Runs \code{\link{run_erbot_dataset}()} under a grid of key hyperparameters
#' (blocking window, threshold, clustering method) and returns the best result
#' by ARI (if truth is available) or silhouette (otherwise).
#'
#' @param dataset Character.  One of \code{"cora"}, \code{"affiliation"},
#'   \code{"d10k"}, or \code{"ncvr"}.
#' @param threshold_grid Numeric vector of similarity thresholds to try.
#' @param cluster_methods Character vector of clustering methods to try.
#' @param out_dir Optional output directory for the best result's reports.
#' @param verbose Logical.  Print tuning progress.
#' @param ... Additional arguments forwarded to \code{\link{run_erbot_dataset}()}.
#'
#' @return The \code{er_result} with the highest ARI (or silhouette).
#' @export
run_erbot_tuned_dataset <- function(
    dataset         = c("cora", "affiliation", "d10k", "ncvr"),
    threshold_grid  = c(0.3, 0.4, 0.5, 0.6, 0.7),
    cluster_methods = c("louvain", "leiden", "threshold_cc"),
    out_dir         = NULL,
    verbose         = TRUE,
    ...
) {
  dataset <- match.arg(dataset)
  `%||%` <- get("%||%", asNamespace("erbot"))

  best_result <- NULL
  best_score  <- -Inf

  total <- length(threshold_grid) * length(cluster_methods)
  run_n <- 0L

  for (thr in threshold_grid) {
    for (meth in cluster_methods) {
      run_n <- run_n + 1L
      if (verbose)
        message(sprintf("[tune %d/%d] dataset=%s  threshold=%.2f  method=%s",
                        run_n, total, dataset, thr, meth))
      res <- tryCatch(
        run_erbot_dataset(
          dataset         = dataset,
          threshold       = thr,
          cluster_methods = meth,
          merge           = "none",
          verbose         = FALSE,
          ...
        ),
        error = function(e) {
          message("  skipped (error): ", e$message); NULL
        }
      )
      if (is.null(res)) next

      # Score: ARI of the final clustering (or silhouette if unsupervised)
      score <- if (!is.null(res$performance)) {
        final_row <- res$performance[res$performance$method == "final", , drop = FALSE]
        if (nrow(final_row) && "ARI" %in% names(final_row))
          final_row$ARI[1]
        else
          max(res$performance$ARI, na.rm = TRUE)
      } else {
        res$unsupervised$sim_gap %||% -Inf
      }
      score <- if (is.finite(score)) score else -Inf

      if (verbose) message(sprintf("  score=%.4f", score))

      if (score > best_score) {
        best_score  <- score
        best_result <- res
      }
    }
  }

  if (is.null(best_result))
    stop("All tuning runs failed for dataset '", dataset, "'.")

  if (verbose)
    message(sprintf("[tune] Best score=%.4f", best_score))

  # Write output for the winning configuration if requested
  if (!is.null(out_dir) && !is.null(best_result)) {
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    readr::write_csv(best_result$predictions,
                     file.path(out_dir, paste0(dataset, "_tuned_predictions.csv")))
    if (!is.null(best_result$performance))
      readr::write_csv(best_result$performance,
                       file.path(out_dir, paste0(dataset, "_tuned_performance.csv")))
  }

  best_result
}


# ── Stability-assessed ER ─────────────────────────────────────────────────────

#' Run ERBOT with bootstrap stability assessment
#'
#' Runs the pipeline \code{B} times on random subsamples of \code{frac}
#' fraction of records, computes pairwise ARI across runs to assess stability,
#' and returns the single full-data run together with stability scores.
#'
#' @param data Data argument for \code{\link{er_run}()}.
#' @param B Integer.  Number of bootstrap replications.  Default 10.
#' @param frac Numeric in (0,1].  Subsample fraction.  Default 0.8.
#' @param out_dir Optional output directory.
#' @param ... Additional arguments forwarded to \code{\link{er_run}()}.
#'
#' @return A list with elements \code{result} (full-data \code{er_result}),
#'   \code{mean_ari} (mean pairwise ARI across bootstrap runs), and
#'   \code{ari_matrix} (B x B pairwise ARI matrix).
#' @export
run_stable_er <- function(data, B = 10L, frac = 0.8, out_dir = NULL, ...) {
  # Full-data run
  full_result <- er_run(data = data, out_dir = NULL, ...)

  df_full <- er_load(data)
  n       <- nrow(df_full)
  size    <- max(10L, as.integer(frac * n))

  boot_labels <- vector("list", B)
  for (b in seq_len(B)) {
    idx    <- sort(sample.int(n, size, replace = FALSE))
    df_sub <- df_full[idx, , drop = FALSE]
    res_b  <- tryCatch(
      er_run(data = df_sub, verbose = FALSE, ...),
      error = function(e) NULL
    )
    if (!is.null(res_b)) boot_labels[[b]] <- res_b$clusters
  }
  boot_labels <- boot_labels[!vapply(boot_labels, is.null, logical(1L))]

  # Pairwise ARI
  B_ok <- length(boot_labels)
  ari_mat <- matrix(NA_real_, B_ok, B_ok)
  if (B_ok >= 2L && requireNamespace("GCMER", quietly = TRUE)) {
    for (i in seq_len(B_ok)) {
      for (j in seq_len(B_ok)) {
        if (i == j) { ari_mat[i, j] <- 1; next }
        # Both vectors have the same length (sub-sample size)
        l1 <- boot_labels[[i]]; l2 <- boot_labels[[j]]
        mn <- min(length(l1), length(l2))
        ari_mat[i, j] <- tryCatch(
          GCMER::adj_rand(l1[seq_len(mn)], l2[seq_len(mn)]),
          error = function(e) NA_real_
        )
      }
    }
  }
  mean_ari <- mean(ari_mat[upper.tri(ari_mat)], na.rm = TRUE)
  message(sprintf("[run_stable_er] Mean bootstrap ARI = %.4f (B=%d)", mean_ari, B_ok))

  if (!is.null(out_dir)) {
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    readr::write_csv(full_result$predictions,
                     file.path(out_dir, "stable_er_predictions.csv"))
    saveRDS(list(mean_ari = mean_ari, ari_matrix = ari_mat),
            file.path(out_dir, "stable_er_stability.rds"))
  }

  list(result     = full_result,
       mean_ari   = mean_ari,
       ari_matrix = ari_mat)
}


# ── Custom-weight runner ───────────────────────────────────────────────────────

#' Run ERBOT with a user-supplied field weight vector
#'
#' Convenience wrapper that passes a named numeric weight vector directly to
#' \code{\link{er_run}()}, bypassing the weight-learning step.
#'
#' @param data Data argument for \code{\link{er_run}()}.
#' @param weights Named numeric vector of field weights (will be normalised to
#'   sum to 1).
#' @param ... Additional arguments forwarded to \code{\link{er_run}()}.
#'
#' @return An \code{er_result} object.
#' @export
run_with_weights <- function(data, weights, ...) {
  if (!is.numeric(weights) || is.null(names(weights)))
    stop("'weights' must be a named numeric vector.")
  s <- sum(weights, na.rm = TRUE)
  if (s <= 0) stop("'weights' must have a positive sum.")
  weights <- weights / s
  er_run(data = data, weights = weights, ...)
}


# ── Back-compatibility aliases ─────────────────────────────────────────────────

#' @rdname er_run
#' @export
er_main <- er_run

#' @rdname er_run
#' @export
er_main_simple <- er_run

#' @rdname er_run
#' @export
er_unified_pipeline <- er_run

#' @rdname er_run
#' @export
er_general_pipeline <- er_run


# ── Internal helper ────────────────────────────────────────────────────────────

# Return the package root directory (works both installed and in development).
.pkg_root <- function() {
  tryCatch(
    normalizePath(file.path(dirname(sys.frame(0)$ofile), "..")),
    error = function(e)
      tryCatch(
        system.file(package = "erbot"),
        error = function(e2) "."
      )
  )
}
