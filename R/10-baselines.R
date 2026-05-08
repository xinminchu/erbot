########################################
# File: R/10-baselines.R
#
# External baseline wrappers and an improved Fellegi–Sunter EM for
# comparison against ERBOT clustering methods.
#
# Public API
# ----------
#   er_baseline_fs_em()       -- Fellegi–Sunter joint-comparison-vector EM
#   er_baseline_splink()      -- Splink (Python, via system2)
#   er_baseline_deepmatcher() -- DeepMatcher (Python, via system2)
#   er_baseline_ditto()       -- Ditto / BERT (Python, via system2)
#   er_run_baselines()        -- run all baselines, return unified perf table
#
# Python backends live in inst/python/er_*.py and are called via system2().
# All baselines return a one-row (or multi-row) data.frame with columns:
#   method, ARI, Bcubed_F, Vmeasure, Homogeneity, Completeness
# so they can be row-bound with er_evaluate() output for direct comparison.
########################################

# ── Internal helpers ──────────────────────────────────────────────────────────

#' Locate the inst/python directory for the package
#' @keywords internal
.python_script <- function(name) {
  # Works both from devtools::load_all() and installed package
  candidates <- c(
    system.file("python", name, package = "erbot"),
    file.path(find.package("erbot", quiet = TRUE), "python", name),
    file.path(dirname(dirname(sys.frame(1)$ofile %||% "")), "inst", "python", name)
  )
  candidates <- candidates[nchar(candidates) > 0 & file.exists(candidates)]
  if (!length(candidates)) {
    # fallback: walk up from this file's directory
    this_dir <- tryCatch(dirname(sys.frame(0)$ofile), error = function(e) ".")
    candidate <- file.path(this_dir, "..", "inst", "python", name)
    if (file.exists(candidate)) return(normalizePath(candidate))
    stop(sprintf(
      "Cannot locate inst/python/%s. Re-install erbot or call devtools::load_all().",
      name
    ))
  }
  candidates[[1L]]
}

#' Find Python 3 executable
#' @keywords internal
.python_exe <- function() {
  for (cmd in c(Sys.getenv("ERBOT_PYTHON", ""), "python3", "python")) {
    if (nchar(cmd) == 0L) next
    v <- tryCatch(
      suppressWarnings(system2(cmd, "--version", stdout = TRUE, stderr = TRUE)),
      error = function(e) ""
    )
    if (any(grepl("Python 3", v))) return(cmd)
  }
  stop(paste0(
    "Python 3 not found. Install Python 3 and ensure it is on PATH, ",
    "or set env var ERBOT_PYTHON to the full path of the python executable."
  ))
}

#' Run a Python baseline script via system2 and return structured output
#' @keywords internal
.run_python_baseline <- function(script, args, output_csv) {
  py  <- .python_exe()
  ret <- system2(py, c(script, args), stdout = output_csv, stderr = TRUE)
  if (!file.exists(output_csv) || file.info(output_csv)$size == 0L) {
    stop(sprintf("Python baseline script returned exit code %d.", ret))
  }
  readr::read_csv(output_csv, show_col_types = FALSE)
}

#' Convert a data.frame of pair-level match predictions to cluster labels
#' @keywords internal
.pairs_to_labels <- function(preds, id_vec, threshold = 0.5) {
  matched <- preds[preds$match_prob >= threshold, c("id1", "id2"), drop = FALSE]
  if (!nrow(matched)) return(setNames(seq_along(id_vec), id_vec))
  g    <- igraph::graph_from_data_frame(matched, directed = FALSE,
           vertices = data.frame(name = id_vec))
  cl   <- igraph::components(g)$membership
  cl
}

#' Convert baseline cluster labels + truth to a one-row performance data.frame
#' @keywords internal
.baseline_perf_row <- function(method_name, pred_labels, truth_tbl, id_vec) {
  if (is.null(truth_tbl) || !nrow(truth_tbl)) {
    return(data.frame(method = method_name, ARI = NA_real_,
                      Bcubed_F = NA_real_, Vmeasure = NA_real_,
                      Homogeneity = NA_real_, Completeness = NA_real_,
                      stringsAsFactors = FALSE))
  }
  truth_map <- setNames(as.integer(truth_tbl$cluster_id),
                        as.character(truth_tbl$id))
  has_truth <- !is.na(truth_map[id_vec])
  pred_sub  <- pred_labels[id_vec[has_truth]]
  true_sub  <- truth_map[id_vec[has_truth]]

  ari  <- tryCatch(GCMER::adj_rand(pred_sub, true_sub), error = function(e) NA_real_)
  bcub <- tryCatch(er_bcubed(pred_sub, true_sub), error = function(e) list(F = NA_real_))
  vm   <- tryCatch(er_vmeasure(pred_sub, true_sub),
                   error = function(e) list(V = NA_real_, H = NA_real_, C = NA_real_))

  data.frame(
    method       = method_name,
    ARI          = round(ari, 4L),
    Bcubed_F     = round(bcub$F, 4L),
    Vmeasure     = round(vm$V, 4L),
    Homogeneity  = round(vm$H, 4L),
    Completeness = round(vm$C, 4L),
    stringsAsFactors = FALSE
  )
}

# ── 1. Fellegi–Sunter Joint-Comparison-Vector EM ──────────────────────────────

#' Fellegi–Sunter EM baseline
#'
#' Implements a proper joint Fellegi–Sunter comparison-vector EM
#' (Fellegi & Sunter 1969; Larsen & Rubin 2001).  Unlike the per-field
#' approximation in \code{er_weights(method="fellegi_sunter")}, this version:
#' \enumerate{
#'   \item Discretises each field similarity into \eqn{L} levels (default 3).
#'   \item Runs EM on the joint comparison vector under conditional-independence
#'         mixture: \eqn{P(\boldsymbol{\gamma} | M)} and
#'         \eqn{P(\boldsymbol{\gamma} | U)}, each a product of per-field
#'         multinomials.
#'   \item Classifies pairs above a log-odds threshold as matches.
#'   \item Applies threshold-connected-components to build entity clusters.
#' }
#'
#' No ground truth is required; this is a fully unsupervised baseline.
#'
#' @param data data.frame of records.
#' @param truth Optional ground truth (named int vector or tibble(id, cluster_id)).
#' @param id_col Character. ID column. Auto-detected if NULL.
#' @param block Blocking spec passed to \code{er_block()}.
#' @param n_levels Integer. Number of discretisation bins per field (default 3).
#' @param n_iter Integer. EM iterations (default 20).
#' @param pi_m_init Numeric. Initial match prior (default 0.1).
#' @param log_odds_threshold Numeric. Classification threshold on log-odds.
#' @param verbose Logical.
#' @return A named list: \code{performance} (data.frame), \code{predictions}
#'   (tibble), \code{weights} (named vector), \code{log_odds} (pair-level).
#' @export
er_baseline_fs_em <- function(data, truth = NULL, id_col = NULL,
                               block = "auto",
                               n_levels = 3L, n_iter = 20L,
                               pi_m_init = 0.1,
                               log_odds_threshold = 0,
                               verbose = TRUE) {
  `%||%` <- get("%||%", asNamespace("erbot"))
  .msg <- function(...) if (verbose) message(...)

  # Stage 1–4: load, diagnose, block, similarity (reuse pipeline)
  .msg("[fs_em] Loading and blocking...")
  df      <- er_load(data)
  n       <- nrow(df)
  diag    <- er_diagnose(df, id_col = id_col)
  id_col_used <- diag$id_col
  id_vec  <- as.character(df[[id_col_used]])

  pairs <- er_block(df, diag = diag,
                    method   = if (is.list(block)) block$method %||% "auto" else block,
                    block_key = if (is.list(block)) block$key %||% NULL else NULL,
                    prefix_len = if (is.list(block)) block$prefix_len %||% 3L else 3L,
                    max_pairs  = if (is.list(block)) block$max_pairs %||% 2e6 else 2e6)
  .msg(sprintf("[fs_em] %s candidate pairs.", format(nrow(pairs), big.mark = ",")))

  sim_list <- er_similarity(df, pairs, diag = diag)
  fnames   <- names(sim_list)
  K        <- length(fnames)
  N        <- nrow(pairs)

  # Build gamma matrix: N × K, each column in {1,...,n_levels}
  .msg("[fs_em] Building comparison vector matrix...")
  gamma <- matrix(NA_integer_, nrow = N, ncol = K,
                  dimnames = list(NULL, fnames))
  breaks_list <- vector("list", K); names(breaks_list) <- fnames
  for (k in seq_len(K)) {
    sv <- sim_list[[fnames[k]]]
    # Replace NA with midpoint (missing field = no information)
    sv[is.na(sv)] <- 0.5
    bk <- seq(0, 1, length.out = n_levels + 1L)
    bk[1L] <- -Inf; bk[n_levels + 1L] <- Inf
    breaks_list[[fnames[k]]] <- bk
    gamma[, k] <- as.integer(cut(sv, breaks = bk, labels = FALSE,
                                  include.lowest = TRUE))
  }

  # EM: mixture of two products-of-multinomials (M and U components)
  # Parameters: pi_m (scalar), m_probs[k, l], u_probs[k, l]
  .msg("[fs_em] Running EM...")
  eps <- 1e-6
  pi_m   <- pi_m_init
  # Initialise m_probs: peaked near level n_levels; u_probs: peaked near level 1
  m_probs <- matrix(eps, nrow = K, ncol = n_levels)
  u_probs <- matrix(eps, nrow = K, ncol = n_levels)
  for (k in seq_len(K)) {
    m_probs[k, n_levels] <- 1 - eps * (n_levels - 1L)
    u_probs[k, 1L]       <- 1 - eps * (n_levels - 1L)
  }

  for (iter in seq_len(n_iter)) {
    # E-step: log P(gamma_i | M) and log P(gamma_i | U)
    log_pm <- rowSums(matrix(
      vapply(seq_len(K), function(k) log(m_probs[k, gamma[, k]]), numeric(N)),
      nrow = N))
    log_pu <- rowSums(matrix(
      vapply(seq_len(K), function(k) log(u_probs[k, gamma[, k]]), numeric(N)),
      nrow = N))
    # Stabilise
    log_pm_scaled <- log_pm - apply(cbind(log_pm, log_pu), 1L, max)
    log_pu_scaled <- log_pu - apply(cbind(log_pm, log_pu), 1L, max)
    pm <- pi_m * exp(log_pm_scaled)
    pu <- (1 - pi_m) * exp(log_pu_scaled)
    denom <- pm + pu
    denom[denom == 0] <- 1e-300
    r_m <- pm / denom        # posterior P(M | gamma_i)

    # M-step
    pi_m <- mean(r_m)
    pi_m <- pmax(eps, pmin(1 - eps, pi_m))
    for (k in seq_len(K)) {
      for (l in seq_len(n_levels)) {
        idx_l        <- gamma[, k] == l
        m_probs[k, l] <- (sum(r_m[idx_l]) + eps) / (sum(r_m) + n_levels * eps)
        u_probs[k, l] <- (sum(1 - r_m[idx_l]) + eps) /
                          (sum(1 - r_m) + n_levels * eps)
      }
    }
  }

  # Log-odds ratio per pair: log P(M|gamma) / P(U|gamma) (without prior)
  log_odds <- rowSums(matrix(
    vapply(seq_len(K), function(k)
      log(m_probs[k, gamma[, k]]) - log(u_probs[k, gamma[, k]]),
      numeric(N)),
    nrow = N))

  # Field-level discriminativity: E_M[log m_k] - E_U[log u_k]
  field_weights <- vapply(seq_len(K), function(k) {
    wm <- sum(r_m * (log(m_probs[k, gamma[, k]]) - log(u_probs[k, gamma[, k]])))
    max(0, wm / max(sum(r_m), 1e-10))
  }, numeric(1L))
  names(field_weights) <- fnames
  if (sum(field_weights) > 0) field_weights <- field_weights / sum(field_weights)

  # Classify pairs and build similarity graph
  match_prob <- 1 / (1 + exp(-log_odds))     # sigmoid of log-odds
  S <- er_pairs_to_sparse(pairs, match_prob, n)

  # Cluster via threshold-CC at log_odds_threshold
  thresh_prob <- 1 / (1 + exp(-log_odds_threshold))
  labels <- er_cluster(S, method = "threshold_cc", threshold = thresh_prob)

  # Evaluate
  truth_parsed <- if (!is.null(truth))
    tryCatch(er_truth_from_any(truth), error = function(e) NULL) else NULL
  perf <- .baseline_perf_row("fs_em", labels, truth_parsed, id_vec)

  predictions <- tibble::tibble(id = id_vec, cluster_id = as.integer(labels))
  list(
    performance  = perf,
    predictions  = predictions,
    weights      = field_weights,
    log_odds     = log_odds,
    match_prob   = match_prob,
    pi_m         = pi_m,
    m_probs      = m_probs,
    u_probs      = u_probs
  )
}

# ── 2. Splink ─────────────────────────────────────────────────────────────────

#' Splink probabilistic linkage baseline
#'
#' Calls the Python \pkg{splink} library (version \eqn{\geq} 4) via
#' \code{system2()}.  Splink implements a Fellegi–Sunter EM with
#' DuckDB-backed blocking and comparison functions.
#'
#' @param data data.frame of records.
#' @param truth Optional ground truth.
#' @param id_col Character. ID column.
#' @param block_col Character. Column to block on. Auto-detected if NULL.
#' @param fields Character vector. Comparison fields. Auto-detected if NULL.
#' @param threshold Numeric. Match probability threshold (default 0.9).
#' @param tmp_dir Character. Temporary directory for CSV interchange.
#' @param verbose Logical.
#' @return Named list: \code{performance} (data.frame), \code{predictions}
#'   (tibble(id, cluster_id)), \code{raw} (pair-level match probabilities).
#' @export
er_baseline_splink <- function(data, truth = NULL, id_col = NULL,
                                block_col = NULL, fields = NULL,
                                threshold = 0.9,
                                tmp_dir = tempdir(),
                                verbose = TRUE) {
  .msg <- function(...) if (verbose) message(...)
  .msg("[splink] Preparing data...")

  df <- er_load(data)
  n  <- nrow(df)
  if (is.null(id_col)) {
    diag   <- er_diagnose(df)
    id_col <- diag$id_col
  }
  id_vec <- as.character(df[[id_col]])

  # Write data to temp CSV
  data_csv <- file.path(tmp_dir, "erbot_splink_data.csv")
  out_csv  <- file.path(tmp_dir, "erbot_splink_output.csv")
  readr::write_csv(df, data_csv)

  # Auto-detect block_col and fields
  if (is.null(block_col)) {
    non_id <- setdiff(names(df), id_col)
    block_col <- non_id[which.max(vapply(non_id, function(cn)
      length(unique(substr(df[[cn]], 1L, 3L))), integer(1L)))]
  }
  if (is.null(fields)) {
    fields <- setdiff(names(df), id_col)
  }

  script <- tryCatch(.python_script("er_splink.py"), error = function(e) {
    stop("Splink Python backend not found: ", e$message)
  })

  args <- c(
    script,
    "--data",      shQuote(data_csv),
    "--output",    shQuote(out_csv),
    "--id_col",    shQuote(id_col),
    "--block_col", shQuote(block_col),
    "--fields",    paste(shQuote(fields), collapse = " "),
    "--threshold", threshold
  )

  .msg("[splink] Running Python Splink...")
  py  <- tryCatch(.python_exe(), error = function(e) {
    stop("Python not found for Splink baseline: ", e$message)
  })
  ret <- suppressWarnings(system2(py, args, stdout = TRUE, stderr = TRUE))
  if (verbose) message(paste(ret[grepl("\\[splink\\]", ret)], collapse = "\n"))

  if (!file.exists(out_csv) || file.info(out_csv)$size == 0L) {
    stop("[splink] Output file not produced. Check Python splink installation.\n",
         "Install with: pip install splink")
  }

  preds_raw <- readr::read_csv(out_csv, show_col_types = FALSE)
  labels    <- .pairs_to_labels(preds_raw, id_vec, threshold = threshold)
  truth_parsed <- if (!is.null(truth))
    tryCatch(er_truth_from_any(truth), error = function(e) NULL) else NULL
  perf <- .baseline_perf_row("splink", labels, truth_parsed, id_vec)

  list(
    performance  = perf,
    predictions  = tibble::tibble(id = id_vec, cluster_id = as.integer(labels[id_vec])),
    raw          = preds_raw
  )
}

# ── 3. DeepMatcher ────────────────────────────────────────────────────────────

#' DeepMatcher deep learning baseline
#'
#' Calls the Python \pkg{deepmatcher} library via \code{system2()}.
#' DeepMatcher trains a recurrent/attention network on labelled record pairs
#' and predicts match/non-match for all candidate pairs.
#'
#' Requires a labelled train set (from \code{truth}).  A 70/30 internal
#' split of gold pairs + sampled non-matches is used for train/validation.
#'
#' @param data data.frame of records.
#' @param truth Ground truth (required for DeepMatcher training).
#' @param id_col Character. ID column.
#' @param block Blocking spec for candidate generation.
#' @param model Character. DeepMatcher model type: \code{"hybrid"},
#'   \code{"rnn"}, \code{"attention"}, \code{"sif"} (default \code{"hybrid"}).
#' @param epochs Integer. Training epochs (default 10).
#' @param threshold Numeric. Match probability threshold (default 0.5).
#' @param tmp_dir Character. Working directory for data exchange.
#' @param verbose Logical.
#' @return Named list: \code{performance}, \code{predictions}, \code{raw}.
#' @export
er_baseline_deepmatcher <- function(data, truth, id_col = NULL,
                                     block = "auto",
                                     model = "hybrid", epochs = 10L,
                                     threshold = 0.5,
                                     tmp_dir = tempdir(),
                                     verbose = TRUE) {
  .msg <- function(...) if (verbose) message(...)
  if (is.null(truth)) stop("DeepMatcher requires ground truth for training.")
  .msg("[deepmatcher] Preparing candidate pairs and labels...")

  df   <- er_load(data)
  n    <- nrow(df)
  diag <- er_diagnose(df, id_col = id_col)
  id_col_used <- diag$id_col
  id_vec <- as.character(df[[id_col_used]])

  # Generate blocking pairs and similarities
  block_method  <- if (is.list(block)) block$method   %||% "auto" else block
  block_key     <- if (is.list(block)) block$key       %||% NULL  else NULL
  prefix_len    <- if (is.list(block)) block$prefix_len %||% 3L   else 3L
  pairs <- er_block(df, diag = diag, method = block_method,
                    block_key = block_key, prefix_len = prefix_len)

  truth_parsed <- er_truth_from_any(truth)
  truth_map    <- setNames(as.integer(truth_parsed$cluster_id),
                           as.character(truth_parsed$id))
  truth_vec    <- truth_map[id_vec]

  # Build pair labels: 1 if same cluster, 0 otherwise
  p1_cluster <- truth_vec[pairs$idx1]
  p2_cluster <- truth_vec[pairs$idx2]
  pair_label <- as.integer(!is.na(p1_cluster) & !is.na(p2_cluster) &
                             p1_cluster == p2_cluster)

  # Build DeepMatcher-format data (left + right record columns side by side)
  field_cols <- setdiff(names(df), id_col_used)
  left_df  <- df[pairs$idx1, field_cols, drop = FALSE]
  right_df <- df[pairs$idx2, field_cols, drop = FALSE]
  names(left_df)  <- paste0("left_",  field_cols)
  names(right_df) <- paste0("right_", field_cols)
  dm_df <- cbind(
    data.frame(id1 = id_vec[pairs$idx1], id2 = id_vec[pairs$idx2],
               label = pair_label),
    left_df, right_df
  )

  # Write to temp
  dm_csv  <- file.path(tmp_dir, "erbot_dm_pairs.csv")
  out_csv <- file.path(tmp_dir, "erbot_dm_output.csv")
  readr::write_csv(dm_df, dm_csv)

  script <- tryCatch(.python_script("er_deepmatcher.py"), error = function(e) {
    stop("DeepMatcher Python backend not found: ", e$message)
  })
  py <- tryCatch(.python_exe(), error = function(e) {
    stop("Python not found for DeepMatcher: ", e$message)
  })

  args <- c(
    script,
    "--input",     shQuote(dm_csv),
    "--output",    shQuote(out_csv),
    "--fields",    paste(shQuote(field_cols), collapse = " "),
    "--model",     model,
    "--epochs",    epochs,
    "--threshold", threshold
  )

  .msg(sprintf("[deepmatcher] Training %s model for %d epochs...", model, epochs))
  ret <- suppressWarnings(system2(py, args, stdout = TRUE, stderr = TRUE))
  if (verbose) message(paste(ret[grepl("\\[deepmatcher\\]", ret)], collapse = "\n"))

  if (!file.exists(out_csv) || file.info(out_csv)$size == 0L) {
    stop("[deepmatcher] Output not produced. Check DeepMatcher installation.\n",
         "Install with: pip install deepmatcher")
  }

  preds_raw <- readr::read_csv(out_csv, show_col_types = FALSE)
  labels    <- .pairs_to_labels(preds_raw, id_vec, threshold = threshold)
  perf      <- .baseline_perf_row("deepmatcher", labels, truth_parsed, id_vec)

  list(
    performance = perf,
    predictions = tibble::tibble(id = id_vec, cluster_id = as.integer(labels[id_vec])),
    raw         = preds_raw
  )
}

# ── 4. Ditto ──────────────────────────────────────────────────────────────────

#' Ditto (BERT-based) entity matching baseline
#'
#' Calls the Python \pkg{ditto} library via \code{system2()}.  Ditto
#' fine-tunes a pre-trained language model (default \code{distilbert-base})
#' on serialised record-pair strings for binary match classification.
#'
#' @param data data.frame of records.
#' @param truth Ground truth (required for Ditto fine-tuning).
#' @param id_col Character. ID column.
#' @param block Blocking spec.
#' @param lm Character. Hugging Face model ID
#'   (default \code{"distilbert-base-uncased"}).
#' @param epochs Integer. Fine-tuning epochs (default 5).
#' @param threshold Numeric. Match probability threshold (default 0.5).
#' @param tmp_dir Character. Working directory.
#' @param verbose Logical.
#' @return Named list: \code{performance}, \code{predictions}, \code{raw}.
#' @export
er_baseline_ditto <- function(data, truth, id_col = NULL,
                               block = "auto",
                               lm = "distilbert-base-uncased",
                               epochs = 5L,
                               threshold = 0.5,
                               tmp_dir = tempdir(),
                               verbose = TRUE) {
  .msg <- function(...) if (verbose) message(...)
  if (is.null(truth)) stop("Ditto requires ground truth for fine-tuning.")
  .msg("[ditto] Preparing serialised record pairs...")

  df   <- er_load(data)
  n    <- nrow(df)
  diag <- er_diagnose(df, id_col = id_col)
  id_col_used <- diag$id_col
  id_vec  <- as.character(df[[id_col_used]])
  field_cols <- setdiff(names(df), id_col_used)

  block_method  <- if (is.list(block)) block$method   %||% "auto" else block
  block_key     <- if (is.list(block)) block$key       %||% NULL  else NULL
  prefix_len    <- if (is.list(block)) block$prefix_len %||% 3L   else 3L
  pairs <- er_block(df, diag = diag, method = block_method,
                    block_key = block_key, prefix_len = prefix_len)

  truth_parsed <- er_truth_from_any(truth)
  truth_map    <- setNames(as.integer(truth_parsed$cluster_id),
                           as.character(truth_parsed$id))
  truth_vec    <- truth_map[id_vec]
  p1_cl <- truth_vec[pairs$idx1]; p2_cl <- truth_vec[pairs$idx2]
  pair_label <- as.integer(!is.na(p1_cl) & !is.na(p2_cl) & p1_cl == p2_cl)

  # Ditto format: each row is a JSONL entry
  # {"left": "COL field1 VAL val1 ...", "right": "...", "label": 0/1}
  .ser_record <- function(row_idx) {
    r <- df[row_idx, field_cols, drop = FALSE]
    paste(mapply(function(f, v) paste("COL", f, "VAL", v),
                 field_cols, as.character(r[1, ])),
          collapse = " ")
  }

  ditto_lines <- vapply(seq_len(nrow(pairs)), function(i) {
    left  <- .ser_record(pairs$idx1[i])
    right <- .ser_record(pairs$idx2[i])
    jsonlite::toJSON(list(
      left   = jsonlite::unbox(left),
      right  = jsonlite::unbox(right),
      label  = jsonlite::unbox(pair_label[i]),
      id1    = jsonlite::unbox(id_vec[pairs$idx1[i]]),
      id2    = jsonlite::unbox(id_vec[pairs$idx2[i]])
    ), auto_unbox = FALSE)
  }, character(1L))

  ditto_file <- file.path(tmp_dir, "erbot_ditto_pairs.jsonl")
  out_csv    <- file.path(tmp_dir, "erbot_ditto_output.csv")
  writeLines(ditto_lines, ditto_file)

  script <- tryCatch(.python_script("er_ditto.py"), error = function(e) {
    stop("Ditto Python backend not found: ", e$message)
  })
  py <- tryCatch(.python_exe(), error = function(e) {
    stop("Python not found for Ditto: ", e$message)
  })

  args <- c(
    script,
    "--input",     shQuote(ditto_file),
    "--output",    shQuote(out_csv),
    "--lm",        shQuote(lm),
    "--epochs",    epochs,
    "--threshold", threshold
  )

  .msg(sprintf("[ditto] Fine-tuning %s for %d epochs...", lm, epochs))
  ret <- suppressWarnings(system2(py, args, stdout = TRUE, stderr = TRUE))
  if (verbose) message(paste(ret[grepl("\\[ditto\\]", ret)], collapse = "\n"))

  if (!file.exists(out_csv) || file.info(out_csv)$size == 0L) {
    stop("[ditto] Output not produced. Check Ditto/transformers installation.\n",
         "Install with: pip install transformers torch")
  }

  preds_raw <- readr::read_csv(out_csv, show_col_types = FALSE)
  labels    <- .pairs_to_labels(preds_raw, id_vec, threshold = threshold)
  perf      <- .baseline_perf_row("ditto", labels, truth_parsed, id_vec)

  list(
    performance = perf,
    predictions = tibble::tibble(id = id_vec, cluster_id = as.integer(labels[id_vec])),
    raw         = preds_raw
  )
}

# ── 5. Run all baselines ──────────────────────────────────────────────────────

#' Run all external baselines and return a unified performance table
#'
#' Runs Fellegi–Sunter EM, Splink, DeepMatcher, and Ditto.  Any baseline that
#' fails (missing Python dependency, GPU requirement, etc.) is skipped with a
#' warning and represented as a row of NAs in the output table.
#'
#' @param data data.frame of records.
#' @param truth Optional ground truth.
#' @param id_col Character. ID column.
#' @param block Blocking spec (used for DeepMatcher and Ditto pair generation).
#' @param baselines Character vector. Which baselines to run.
#'   Default \code{c("fs_em", "splink", "deepmatcher", "ditto")}.
#' @param ... Additional arguments forwarded to individual baseline functions.
#' @return data.frame with one row per baseline and columns
#'   \code{method, ARI, Bcubed_F, Vmeasure, Homogeneity, Completeness}.
#' @export
er_run_baselines <- function(data, truth = NULL, id_col = NULL,
                              block = "auto",
                              baselines = c("fs_em", "splink",
                                            "deepmatcher", "ditto"),
                              ...) {
  results <- list()

  for (bl in baselines) {
    message(sprintf("[baselines] Running %s...", bl))
    row <- tryCatch({
      res <- switch(bl,
        fs_em       = er_baseline_fs_em(data, truth, id_col = id_col,
                                         block = block, verbose = FALSE, ...),
        splink      = er_baseline_splink(data, truth, id_col = id_col,
                                          verbose = FALSE, ...),
        deepmatcher = er_baseline_deepmatcher(data, truth, id_col = id_col,
                                               block = block, verbose = FALSE, ...),
        ditto       = er_baseline_ditto(data, truth, id_col = id_col,
                                         block = block, verbose = FALSE, ...),
        stop("Unknown baseline: ", bl)
      )
      res$performance
    }, error = function(e) {
      warning(sprintf("[baselines] %s failed: %s", bl, conditionMessage(e)))
      data.frame(method = bl, ARI = NA_real_, Bcubed_F = NA_real_,
                 Vmeasure = NA_real_, Homogeneity = NA_real_,
                 Completeness = NA_real_, stringsAsFactors = FALSE)
    })
    results[[bl]] <- row
  }

  do.call(rbind, results)
}
