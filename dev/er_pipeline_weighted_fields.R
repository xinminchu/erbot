# =====================================================================
# ER General Pipeline with Weighted Fields, CV-based Weight Learning
# Author: ChatGPT (ERBOT helper)
# Description:
#   1) Allow user-specified field weights (sum to 1) for any fields in a
#      CORA-like dataset (excluding the unique id column).
#   2) If er_main supports a `field_weights` argument, we will use it.
#      Otherwise, we fallback to a robust approximation by *expanding*
#      fields proportional to weights (token repetition) before calling
#      er_main.
#   3) Provide cross-validated supervised weight learning using Adjusted
#      Rand Index (ARI) on labeled truth.
#   4) Expose a single-entry `er_general_pipeline()` to run once with
#      either fixed/user weights or learned weights.
#
# Notes:
#   - This code assumes you have er_main() and er_save_report_pdf()
#     available in your environment, with signatures compatible with
#     examples you shared previously.
#   - Cross-validation for clustering is approximate: we run on subsets
#     of records (folds) and evaluate ARI against the subset of truth.
#   - If your truth is a *pair list* (id1, id2, match), we subset pairs
#     to ones fully within each fold. If truth is a *vector of labels*,
#     we subset by indices.
#   - For speed, set modest budgets/folds or sub-sample per fold.
# =====================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
})

# ------------------------------
# Helpers: Weights & Simplex
# ------------------------------
normalize_weights <- function(weights, field_names) {
  # Accepts numeric vector (named or unnamed) or list; returns named numeric
  w <- unlist(weights)
  if (is.null(names(w))) {
    if (length(w) != length(field_names)) stop("weights length must match fields")
    names(w) <- field_names
  } else {
    # Reorder & fill missing with 0
    w <- w[field_names]
    w[is.na(w)] <- 0
  }
  if (any(w < 0)) stop("weights must be non-negative")
  s <- sum(w)
  if (s <= 0) stop("sum(weights) must be > 0")
  w <- w / s
  return(w)
}

rdirichlet_simple <- function(n, alpha) {
  # Sample n Dirichlet vectors using gamma normalization
  # alpha: numeric vector >0
  k <- length(alpha)
  out <- matrix(NA_real_, nrow = n, ncol = k)
  for (i in seq_len(n)) {
    g <- stats::rgamma(k, shape = alpha, rate = 1)
    out[i, ] <- g / sum(g)
  }
  colnames(out) <- names(alpha)
  out
}

# ------------------------------
# Helpers: Field Expansion (fallback weighting)
# ------------------------------
expand_fields_by_weights <- function(fields, weights, base_rep = 10L) {
  # Approximate weighting by repeating field names according to weights.
  # Returns a character vector of fields (with repeats).
  w <- normalize_weights(weights, fields)
  reps <- pmax(1L, round(w * base_rep))  # ensure at least one
  expanded <- unlist(mapply(function(f, r) rep(f, r), fields, reps, SIMPLIFY = FALSE), use.names = FALSE)
  attr(expanded, "weights") <- w
  attr(expanded, "reps") <- reps
  expanded
}

# ------------------------------
# Helpers: Truth subsetting for CV
# ------------------------------
subset_truth <- function(truth, keep_ids, id_col = "id") {
  # Returns truth restricted to records with id in keep_ids.
  # Supports (i) vector of cluster labels named by id, or
  #          (ii) data.frame of pairs with columns id1,id2,match (case-insensitive), or
  #          (iii) list with $pairs or $labels components.

  # Utility: lower-case column names
  lc <- function(x) { tolower(gsub("\\s+", "_", x)) }

  # Named vector of labels case
  if (is.vector(truth) && !is.null(names(truth))) {
    return(truth[intersect(names(truth), keep_ids)])
  }

  # Data.frame of pairs case
  if (is.data.frame(truth)) {
    cn <- lc(names(truth))
    id1 <- which(cn %in% c("id1", paste0(id_col, "1")))[1]
    id2 <- which(cn %in% c("id2", paste0(id_col, "2")))[1]
    match_col <- which(cn %in% c("match","is_match","label"))[1]
    if (is.na(id1) || is.na(id2)) return(truth) # can't subset confidently
    tsub <- truth[truth[[id1]] %in% keep_ids & truth[[id2]] %in% keep_ids, , drop = FALSE]
    return(tsub)
  }

  # List with possible components
  if (is.list(truth)) {
    if (!is.null(truth$labels)) {
      tr <- truth$labels
      if (!is.null(names(tr))) tr <- tr[intersect(names(tr), keep_ids)]
      return(list(labels = tr))
    }
    if (!is.null(truth$pairs) && is.data.frame(truth$pairs)) {
      pr <- truth$pairs
      cn <- lc(names(pr))
      id1 <- which(cn %in% c("id1", paste0(id_col, "1")))[1]
      id2 <- which(cn %in% c("id2", paste0(id_col, "2")))[1]
      if (!is.na(id1) && !is.na(id2)) {
        pr <- pr[pr[[id1]] %in% keep_ids & pr[[id2]] %in% keep_ids, , drop = FALSE]
      }
      return(list(pairs = pr))
    }
  }

  truth # fallback
}

subset_data_ids <- function(data, keep_ids, id_col = "id") {
  if (id_col %in% names(data)) {
    data[data[[id_col]] %in% keep_ids, , drop = FALSE]
  } else {
    # fallback: use rownames if present, else integer row index
    rn <- rownames(data)
    if (!is.null(rn)) {
      data[rn %in% keep_ids, , drop = FALSE]
    } else {
      idx <- as.integer(keep_ids)
      data[idx, , drop = FALSE]
    }
  }
}

# ------------------------------
# Helper: Extract ARI from er_main result
# ------------------------------
extract_adj_rand <- function(res, method = c("kmeans","gc","louvain")) {
  method <- match.arg(method)
  # Try common locations; adapt to your res structure
  try_paths <- list(
    c("metrics", method, "adj_rand"),
    c("summary", "metrics", method, "adj_rand"),
    c(method, "adj_rand")
  )
  for (p in try_paths) {
    cur <- res
    ok <- TRUE
    for (nm in p) {
      if (is.null(cur)) { ok <- FALSE; break }
      cur <- tryCatch(cur[[nm]], error = function(e) NULL)
      if (is.null(cur)) { ok <- FALSE; break }
    }
    if (ok && is.numeric(cur) && length(cur) == 1) return(as.numeric(cur))
  }
  # As a last resort, scan recursively for numeric scalar named 'adj_rand'
  scan_adj <- function(x) {
    if (is.list(x)) {
      for (nm in names(x)) {
        if (identical(nm, "adj_rand") && is.numeric(x[[nm]]) && length(x[[nm]]) == 1)
          return(as.numeric(x[[nm]]))
        val <- scan_adj(x[[nm]])
        if (!is.null(val)) return(val)
      }
    }
    NULL
  }
  scan_adj(res)
}

# ------------------------------
# Core runner: with user weights (uses native or fallback weighting)
# ------------------------------
run_with_weights <- function(data, truth, fields, weights, id_col = "id",
                             k_grid = seq(10, 300, by = 10), base_rep = 12L,
                             write_csv = NULL, er_method = c("kmeans","gc","louvain"),
                             save_pdf = NULL, pdf_title = "ER Report") {
  er_method <- match.arg(er_method)
  w_norm <- normalize_weights(weights, fields)

  # Try native field_weights first
  res <- NULL
  used_native <- FALSE
  try({
    res <- er_main(
      data = data, truth = truth, fields = fields,
      field_weights = w_norm, k_grid = k_grid, write_csv = write_csv
    )
    used_native <- TRUE
  }, silent = TRUE)

  # Fallback: expand fields by weights and call er_main
  if (is.null(res)) {
    expanded_fields <- expand_fields_by_weights(fields, w_norm, base_rep = base_rep)
    res <- er_main(
      data = data, truth = truth, fields = expanded_fields,
      k_grid = k_grid, write_csv = write_csv
    )
  } else {
    attr(res, "weights_used") <- w_norm
  }

  if (!is.null(save_pdf)) {
    try(er_save_report_pdf(res, save_pdf, pdf_title), silent = TRUE)
  }

  ari <- extract_adj_rand(res, method = er_method)
  list(result = res, weights = w_norm, method = er_method, adj_rand = ari, native_weights = used_native)
}

# ------------------------------
# CV Weight Learning (supervised by ARI)
# ------------------------------
learn_best_weights_cv <- function(data, truth, fields, id_col = "id",
                                  folds = 3, budget = 20,
                                  candidate_weights = NULL, # list of numeric named vectors
                                  dirichlet_alpha = NULL,   # numeric vector named by fields
                                  base_rep = 12L,
                                  k_grid = seq(10, 300, by = 10),
                                  max_records_per_fold = NULL,
                                  er_method = c("kmeans","gc","louvain"),
                                  seed = 42, verbose = TRUE) {
  er_method <- match.arg(er_method)
  set.seed(seed)

  # Prepare candidates
  if (is.null(candidate_weights)) {
    if (is.null(dirichlet_alpha)) dirichlet_alpha <- rep(1, length(fields))
    names(dirichlet_alpha) <- fields
    ws <- rdirichlet_simple(budget, dirichlet_alpha)
    candidate_weights <- lapply(seq_len(nrow(ws)), function(i) {
      w <- ws[i, ]
      names(w) <- colnames(ws)
      w
    })
  }

  # Build CV folds as splits of IDs
  all_ids <- if (id_col %in% names(data)) data[[id_col]] else seq_len(nrow(data))
  all_ids <- as.character(all_ids)
  n <- length(all_ids)
  fold_ids <- split(all_ids, sort.int(seq_len(n) %% folds))

  eval_table <- dplyr::tibble()

  # Evaluate each weight vector
  for (ci in seq_along(candidate_weights)) {
    w <- candidate_weights[[ci]]
    w_norm <- normalize_weights(w, fields)

    ari_folds <- c()
    for (fi in seq_along(fold_ids)) {
      ids_fold <- fold_ids[[fi]]
      if (!is.null(max_records_per_fold) && length(ids_fold) > max_records_per_fold) {
        ids_fold <- sample(ids_fold, max_records_per_fold)
      }
      dat_fold <- subset_data_ids(data, ids_fold, id_col = id_col)
      tru_fold <- subset_truth(truth, ids_fold, id_col = id_col)

      res <- try(run_with_weights(dat_fold, tru_fold, fields, w_norm, id_col = id_col,
                                  k_grid = k_grid, base_rep = base_rep, write_csv = NULL,
                                  er_method = er_method, save_pdf = NULL), silent = TRUE)
      ari <- if (inherits(res, "try-error")) NA_real_ else res$adj_rand
      ari_folds <- c(ari_folds, ari)
      if (verbose) cat(sprintf("Candidate %d fold %d: ARI=%.4f\n", ci, fi, ifelse(is.na(ari), NaN, ari)))
    }

    mean_ari <- mean(ari_folds, na.rm = TRUE)
    eval_table <- bind_rows(eval_table,
                            tibble(candidate = ci, mean_ari = mean_ari,
                                   weights = list(w_norm), ari_folds = list(ari_folds)))
  }

  # Pick the best weights
  best_row <- eval_table %>% arrange(desc(mean_ari)) %>% slice(1)
  best_weights <- best_row$weights[[1]]

  list(best_weights = best_weights, evaluations = eval_table)
}

# ------------------------------
# One-stop pipeline
# ------------------------------
er_general_pipeline <- function(data, truth, id_col = "id",
                                fields = setdiff(names(data), id_col),
                                weights = NULL,              # optional user weights (named numeric)
                                learn_weights = FALSE,       # set TRUE to learn via CV
                                folds = 3, budget = 20,      # for learning
                                dirichlet_alpha = NULL,      # for random search
                                base_rep = 12L,
                                er_method = c("kmeans","gc","louvain"),
                                k_grid = seq(10, 300, by = 10),
                                write_csv = NULL, save_pdf = NULL, pdf_title = "ER Report",
                                max_records_per_fold = NULL,
                                seed = 42, verbose = TRUE) {
  er_method <- match.arg(er_method)

  # Optionally learn weights
  learned <- NULL
  if (isTRUE(learn_weights) || is.null(weights)) {
    if (verbose) cat("[er_general_pipeline] Learning weights via CV...\n")
    learned <- learn_best_weights_cv(
      data = data, truth = truth, fields = fields, id_col = id_col,
      folds = folds, budget = budget, dirichlet_alpha = dirichlet_alpha,
      base_rep = base_rep, k_grid = k_grid, max_records_per_fold = max_records_per_fold,
      er_method = er_method, seed = seed, verbose = verbose
    )
    weights <- learned$best_weights
    if (verbose) cat(sprintf("[er_general_pipeline] Best weights: %s\n",
                             paste(sprintf("%s=%.3f", names(weights), weights), collapse=", ")))
  } else {
    # Normalize user weights
    weights <- normalize_weights(weights, fields)
  }

  # Final full run with selected weights
  if (verbose) cat("[er_general_pipeline] Final run with selected weights...\n")
  final <- run_with_weights(
    data = data, truth = truth, fields = fields, weights = weights, id_col = id_col,
    k_grid = k_grid, base_rep = base_rep, write_csv = write_csv,
    er_method = er_method, save_pdf = save_pdf, pdf_title = pdf_title
  )

  list(final = final, learned = learned, fields = fields, weights = weights)
}

# =====================================================================
# USAGE EXAMPLES (Scroll here)
# =====================================================================
# Assume you have objects: `cora` (data frame), `cora_gold` (truth),
# and that er_main() / er_save_report_pdf() are available.
#
# 1) User-specified weights (must sum to 1; if not, they'll be normalized)
# fields_use <- c("title","authors","address","year_new")
# user_w     <- c(title=0.45, authors=0.35, address=0.15, year_new=0.05)
# out <- er_general_pipeline(
#   data = cora, truth = cora_gold, id_col = "id",
#   fields = fields_use, weights = user_w, learn_weights = FALSE,
#   er_method = "kmeans", k_grid = seq(10, 300, by=10), base_rep = 12,
#   write_csv = "D:/erbot/data/cora_pred_weighted.csv",
#   save_pdf  = "D:/erbot/results/cora_report_weighted.pdf",
#   pdf_title = "CORA — Weighted Fields"
# )
# out$final$adj_rand  # ARI of the final run
# out$final$weights   # normalized weights actually used
#
# 2) Learn best weights via CV (random Dirichlet search)
# fields_use <- c("title","authors","address","year_new")
# out_cv <- er_general_pipeline(
#   data = cora, truth = cora_gold, id_col = "id",
#   fields = fields_use, weights = NULL, learn_weights = TRUE,
#   folds = 3, budget = 12, dirichlet_alpha = rep(1, length(fields_use)),
#   er_method = "kmeans", k_grid = seq(10, 300, by=10), base_rep = 12,
#   max_records_per_fold = 800, # optional speed cap per fold
#   write_csv = "D:/erbot/data/cora_pred_learned.csv",
#   save_pdf  = "D:/erbot/results/cora_report_learned.pdf",
#   pdf_title = "CORA — Learned Weights"
# )
# out_cv$weights                     # best weights selected
# out_cv$final$adj_rand              # ARI on full data with learned weights
# out_cv$learned$evaluations         # table of candidates and fold ARIs
#
# 3) GC or Louvain as the scoring method
# out_gc <- er_general_pipeline(
#   data = cora, truth = cora_gold, id_col = "id",
#   fields = fields_use, weights = user_w, learn_weights = FALSE,
#   er_method = "gc", k_grid = seq(10, 300, by=10), base_rep = 12
# )
# out_gc$final$adj_rand
#
# Tips:
# - Increase `budget` and `folds` for better weight search at the cost of time.
# - If er_main supports `field_weights`, the pipeline uses it automatically; otherwise,
#   it falls back to field expansion to approximate weights in TF-IDF pipelines.
# - Use `max_records_per_fold` to keep CV manageable on big datasets.
# =====================================================================
