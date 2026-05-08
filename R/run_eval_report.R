########################################
# File: R/run_eval_report.R
# Driver: runs Protocol A + Protocol B on all four benchmark datasets
# and assembles the delta-ARI comparison table.
#
# Usage (from project root):
#   Rscript R/run_eval_report.R
#
# Output (written to results/eval_report/):
#   eval_summary.csv   -- one row per dataset × protocol
#   delta_table.csv    -- one row per dataset: ARI_A, ARI_B, delta
#   sweep_<ds>.csv     -- full Protocol A sweep for each dataset
#   fold_best_<ds>.csv -- Protocol B per-fold best params for each dataset
#   eval_results.rds   -- full R object (list of results)
########################################

if (identical(environmentName(topenv()), "R_GlobalEnv")) {

setwd("D:/ResearchHub/01_projects/erbot")

# ── Load package or source all R files ────────────────────────────────────────
if (requireNamespace("erbot", quietly = TRUE)) {
  library(erbot)
} else {
  # Dev mode: source all R files in order
  r_files <- sort(list.files("R", pattern = "\\.R$", full.names = TRUE))
  # Source dependencies first, protocols last
  core <- r_files[!grepl("12-cv_protocols|run_eval_report", r_files)]
  for (f in core) source(f, local = FALSE)
  source("R/12-cv_protocols.R", local = FALSE)

  # Dev-mode shim: register a minimal "erbot" namespace so that
  # get("%||%", asNamespace("erbot")) calls inside the sourced functions work.
  if (!isNamespaceLoaded("erbot")) {
    .erbot_ns <- new.env(parent = globalenv())
    # Copy all currently defined objects into the fake namespace
    for (.nm in ls(globalenv(), all.names = TRUE))
      tryCatch(assign(.nm, get(.nm, envir = globalenv()), envir = .erbot_ns),
               error = function(e) NULL)
    .erbot_meta <- new.env(parent = emptyenv())
    .erbot_meta$spec    <- c(name = "erbot", version = "0.0.1")
    .erbot_meta$exports <- character()
    .erbot_meta$imports <- list()
    .erbot_meta$DLLs    <- list()
    assign(".__NAMESPACE__.", .erbot_meta, envir = .erbot_ns)
    .Internal(registerNamespace("erbot", .erbot_ns))
    rm(.erbot_ns, .erbot_meta, .nm)
  }
}

library(data.table)
library(magrittr)    # %>%  used inside er_truth_from_any and other helpers

OUT_DIR <- "results/eval_report"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ── Parameter grid ─────────────────────────────────────────────────────────────
METHOD_GRID <- c("threshold_cc", "louvain", "leiden",
                 "label_prop", "hclust_avg", "field_ensemble")
TAU_GRID    <- c(0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)
N_FOLDS     <- 5L
BASE_SEED   <- 42L


# ── Dataset preparation helpers ────────────────────────────────────────────────

# Run stages 1-4 of erbot and return internals for the sweep engine.
# Returns list(sim_list, pairs, n, truth_tbl, id_vec).
.prepare_cora <- function() {
  here       <- getwd()
  data_path  <- file.path(here, "data", "cora.csv")
  truth_path <- file.path(here, "data", "cora_gold.csv")
  if (!file.exists(data_path))  stop("CORA data not found: ", data_path)
  if (!file.exists(truth_path)) stop("CORA gold not found: ", truth_path)

  df      <- er_load(data_path)
  n       <- nrow(df)
  diag_   <- er_diagnose(df, text_cols = c("title","authors","address",
                                           "booktitle","journal","year"))
  id_vec  <- as.character(df[[diag_$id_col]])
  pairs   <- er_block(df, diag = diag_, method = "none")
  sim_list <- er_similarity(df, pairs, diag = diag_)

  gold_raw  <- data.table::fread(truth_path, sep = ";", showProgress = FALSE)
  names(gold_raw) <- tolower(names(gold_raw))
  truth_tbl <- er_truth_from_any(tibble::as_tibble(gold_raw))
  if (is.null(truth_tbl)) stop("CORA: ground truth not found.")

  list(sim_list = sim_list, pairs = pairs, n = n,
       truth_tbl = truth_tbl, id_vec = id_vec)
}

.prepare_affiliation <- function() {
  here       <- getwd()
  data_path  <- file.path(here, "data", "affiliationstrings_ids.csv")
  truth_path <- file.path(here, "data", "affiliationstrings_mapping.csv")
  if (!file.exists(data_path))  stop("Affiliation data not found: ", data_path)
  if (!file.exists(truth_path)) stop("Affiliation truth not found: ", truth_path)

  df       <- er_load(data_path)
  # Raw file has id1/affil1 — rename to id/affiliation
  if ("id1"    %in% names(df) && !"id"          %in% names(df))
    names(df)[names(df) == "id1"]    <- "id"
  if ("affil1" %in% names(df))
    names(df)[names(df) == "affil1"] <- "affiliation"
  n        <- nrow(df)
  diag_    <- er_diagnose(df, text_cols = "affiliation")
  id_vec   <- as.character(df[[diag_$id_col]])
  pairs    <- er_block(df, diag = diag_,
                       method = "sn", block_key = "affiliation", sn_window = 10L)
  sim_list <- er_similarity(df, pairs, diag = diag_)
  # affiliationstrings_mapping.csv is (entity_id, record_id) — not a pair list.
  # V1 = entity/cluster ID, V2 = record ID (matching id column in df).
  map_raw   <- data.table::fread(truth_path, showProgress = FALSE, header = FALSE,
                                  col.names = c("cluster_id", "record_id"))
  truth_tbl <- tibble::tibble(id         = as.character(map_raw$record_id),
                              cluster_id = as.integer(map_raw$cluster_id))

  list(sim_list = sim_list, pairs = pairs, n = n,
       truth_tbl = truth_tbl, id_vec = id_vec)
}

.prepare_d10k <- function() {
  here       <- getwd()
  rec_path   <- file.path(here, "data", "10Kfull.csv")
  dup_path   <- file.path(here, "data", "10Kduplicates.csv")
  if (!file.exists(rec_path)) stop("D10K records not found: ", rec_path)
  if (!file.exists(dup_path)) stop("D10K duplicates not found: ", dup_path)

  # Load only the two text columns; embedding/numeric columns cause problems
  df_raw <- tryCatch(
    data.table::fread(rec_path, sep = "|",
                      select = c("Id", "Aggregate Value"),
                      showProgress = FALSE),
    error = function(e)
      data.table::fread(rec_path, sep = "|", select = c(1L, 2L),
                        showProgress = FALSE)
  )
  df <- tibble::as_tibble(df_raw)
  names(df) <- tolower(gsub("[[:space:]]+", "_", trimws(names(df))))
  if ("aggregate_value" %in% names(df))
    names(df)[names(df) == "aggregate_value"] <- "text"
  n        <- nrow(df)
  diag_    <- er_diagnose(df, text_cols = "text")
  id_vec   <- as.character(df[[diag_$id_col]])
  pairs    <- er_block(df, diag = diag_,
                       method = "sn", block_key = "text", sn_window = 40L)
  sim_list <- er_similarity(df, pairs, diag = diag_)

  # D10K truth: Entity1|Entity2 pair list -> cluster labels
  dup_df   <- data.table::fread(dup_path, showProgress = FALSE)
  names(dup_df) <- tolower(names(dup_df))
  if (!all(c("id1","id2") %in% names(dup_df)))
    names(dup_df)[1:2] <- c("id1", "id2")
  truth_tbl <- er_truth_from_any(tibble::as_tibble(dup_df))

  list(sim_list = sim_list, pairs = pairs, n = n,
       truth_tbl = truth_tbl, id_vec = id_vec)
}

.prepare_ncvr5 <- function(n_sample = 30000L, seed = 42L) {
  here      <- getwd()
  ncvr_root <- file.path(here, "data")
  df_full   <- ncvr_read(ncvr_root, which = "5")
  # In the 5-party NCVR benchmark, recid is the original voter ID:
  # records sharing the same recid across partitions are true duplicates.
  # Add a unique row-level id and use recid as entity_id.
  df_full[["row_id"]]    <- seq_len(nrow(df_full))
  df_full[["entity_id"]] <- as.integer(df_full[["recid"]])

  # Subsample to keep computation tractable; oversample matches so that
  # duplicates are well-represented.
  set.seed(seed)
  recid_counts <- table(df_full[["recid"]])
  dup_recids   <- names(recid_counts[recid_counts > 1L])
  dup_rows  <- which(df_full[["recid"]] %in% dup_recids)
  sing_rows <- which(!df_full[["recid"]] %in% dup_recids)
  n_dup  <- min(length(dup_rows),  round(n_sample * 0.5))
  n_sing <- min(length(sing_rows), n_sample - n_dup)
  keep   <- c(sample(dup_rows, n_dup), sample(sing_rows, n_sing))
  df     <- df_full[keep, ]

  fields  <- ncvr_guess_fields(df)
  if (!length(fields)) stop("NCVR5: could not detect ER fields.")
  n       <- nrow(df)
  id_vec  <- as.character(df[["row_id"]])
  diag_   <- er_diagnose(df, id_col = "row_id", text_cols = fields)
  pairs   <- er_block(df, diag = diag_, method = "prefix",
                      block_key = fields[1], prefix_len = 3L,
                      max_pairs = 2e6)
  sim_list  <- er_similarity(df, pairs, diag = diag_)
  truth_tbl <- tibble::tibble(id         = id_vec,
                              cluster_id = as.integer(df[["entity_id"]]))

  list(sim_list = sim_list, pairs = pairs, n = n,
       truth_tbl = truth_tbl, id_vec = id_vec)
}


# ── Dataset registry ───────────────────────────────────────────────────────────
DATASETS <- list(
  CORA        = .prepare_cora,
  Affiliation = .prepare_affiliation,
  Synth10K    = .prepare_d10k,
  NCVR5       = .prepare_ncvr5
)


# ── Main loop ──────────────────────────────────────────────────────────────────
all_results <- list()

for (ds_name in names(DATASETS)) {
  cat(sprintf("\n====== Dataset: %s ======\n", ds_name))

  prep <- tryCatch(DATASETS[[ds_name]](), error = function(e) {
    message("  FAILED to prepare dataset: ", e$message)
    NULL
  })
  if (is.null(prep)) next

  cat(sprintf("  n=%d records, %d pairs, %d fields\n",
              prep$n, nrow(prep$pairs), length(prep$sim_list)))

  # Protocol A
  cat("  Running Protocol A (full-data sweep)...\n")
  t0 <- proc.time()[["elapsed"]]
  pa <- tryCatch(
    er_protocol_a(prep$sim_list, prep$pairs, prep$n,
                  prep$truth_tbl, prep$id_vec,
                  method_grid   = METHOD_GRID,
                  tau_grid      = TAU_GRID,
                  weight_method = "ari",
                  seed          = BASE_SEED,
                  verbose       = TRUE),
    error = function(e) { message("  Protocol A error: ", e$message); NULL }
  )
  cat(sprintf("  Protocol A done in %.1f min\n",
              (proc.time()[["elapsed"]] - t0) / 60))

  # Protocol B
  cat("  Running Protocol B (CV5 tune -> refit)...\n")
  t0 <- proc.time()[["elapsed"]]
  pb <- tryCatch(
    er_protocol_b(prep$sim_list, prep$pairs, prep$n,
                  prep$truth_tbl, prep$id_vec,
                  method_grid   = METHOD_GRID,
                  tau_grid      = TAU_GRID,
                  weight_method = "ari",
                  n_folds       = N_FOLDS,
                  base_seed     = BASE_SEED,
                  verbose       = TRUE),
    error = function(e) { message("  Protocol B error: ", e$message); NULL }
  )
  cat(sprintf("  Protocol B done in %.1f min\n",
              (proc.time()[["elapsed"]] - t0) / 60))

  all_results[[ds_name]] <- list(dataset = ds_name, A = pa, B = pb, prep = prep)

  # Save sweep files
  if (!is.null(pa))
    write.csv(pa$sweep,
              file.path(OUT_DIR, paste0("sweep_", tolower(ds_name), ".csv")),
              row.names = FALSE)
  if (!is.null(pb))
    write.csv(pb$fold_best,
              file.path(OUT_DIR, paste0("fold_best_", tolower(ds_name), ".csv")),
              row.names = FALSE)
}

# Save full results object
saveRDS(all_results, file.path(OUT_DIR, "eval_results.rds"))


# ── Assemble summary tables ────────────────────────────────────────────────────

summary_rows <- list()
delta_rows   <- list()

for (ds_name in names(all_results)) {
  res <- all_results[[ds_name]]
  pa  <- res$A
  pb  <- res$B
  n   <- res$prep$n

  if (!is.null(pa)) {
    summary_rows[[length(summary_rows) + 1L]] <- data.frame(
      dataset      = ds_name,
      protocol     = "A (full-data best)",
      best_method  = pa$best_method,
      best_tau     = pa$best_tau,
      ARI          = pa$ARI,
      Bcubed_F     = pa$Bcubed_F,
      Vmeasure     = pa$Vmeasure,
      n_records    = n,
      stringsAsFactors = FALSE
    )
  }
  if (!is.null(pb)) {
    summary_rows[[length(summary_rows) + 1L]] <- data.frame(
      dataset      = ds_name,
      protocol     = "B (CV5 refit)",
      best_method  = pb$consensus_method,
      best_tau     = pb$consensus_tau,
      ARI          = pb$ARI,
      Bcubed_F     = pb$Bcubed_F,
      Vmeasure     = pb$Vmeasure,
      n_records    = n,
      stringsAsFactors = FALSE
    )
  }
  if (!is.null(pa) && !is.null(pb)) {
    delta <- er_delta_ari(pa, pb)
    delta_rows[[length(delta_rows) + 1L]] <- data.frame(
      dataset         = ds_name,
      ARI_A           = pa$ARI,
      ARI_B           = pb$ARI,
      delta_ARI       = delta["delta_ARI"],
      method_A        = pa$best_method,
      tau_A           = pa$best_tau,
      method_B        = pb$consensus_method,
      tau_B           = pb$consensus_tau,
      Bcubed_F_A      = pa$Bcubed_F,
      Bcubed_F_B      = pb$Bcubed_F,
      stringsAsFactors = FALSE
    )
  }
}

eval_summary <- do.call(rbind, summary_rows)
delta_table  <- do.call(rbind, delta_rows)

write.csv(eval_summary, file.path(OUT_DIR, "eval_summary.csv"), row.names = FALSE)
write.csv(delta_table,  file.path(OUT_DIR, "delta_table.csv"),  row.names = FALSE)

# ── Print delta table to console ───────────────────────────────────────────────
cat("\n\n========== DELTA ARI TABLE ==========\n")
cat(sprintf("%-14s  %6s  %6s  %8s  %-14s  %-14s\n",
            "Dataset", "ARI_A", "ARI_B", "delta", "Method_A", "Method_B"))
cat(strrep("-", 72), "\n")
for (i in seq_len(nrow(delta_table))) {
  r <- delta_table[i, ]
  cat(sprintf("%-14s  %6.4f  %6.4f  %+8.4f  %-14s  %-14s\n",
              r$dataset, r$ARI_A, r$ARI_B, r$delta_ARI, r$method_A, r$method_B))
}
cat(strrep("=", 72), "\n")
cat(sprintf("\nResults written to: %s\n", normalizePath(OUT_DIR)))
}
