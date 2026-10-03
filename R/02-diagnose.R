########################################
# File: R/02-diagnose.R
# Field diagnosis: type detection, missingness, cardinality, blocking and
# similarity recommendations.  Called automatically by er_run() but also
# exported for expert inspection.
########################################

# ── Truth / label column guard (BUG-22 fix, 2026-06-12) ───────────────────────

# Column names that encode ground truth (entity / cluster / class / match id).
# These must NEVER enter the similarity pipeline: a label column scores a
# perfect similarity on every true match, leaking the answer into the features.
# See BUG22_LEAKAGE_ANALYSIS.md.
.ER_LABEL_COLS <- c(
  "cluster_id", "class", "entity_id", "label", "truth",
  "gold", "gold_id", "match_id",
  # common synonyms seen in ER benchmarks
  "cluster", "entityid", "clusterid", "true_id", "trueid",
  "gold_cluster", "gold_label", "ground_truth", "groundtruth", "y", "target"
)

#' Is a column name a ground-truth / label column?
#'
#' Case-insensitive, punctuation-insensitive match against a curated list of
#' label-column names (\code{.ER_LABEL_COLS}).  Used by \code{er_diagnose()}
#' to keep truth columns out of the similarity feature set (BUG-22).
#'
#' @param name Character vector of column names.
#' @param extra Optional character vector of additional label names to treat
#'   as truth columns.
#' @return Logical vector, same length as \code{name}.
#' @keywords internal
.er_is_label_column <- function(name, extra = character(0)) {
  norm <- function(x) gsub("[^a-z0-9]", "", tolower(as.character(x)))
  targets <- norm(c(.ER_LABEL_COLS, extra))
  norm(name) %in% targets
}

# ── Field type detection ───────────────────────────────────────────────────────

#' Detect the type of a single column
#'
#' Heuristics (in priority order):
#' \enumerate{
#'   \item If the column is numeric/integer and its value range is \eqn{\le 200}
#'     → \code{"year"}.
#'   \item If the column is numeric → \code{"numeric"}.
#'   \item If character/factor with cardinality ratio \eqn{< 0.05}
#'     → \code{"categorical"}.
#'   \item If character/factor with average token count \eqn{\le 4}
#'     → \code{"text_short"}.
#'   \item Otherwise → \code{"text_long"}.
#' }
#'
#' @param col A single vector (one column of a data.frame).
#' @param n Integer. Number of records (for cardinality ratio).
#' @return One of \code{"year"}, \code{"numeric"}, \code{"categorical"},
#'   \code{"text_short"}, \code{"text_long"}.
#' @keywords internal
.er_detect_field_type <- function(col, n) {
  if (is.numeric(col) || is.integer(col)) {
    vals <- col[!is.na(col) & is.finite(col)]
    if (length(vals) >= 2) {
      rng  <- diff(range(vals))
      # integers in a narrow range look like years
      if (all(vals == round(vals)) && rng <= 200 && rng >= 0) return("year")
    }
    return("numeric")
  }
  # character / factor
  col_c <- as.character(col)
  n_unique <- length(unique(col_c[!is.na(col_c) & col_c != ""]))
  card_ratio <- n_unique / max(n, 1L)
  if (card_ratio < 0.05) return("categorical")
  # average token count
  sample_c <- col_c[!is.na(col_c) & col_c != ""]
  if (!length(sample_c)) return("text_short")
  avg_tok <- mean(lengths(strsplit(sample_c[seq_len(min(500L, length(sample_c)))],
                                   "\\s+")))
  if (avg_tok <= 4) "text_short" else "text_long"
}

# ── Recommended similarity per type ───────────────────────────────────────────

#' Map a field type to its recommended similarity method
#'
#' @param field_type Character; one of the types returned by
#'   \code{.er_detect_field_type()}.
#' @return Character scalar naming the similarity method.
#' @keywords internal
.er_recommend_sim <- function(field_type) {
  switch(field_type,
         year        = "year",
         numeric     = "numeric",
         categorical = "categorical",
         text_short  = "jw",
         text_long   = "jw",   # BoW not yet implemented; fall back to jw
         "jw")
}

# ── Main er_diagnose() ─────────────────────────────────────────────────────────

#' Diagnose a dataset for entity resolution
#'
#' Inspects each column and returns field-level metadata used by \code{er_block()}
#' and \code{er_similarity()} to make automatic decisions.
#'
#' @param data A \code{data.frame} (already loaded by \code{er_load()}).
#' @param id_col Character. Name of the ID column. \code{NULL} triggers
#'   auto-detection from \code{id_candidates}.
#' @param text_cols Character vector of columns to analyse. \code{NULL} means
#'   analyse all non-ID, non-embedding columns.
#' @param source_col Character. Name of the source-ID column (present in
#'   linkage tasks). \code{NULL} for auto-detection.
#' @param id_candidates Character vector of column names tried for ID
#'   auto-detection.
#' @param embed_candidates Character vector of embedding column name patterns to
#'   exclude from analysis.
#' @param pair_budget Numeric. If estimated candidate pairs exceed this, blocking
#'   is flagged as required. Default \code{1e6}.
#'
#' @return A named list:
#' \describe{
#'   \item{\code{fields}}{A \code{tibble} with one row per analysed column and
#'     columns: \code{name}, \code{type}, \code{missingness}, \code{cardinality_ratio},
#'     \code{avg_tokens}, \code{sim_method}, \code{blocking_candidate}.}
#'   \item{\code{id_col}}{Detected (or supplied) ID column name.}
#'   \item{\code{source_col}}{Source-ID column if detected, else \code{NULL}.}
#'   \item{\code{mode}}{Inferred ER mode: \code{"dedup"} or \code{"link"}.}
#'   \item{\code{n}}{Number of records.}
#'   \item{\code{estimated_pairs}}{Estimated candidate pairs under no blocking.}
#'   \item{\code{blocking_needed}}{Logical; TRUE when pairs exceed
#'     \code{pair_budget}.}
#'   \item{\code{recommended_block_method}}{One of \code{"none"},
#'     \code{"standard"}, \code{"prefix"}, \code{"sn"}.}
#'   \item{\code{recommended_block_key}}{Suggested blocking key column.}
#' }
#' @export
er_diagnose <- function(data,
                        id_col          = NULL,
                        text_cols       = NULL,
                        source_col      = NULL,
                        id_candidates   = c("id", "affiliation_id", "record_id",
                                            "rec_id", "docid", "rowid",
                                            "paper_id"),
                        embed_candidates = c("embedded clean ag.value",
                                             "embedded ag.value", "emb",
                                             "embedding", "vector",
                                             "embedding_clean"),
                        label_cols      = NULL,
                        pair_budget     = 1e6) {

  df <- tibble::as_tibble(data)
  names(df) <- tolower(names(df))
  n <- nrow(df)

  # ── Detect ID column ─────────────────────────────────────────────────────────
  if (is.null(id_col)) {
    id_col <- intersect(id_candidates, names(df))[1]
    if (is.na(id_col)) {
      df[[".__id__"]] <- as.character(seq_len(n))
      id_col <- ".__id__"
    }
  } else {
    id_col <- tolower(id_col)
  }

  # ── Detect source column (linkage mode) ──────────────────────────────────────
  if (is.null(source_col)) {
    source_cands <- c("source_id", "source", "dataset", "file_id")
    source_col   <- intersect(source_cands, names(df))[1]
    if (is.na(source_col)) source_col <- NULL
  } else {
    source_col <- tolower(source_col)
  }
  mode <- if (!is.null(source_col)) "link" else "dedup"

  # ── Columns to analyse ───────────────────────────────────────────────────────
  exclude <- union(tolower(c(id_candidates, embed_candidates)),
                   c(id_col, source_col %||% character(0)))
  if (is.null(text_cols)) {
    analyse_cols <- setdiff(names(df), exclude)
  } else {
    analyse_cols <- intersect(tolower(text_cols), names(df))
  }

  # ── BUG-22 guard: never let a ground-truth / label column become a feature ────
  # A label column (cluster_id, class, entity_id, ...) scores a perfect
  # similarity on every true match, leaking the answer into the similarity
  # pipeline. Drop such columns here regardless of how analyse_cols was built,
  # and report them so the leak is visible. See BUG22_LEAKAGE_ANALYSIS.md.
  is_label    <- .er_is_label_column(analyse_cols, extra = tolower(label_cols %||% character(0)))
  label_drops <- analyse_cols[is_label]
  if (length(label_drops)) {
    warning("er_diagnose: dropping ground-truth/label column(s) from the ",
            "similarity field set to prevent leakage (BUG-22): ",
            paste(label_drops, collapse = ", "),
            ". Pass label_cols= to extend the list, or text_cols= to control ",
            "the field set explicitly.")
    analyse_cols <- analyse_cols[!is_label]
  }

  if (!length(analyse_cols)) {
    warning("er_diagnose: no columns to analyse. Check id_col / text_cols.")
    analyse_cols <- character(0)
  }

  # ── Per-field analysis ───────────────────────────────────────────────────────
  field_rows <- lapply(analyse_cols, function(cn) {
    col <- df[[cn]]
    n_na  <- sum(is.na(col) | (is.character(col) & col == ""))
    miss  <- n_na / n

    ftype <- .er_detect_field_type(col, n)

    col_c     <- as.character(col)
    n_unique  <- length(unique(col_c[!is.na(col_c) & col_c != ""]))
    card_ratio <- n_unique / max(n, 1L)

    sample_c <- col_c[!is.na(col_c) & col_c != ""]
    avg_tok  <- if (length(sample_c))
      mean(lengths(strsplit(sample_c[seq_len(min(500L, length(sample_c)))],
                            "\\s+")))
    else
      0

    sim_method <- .er_recommend_sim(ftype)

    # good blocking candidate: low missingness, categorical or short-text
    block_cand <- miss < 0.15 && ftype %in% c("categorical", "text_short")

    list(name              = cn,
         type              = ftype,
         missingness       = round(miss, 4),
         cardinality_ratio = round(card_ratio, 4),
         avg_tokens        = round(avg_tok, 2),
         sim_method        = sim_method,
         blocking_candidate = block_cand)
  })

  fields_tbl <- if (length(field_rows))
    tibble::as_tibble(do.call(rbind, lapply(field_rows, function(r) {
      tibble::tibble(
        name               = r$name,
        type               = r$type,
        missingness        = r$missingness,
        cardinality_ratio  = r$cardinality_ratio,
        avg_tokens         = r$avg_tokens,
        sim_method         = r$sim_method,
        blocking_candidate = r$blocking_candidate
      )
    })))
  else
    tibble::tibble(name = character(), type = character(),
                   missingness = numeric(), cardinality_ratio = numeric(),
                   avg_tokens = numeric(), sim_method = character(),
                   blocking_candidate = logical())

  # ── Blocking recommendation ──────────────────────────────────────────────────
  est_pairs      <- choose(n, 2)
  blocking_needed <- est_pairs > pair_budget

  # Prefer categorical blocking key with lowest missingness
  block_cands <- fields_tbl[fields_tbl$blocking_candidate, , drop = FALSE]
  if (nrow(block_cands)) {
    # prefer categorical > text_short; then sort by missingness
    block_cands <- block_cands[order(
      block_cands$type != "categorical",
      block_cands$missingness
    ), , drop = FALSE]
    rec_block_key    <- block_cands$name[1]
    rec_block_type   <- block_cands$type[1]
    rec_block_method <- if (rec_block_type == "categorical") "standard" else "prefix"
  } else {
    # no obvious key: fall back to sorted-neighborhood on any text field
    text_f <- fields_tbl[fields_tbl$type %in% c("text_short", "text_long"), , drop = FALSE]
    if (nrow(text_f)) {
      rec_block_key    <- text_f$name[1]
      rec_block_method <- "sn"
    } else {
      rec_block_key    <- NULL
      rec_block_method <- if (blocking_needed) "sn" else "none"
    }
  }

  if (!blocking_needed) rec_block_method <- "none"

  list(
    fields                   = fields_tbl,
    id_col                   = id_col,
    source_col               = source_col,
    mode                     = mode,
    n                        = n,
    estimated_pairs          = est_pairs,
    blocking_needed          = blocking_needed,
    recommended_block_method = rec_block_method,
    recommended_block_key    = rec_block_key,
    label_dropped            = label_drops   # BUG-22: truth columns removed
  )
}

#' Print an \code{er_diagnose} result
#'
#' @param x Named list returned by \code{\link{er_diagnose}()}.
#' @param ... Currently unused; for S3 method consistency.
#' @return \code{x} invisibly.
#' @export
print.er_diagnose <- function(x, ...) {
  cat(sprintf(
    "er_diagnose: n=%d, mode=%s, estimated_pairs=%s, blocking_needed=%s\n",
    x$n, x$mode,
    format(x$estimated_pairs, big.mark = ","),
    x$blocking_needed
  ))
  cat(sprintf("  id_col: %s\n", x$id_col))
  if (!is.null(x$source_col))
    cat(sprintf("  source_col: %s\n", x$source_col))
  cat(sprintf("  recommended_block: method=%s, key=%s\n",
              x$recommended_block_method,
              x$recommended_block_key %||% "none"))
  cat("\nField summary:\n")
  print(x$fields, n = Inf)
  invisible(x)
}
