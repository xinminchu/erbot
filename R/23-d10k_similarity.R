########################################
# File: R/23-d10k_similarity.R
# D10K synthetic similarity layer: parse the "Aggregate Value" column into
# field-like columns and build per-field similarities on top.
#
# Current state of the repo: er_load("d10k") keeps only Id + Aggregate Value
# (renamed to `text`), because the raw file is pipe-delimited and carries
# multi-line embedding columns. That is enough for one text similarity, but
# not for a field-wise similarity matrix.
#
# This file adds the missing layer WITHOUT pretending the raw file was
# inspected here (data/10Kfull.csv is not in this worktree):
#   * er_parse_d10k_aggregate() -- parse the Aggregate Value string into
#     field-like columns when it carries structure (key=value pairs or a
#     consistent separator); otherwise keep a single `text` field.
#   * er_d10k_similarity_spec() -- build an er_similarity() field spec for
#     the parsed columns.
#   * er_d10k_similarity()      -- block (if needed), compute per-field
#     similarities, combine them, and return the sparse similarity matrix S.
#
# Missing values are never imputed: a missing field stays NA and is handled
# by er_combine()'s NA-aware weighting, exactly like every other dataset.
########################################

#' Parse the D10K "Aggregate Value" column into field-like columns
#'
#' The parser is deliberately conservative. It only splits the aggregate
#' string when the structure is evident across most records:
#' \itemize{
#'   \item \code{key=value} pairs separated by \code{;} or \code{|}, or
#'   \item a single consistent separator producing the same field count in
#'     at least 80\% of non-empty records.
#' }
#' If neither holds, the value is returned unchanged as a single \code{text}
#' column. Nothing is invented: no field names are guessed for unstructured
#' text.
#'
#' @param x Either a character vector of Aggregate Value strings, or a
#'   data.frame with a \code{text} (or \code{aggregate_value}) column, as
#'   returned by \code{er_load("d10k")}.
#' @param id Optional record IDs. If \code{NULL} and \code{x} is a
#'   data.frame with an \code{id} column, that column is used; otherwise
#'   sequential IDs are created.
#' @return A tibble with \code{id} plus either parsed field columns or a
#'   single \code{text} column.
#' @export
er_parse_d10k_aggregate <- function(x, id = NULL) {
  if (is.data.frame(x)) {
    df <- tibble::as_tibble(x)
    names(df) <- tolower(names(df))
    text_col <- intersect(c("text", "aggregate_value"), names(df))[1]
    if (is.na(text_col)) {
      stop("er_parse_d10k_aggregate: data.frame input needs a 'text' or 'aggregate_value' column (see er_load(\"d10k\")).")
    }
    if (is.null(id) && "id" %in% names(df)) id <- df$id
    x <- as.character(df[[text_col]])
  }
  text <- as.character(x)
  if (is.null(id)) id <- seq_along(text)
  id <- as.character(id)
  non_empty <- !is.na(text) & nzchar(trimws(text))

  # ── Case 1: key=value pairs ─────────────────────────────────────────────
  kv_frac <- if (any(non_empty)) {
    mean(grepl("=", text[non_empty], fixed = TRUE))
  } else 0
  if (kv_frac >= 0.8) {
    rows <- lapply(text, function(s) {
      if (is.na(s) || !nzchar(trimws(s))) return(list())
      parts <- strsplit(s, "[;|]")[[1]]
      out <- list()
      for (p in parts) {
        kv <- strsplit(p, "=", fixed = TRUE)[[1]]
        if (length(kv) >= 2L) {
          key <- tolower(trimws(kv[1]))
          key <- gsub("[^a-z0-9]+", "_", key)
          out[[key]] <- trimws(paste(kv[-1], collapse = "="))
        }
      }
      out
    })
    keys <- unique(unlist(lapply(rows, names)))
    if (length(keys) >= 2L) {
      cols <- lapply(keys, function(k) {
        vapply(rows, function(r) {
          v <- r[[k]]
          if (is.null(v) || !length(v)) NA_character_ else v
        }, character(1L))
      })
      names(cols) <- keys
      return(tibble::as_tibble(c(list(id = id), cols)))
    }
  }

  # ── Case 2: one consistent separator ────────────────────────────────────
  for (sep in c(";", "|", "\t", ",")) {
    if (!any(non_empty)) break
    counts <- lengths(strsplit(text[non_empty], sep, fixed = TRUE))
    if (length(counts) && min(counts) >= 2L) {
      modal <- as.integer(names(sort(table(counts), decreasing = TRUE))[1])
      if (modal >= 2L && mean(counts == modal) >= 0.8) {
        mat <- do.call(rbind, lapply(text, function(s) {
          if (is.na(s) || !nzchar(trimws(s))) {
            return(rep(NA_character_, modal))
          }
          p <- strsplit(s, sep, fixed = TRUE)[[1]]
          length(p) <- modal
          trimws(p)
        }))
        cols <- lapply(seq_len(modal), function(j) mat[, j])
        names(cols) <- paste0("field_", seq_len(modal))
        return(tibble::as_tibble(c(list(id = id), cols)))
      }
    }
  }

  # ── Case 3: unstructured; keep a single text field ──────────────────────
  tibble::tibble(id = id, text = text)
}

#' Build an er_similarity() field spec for parsed D10K data
#'
#' Maps each parsed column to a similarity type using the same field-type
#' detection as \code{er_diagnose()}: year → \code{"year"}, numeric →
#' \code{"numeric"}, categorical → \code{"categorical"}, short text →
#' \code{"jw"}, long text → \code{"bow"}.
#'
#' @param parsed A tibble returned by \code{er_parse_d10k_aggregate()}.
#' @return A list of field specs suitable for \code{er_similarity()}.
#' @export
er_d10k_similarity_spec <- function(parsed) {
  df <- tibble::as_tibble(parsed)
  names(df) <- tolower(names(df))
  fields <- setdiff(names(df), c("id"))
  fields <- fields[!.er_is_label_column(fields)]
  if (!length(fields)) {
    stop("er_d10k_similarity_spec: no comparable fields remain after removing id/label columns. Reason: the parsed D10K frame carries no attributes. Next step: check er_parse_d10k_aggregate() output on the real file.")
  }
  n <- nrow(df)
  lapply(fields, function(f) {
    # Parsed Aggregate Value fields arrive as character; a couple of names
    # carry an unambiguous semantics even before type detection.
    if (identical(f, "year")) {
      return(list(name = f, type = "year"))
    }
    if (f %in% c("length", "number", "track", "duration")) {
      return(list(name = f, type = "numeric"))
    }
    ftype <- .er_detect_field_type(df[[f]], n)
    type <- switch(
      ftype,
      year = "year",
      numeric = "numeric",
      categorical = "categorical",
      text_short = "jw",
      text_long = "bow",
      "jw"
    )
    list(name = f, type = type)
  })
}

#' Compute the D10K per-field similarities and combined similarity matrix
#'
#' End-to-end D10K similarity layer: parse the Aggregate Value column (if the
#' input is not already parsed), generate candidate pairs by blocking (unless
#' \code{pairs} is supplied), compute per-field similarities with
#' \code{\link{er_similarity}}, combine them with NA-aware weighting, and
#' assemble the sparse n×n similarity matrix with
#' \code{\link{er_pairs_to_sparse}}.
#'
#' @param data A data.frame from \code{er_load("d10k")} (id + text), a parsed
#'   frame from \code{er_parse_d10k_aggregate()}, or a character vector of
#'   Aggregate Value strings.
#' @param pairs Optional candidate pairs tibble(\code{idx1}, \code{idx2}).
#'   If \code{NULL}, pairs are generated with \code{er_block()} using the
#'   diagnosis of the parsed data.
#' @param weights Weight specification passed to \code{er_combine()}
#'   (\code{"equal"} by default; no ground truth is used).
#' @param id Optional record IDs when \code{data} is a character vector.
#' @return A list with \code{parsed}, \code{pairs}, \code{spec},
#'   \code{sim_list}, \code{combined}, and \code{S} (sparse similarity matrix).
#' @export
er_d10k_similarity <- function(data, pairs = NULL, weights = "equal", id = NULL) {
  parsed <- if (is.data.frame(data) &&
                !("text" %in% tolower(names(data))) &&
                !("aggregate_value" %in% tolower(names(data)))) {
    tibble::as_tibble(data)
  } else {
    er_parse_d10k_aggregate(data, id = id)
  }
  names(parsed) <- tolower(names(parsed))
  if (!"id" %in% names(parsed)) {
    parsed$id <- as.character(seq_len(nrow(parsed)))
  }

  spec <- er_d10k_similarity_spec(parsed)

  if (is.null(pairs)) {
    diag_ <- suppressWarnings(er_diagnose(parsed, id_col = "id"))
    pairs <- er_block(parsed, diag = diag_, method = "auto")
  }

  sim_list <- er_similarity(parsed, pairs, spec = spec)
  if (!length(sim_list)) {
    stop(paste0(
      "er_d10k_similarity: er_similarity() returned no fields. ",
      "Reason: none of the parsed D10K columns survived the spec filter. ",
      "Next step: inspect er_parse_d10k_aggregate() output and er_d10k_similarity_spec() on the real 10Kfull.csv."
    ))
  }
  w <- if (is.character(weights)) {
    er_weights(sim_list, method = weights)
  } else {
    weights
  }
  combined <- er_combine(sim_list, weights = w)
  S <- er_pairs_to_sparse(pairs, combined, nrow(parsed))

  list(
    parsed = parsed,
    pairs = pairs,
    spec = spec,
    sim_list = sim_list,
    combined = combined,
    S = S
  )
}
