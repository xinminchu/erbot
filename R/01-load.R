########################################
# File: R/01-load.R
# Data loading. Replaces 02-io.R.
# Renames er_load_input() -> er_load(); keeps alias for back-compat.
# Adds built-in benchmark loaders: "cora", "affiliation", "d10k".
########################################

#' Read pipe-delimited or auto-detected separator
#'
#' Tries pipe, tab, semicolon, comma in that order.
#'
#' @param path Character. File path.
#' @return data.frame with lower-cased column names.
#' @export
er_read_pipe_or_fix <- function(path) {
  DT <- tryCatch(
    data.table::fread(path, sep = "|", quote = "\"", header = TRUE,
                      fill = TRUE, showProgress = FALSE),
    error = function(e) NULL
  )
  if (!is.null(DT) && ncol(DT) > 1) {
    DF <- as.data.frame(DT); names(DF) <- tolower(names(DF)); return(DF)
  }
  DF <- tryCatch(
    readr::read_delim(path, delim = "|", quote = '"', escape_double = TRUE,
                      trim_ws = TRUE, show_col_types = FALSE),
    error = function(e) NULL
  )
  if (!is.null(DF) && ncol(DF) > 1) {
    DF <- as.data.frame(DF); names(DF) <- tolower(names(DF)); return(DF)
  }
  raw   <- readr::read_lines(path)
  if (!length(raw)) stop("File empty: ", path)
  first <- raw[1]
  sep   <- if (grepl("\\|", first)) "\\|" else if (grepl("\t", first)) "\t" else if (grepl(";", first)) ";" else ","
  hdr   <- strsplit(first, sep)[[1]]
  rows  <- raw[-1]
  mat   <- t(vapply(rows, function(x) {
    parts <- strsplit(x, sep)[[1]]
    length(parts) <- length(hdr); parts[is.na(parts)] <- ""; parts
  }, character(length(hdr))))
  DF    <- as.data.frame(mat, stringsAsFactors = FALSE)
  names(DF) <- tolower(trimws(hdr))
  DF
}

#' Load input data
#'
#' Accepts a \code{data.frame}, a file path (CSV/TSV/pipe-delimited/Excel), or
#' a built-in benchmark keyword:
#' \describe{
#'   \item{\code{"cora"}}{CORA bibliographic dataset (requires \pkg{cora}).}
#'   \item{\code{"affiliation"}}{Affiliation strings dataset bundled with
#'     \pkg{erbot} (\code{inst/extdata/affiliation.csv}).}
#'   \item{\code{"d10k"}}{Synthetic 10 000-record dataset bundled with
#'     \pkg{erbot} (\code{inst/extdata/d10k.csv}).}
#' }
#'
#' @param data A \code{data.frame}, a character file path, or a benchmark
#'   keyword (\code{"cora"}, \code{"affiliation"}, \code{"d10k"}).
#' @param sheet Sheet index/name for Excel files (default 1).
#' @return A \code{tibble} with lower-cased column names.
#' @export
er_load <- function(data, sheet = NULL) {
  `%||%` <- get("%||%", asNamespace("erbot"))
  if (is.data.frame(data)) {
    df <- tibble::as_tibble(data); names(df) <- tolower(names(df)); return(df)
  }
  if (!is.character(data) || length(data) != 1L) {
    stop("'data' must be a data.frame, file path, or benchmark keyword.")
  }

  key <- trimws(tolower(data))

  # ── Built-in benchmarks ──────────────────────────────────────────────────────
  # Helper: locate a bundled dataset file.
  # Search order: (1) installed inst/extdata, (2) dev inst/extdata,
  # (3) dev data/ directory (where large files live during development).
  .find_file <- function(extdata_name, data_candidates = character()) {
    pkg_path <- tryCatch(
      system.file("extdata", extdata_name, package = "erbot", mustWork = FALSE),
      error = function(e) ""
    )
    if (nchar(pkg_path) > 0 && file.exists(pkg_path)) return(pkg_path)
    here <- tryCatch(
      normalizePath(file.path(dirname(sys.frame(0)$ofile), "..")),
      error = function(e) "."
    )
    p <- file.path(here, "inst", "extdata", extdata_name)
    if (file.exists(p)) return(p)
    for (nm in data_candidates) {
      p2 <- file.path(here, "data", nm)
      if (file.exists(p2)) return(p2)
    }
    stop("Built-in dataset not found (tried: ", extdata_name, ", ",
         paste(data_candidates, collapse = ", "), "). ",
         "Supply the file path directly.")
  }

  if (key == "cora") {
    # Prefer CSV file (data/cora.csv) parsed from CORA.xml; fall back to R package.
    csv_path <- tryCatch(.find_file("cora.csv", c("cora.csv")), error = function(e) NULL)
    if (!is.null(csv_path)) {
      df <- tibble::as_tibble(data.table::fread(csv_path, showProgress = FALSE))
      names(df) <- tolower(names(df))
      return(df)
    }
    if (!requireNamespace("cora", quietly = TRUE))
      stop("Package 'cora' not installed and data/cora.csv not found.")
    df <- tibble::as_tibble(get("cora", envir = asNamespace("cora")))
    names(df) <- tolower(names(df))
    if ("book title" %in% names(df))
      names(df)[names(df) == "book title"] <- "booktitle"
    return(df)
  }

  if (key == "affiliation") {
    path <- .find_file("affiliation.csv",
                       c("affiliationstrings_ids.csv", "affiliation.csv"))
    df   <- tibble::as_tibble(data.table::fread(path, showProgress = FALSE))
    names(df) <- tolower(names(df))
    # Normalise column names produced by the raw ids file (id1 / affil1).
    if ("id1"    %in% names(df) && !"id"          %in% names(df))
      names(df)[names(df) == "id1"]    <- "id"
    if ("affil1" %in% names(df))
      names(df)[names(df) == "affil1"] <- "affiliation"
    return(df)
  }

  if (key == "d10k") {
    path <- .find_file("d10k.csv", c("10Kfull.csv", "d10k.csv"))
    # The file is pipe-delimited and contains multi-line embedding columns.
    # Select only Id and Aggregate Value (text) to avoid parsing issues.
    df <- tryCatch(
      data.table::fread(path, sep = "|",
                        select = c("Id", "Aggregate Value"),
                        showProgress = FALSE),
      error = function(e)
        data.table::fread(path, sep = "|", select = c(1L, 2L),
                          showProgress = FALSE)
    )
    df <- tibble::as_tibble(df)
    names(df) <- tolower(gsub("\\s+", "_", trimws(names(df))))
    if ("aggregate_value" %in% names(df))
      names(df)[names(df) == "aggregate_value"] <- "text"
    return(df)
  }

  # ── File path ────────────────────────────────────────────────────────────────
  p   <- data
  ext <- tolower(tools::file_ext(p))
  if (ext %in% c("xlsx", "xls")) {
    df <- readxl::read_excel(p, sheet = sheet %||% 1L) %>% tibble::as_tibble()
  } else {
    df <- tryCatch(
      data.table::fread(p, showProgress = FALSE) %>% tibble::as_tibble(),
      error = function(e) NULL
    )
    if (is.null(df)) df <- tibble::as_tibble(er_read_pipe_or_fix(p))
    if (ncol(df) == 1L) df <- tibble::as_tibble(er_read_pipe_or_fix(p))
  }
  names(df) <- tolower(names(df))
  df
}

#' @rdname er_load
#' @export
er_load_input <- er_load   # backward-compatibility alias


# ── NC Voter helpers (kept from 02-io.R) ─────────────────────────────────────

#' Read NC voter registration CSV splits
#'
#' @param root Directory containing CSV partitions.
#' @param which Partition set: \code{"5"}, \code{"10"}, or \code{"all"}.
#' @return A \code{tibble} with lower-cased column names.
#' @export
ncvr_read <- function(root, which = c("5", "10", "all")) {
  which   <- match.arg(which)
  all_csv <- list.files(root, pattern = "\\.csv$", full.names = TRUE,
                        recursive = TRUE)
  if (!length(all_csv)) stop("No CSVs found under: ", root)
  sel <- switch(which,
                "5"   = all_csv[grepl("_nump_5\\.csv$",  basename(all_csv))],
                "10"  = all_csv[grepl("_nump_10\\.csv$", basename(all_csv))],
                "all" = all_csv)
  if (!length(sel)) stop("No files matched split '", which, "'.")
  DT <- data.table::rbindlist(
    lapply(sel, function(p) data.table::fread(p, showProgress = FALSE)),
    use.names = TRUE, fill = TRUE
  )
  df <- tibble::as_tibble(DT); names(df) <- tolower(names(df)); df
}

#' Guess NCVR-like fields
#'
#' @param df data.frame whose column names will be searched.
#' @return Character vector of matched column names, or \code{NULL}.
#' @export
ncvr_guess_fields <- function(df) {
  nms   <- names(df)
  pick1 <- function(pats) {
    hits <- unique(unlist(lapply(pats, function(p) grep(p, nms, perl = TRUE,
                                                        value = TRUE))))
    if (length(hits)) hits[1] else NA_character_
  }
  # Patterns cover both standard NCVR column names and the synthetic-partition
  # naming (givenname / surname / suburb / postcode).
  first  <- pick1(c("^givenname$", "^given_?name$",
                    "^first(_|)name$", "^voter_?first_?name$", "^first$"))
  middle <- pick1(c("^middle(_|)name$", "^voter_?middle_?name$", "^middle$",
                    "^mi(ddle)?_?name?$"))
  last   <- pick1(c("^surname$", "^last(_|)name$",
                    "^voter_?last_?name$", "^last$"))
  street <- pick1(c("^res(idence)?_?street(_|)address$", "^res_?addr.*$",
                    "^res_?street.*$", "^address(_1)?$"))
  city   <- pick1(c("^suburb$", "^res(idence)?_?city(_|)(desc)?$",
                    "^res_?city$", "^city(_desc)?$"))
  state  <- pick1(c("^res(idence)?_?state(_|)(cd|code)?$", "^res_?state$",
                    "^state(_cd|_code)?$"))
  zip    <- pick1(c("^postcode$", "^post_?code$",
                    "^res(idence)?_?zip(_|)(code)?$", "^res_?zip$",
                    "^zip(_code)?$"))
  fields <- c(first, middle, last, street, city, state, zip)
  fields[!is.na(fields)]
}

# ── Timestamp filename helper ─────────────────────────────────────────────────

#' Generate a timestamped filename
#' @param prefix Character prefix.
#' @param ext File extension (without dot).
#' @return Character filename.
#' @export
make_timestamp_filename <- function(prefix = "erbot_run", ext = "csv") {
  ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  paste0(prefix, "_", ts, ".", ext)
}
