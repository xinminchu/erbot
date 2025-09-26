#' Read "pipe" delimited or try to fix common separators
#' @param path file path
#' @return data.frame with lower-cased names
#' @export
er_read_pipe_or_fix <- function(path){
  DT <- tryCatch(data.table::fread(path, sep="|", quote="\"", header=TRUE, fill=TRUE, showProgress=FALSE), error=function(e) NULL)
  if (!is.null(DT) && ncol(DT) > 1) { DF <- as.data.frame(DT); names(DF) <- tolower(names(DF)); return(DF) }
  DF <- tryCatch(readr::read_delim(path, delim="|", quote='"', escape_double=TRUE, trim_ws=TRUE, show_col_types=FALSE), error=function(e) NULL)
  if (!is.null(DF) && ncol(DF) > 1) { DF <- as.data.frame(DF); names(DF) <- tolower(names(DF)); return(DF) }
  raw <- readr::read_lines(path); if (!length(raw)) stop("File empty: ", path)
  first <- raw[1]; sep <- if (grepl("\\|", first)) "\\|" else if (grepl("\t", first)) "\t" else if (grepl(";", first)) ";" else ","
  hdr <- strsplit(first, sep)[[1]]
  rows <- raw[-1]
  mat <- t(vapply(rows, function(x){
    parts <- strsplit(x, sep)[[1]]
    length(parts) <- length(hdr); parts[is.na(parts)] <- ""
    parts
  }, character(length(hdr))))
  DF <- as.data.frame(mat, stringsAsFactors=FALSE); names(DF) <- tolower(trimws(hdr)); DF
}

#' Load input by path, Excel, or data.frame
#' @param data data.frame or path or special key 'cora'
#' @param sheet sheet index/name for Excel
#' @return tibble with lower-cased names
#' @export
er_load_input <- function(data, sheet=NULL){
  if (is.data.frame(data)) { df <- tibble::as_tibble(data); names(df) <- tolower(names(df)); return(df) }
  if (is.character(data) && length(data) == 1L) {
    key <- tolower(data)
    if (key == "cora") {
      if (!requireNamespace("cora", quietly=TRUE)) stop("Package 'cora' not installed.")
      df <- tibble::as_tibble(get("cora", envir=asNamespace("cora"))); names(df) <- tolower(names(df)); return(df)
    }
    p <- data; ext <- tolower(tools::file_ext(p))
    if (ext %in% c("xlsx","xls")) df <- readxl::read_excel(p, sheet = sheet %||% 1L) %>% tibble::as_tibble()
    else {
      df <- tryCatch(data.table::fread(p, showProgress=TRUE) %>% tibble::as_tibble(), error=function(e) NULL)
      if (is.null(df)) df <- tibble::as_tibble(er_read_pipe_or_fix(p))
      if (ncol(df) == 1) df <- tibble::as_tibble(er_read_pipe_or_fix(p))
    }
    names(df) <- tolower(names(df)); return(df)
  }
  stop("Unsupported 'data'. Provide data.frame, path/URL, or 'cora'.")
}

#' Read NC voter registration CSV splits
#' @param root folder
#' @param which "5","10","all"
#' @return tibble
#' @export
ncvr_read <- function(root, which=c("5","10","all")){
  which <- match.arg(which)
  all_csv <- list.files(root, pattern="\\.csv$", full.names=TRUE, recursive=TRUE)
  if (!length(all_csv)) stop("No CSVs found under: ", root)
  sel <- switch(which,
                "5" = all_csv[grepl("_nump_5\\.csv$", basename(all_csv))],
                "10"= all_csv[grepl("_nump_10\\.csv$", basename(all_csv))],
                "all" = all_csv)
  if (!length(sel)) stop("No files matched split '", which, "'.")
  DT <- data.table::rbindlist(lapply(sel, function(p) data.table::fread(p, showProgress=FALSE)), use.names=TRUE, fill=TRUE)
  df <- tibble::as_tibble(DT); names(df) <- tolower(names(df)); df
}

#' Guess NCVR-like fields
#' @param df data.frame
#' @return character vector of fields
#' @export
ncvr_guess_fields <- function(df){
  nms <- names(df)
  pick1 <- function(pats){
    hits <- unique(unlist(lapply(pats, function(p) grep(p, nms, perl=TRUE, value=TRUE))))
    if (length(hits)) hits[1] else NA_character_
  }
  first  <- pick1(c("^first(_|)name$", "^voter_?first_?name$", "^first$"))
  middle <- pick1(c("^middle(_|)name$", "^voter_?middle_?name$", "^middle$", "^mi(ddle)?_?name?$"))
  last   <- pick1(c("^last(_|)name$", "^voter_?last_?name$", "^surname$", "^last$"))
  street <- pick1(c("^res(idence)?_?street(_|)address$", "^res_?addr.*$", "^res_?street.*$", "^address(_1)?$"))
  city   <- pick1(c("^res(idence)?_?city(_|)(desc)?$", "^res_?city$", "^city(_desc)?$"))
  state  <- pick1(c("^res(idence)?_?state(_|)(cd|code)?$", "^res_?state$", "^state(_cd|_code)?$"))
  zip    <- pick1(c("^res(idence)?_?zip(_|)(code)?$", "^res_?zip$", "^zip(_code)?$"))
  fields <- c(first, middle, last, street, city, state, zip)
  fields <- fields[!is.na(fields)]
  if (!length(fields)) return(NULL)
  fields
}
