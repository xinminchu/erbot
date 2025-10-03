#' Select ID + build normalized text_for_matching column
#' @importFrom tibble tibble
#' @importFrom stringr str_replace_all str_trim
#' @importFrom stringi stri_trans_nfkc
#' @param df data.frame
#' @param id_col optional id name
#' @param fields vector of fields to concatenate
#' @param normalize logical
#' @param auto_fields logical
#' @return tibble with id and text_for_matching
#' @export
er_select_fields <- function(df,
                             id_col=NULL, fields=NULL, extra_fields=NULL,
                             id_candidates=c("id","affiliation_id","record_id","rec_id","docid","rowid","paper_id"),
                             text_candidates=c("title","name","raw_title","string","text","affiliation","aggregate value","clean ag.value"),
                             embed_candidates=c("embedded clean ag.value","embedded ag.value","emb","embedding","vector","embedding_clean"),
                             normalize=TRUE, auto_fields=TRUE, max_auto_fields=5){
  names(df) <- tolower(names(df))

  # pick id col
  if (is.null(id_col)) {
    id_col <- intersect(id_candidates, names(df))[1]
    if (is.na(id_col)) { df$id <- as.character(seq_len(nrow(df))); id_col <- "id" }
  } else stopifnot(id_col %in% names(df))

  # pick text fields
  if (is.null(fields) || !length(fields)) {
    main <- intersect(text_candidates, names(df))[1]
    if (!is.na(main)) {
      fields <- main
    } else if (auto_fields) {
      fields <- er_guess_text_fields(df, id_candidates, embed_candidates, max_auto_fields)
      if (!length(fields)) stop("No usable text fields; pass fields= or rename columns.")
      message("Auto-selected fields: ", paste(fields, collapse=" | "))
    } else {
      stop("No text fields. Supply fields= or ensure one of: ", paste(text_candidates, collapse=", "))
    }
  } else {
    fields <- tolower(fields)
    fields <- fields[fields %in% names(df)]
    if (!length(fields)) stop("Requested fields not found in data.")
  }

  # build text_for_matching WITHOUT dplyr pronouns
  vals <- df[, fields, drop = FALSE]
  # ensure character
  for (jj in seq_along(vals)) {
    vals[[jj]] <- as.character(vals[[jj]])
    vals[[jj]][is.na(vals[[jj]])] <- ""
  }
  txt <- if (ncol(vals) == 1L) {
    vals[[1]]
  } else {
    apply(vals, 1L, function(row) paste(row, collapse = " "))
  }

  if (normalize) {
    txt <- txt |>
      stringi::stri_trans_nfkc() |>
      tolower() |>
      stringr::str_replace_all("\\s+"," ") |>
      stringr::str_trim()
  }
  txt[is.na(txt) | txt == ""] <- " "

  tibble::tibble(
    id = as.character(df[[id_col]]),
    text_for_matching = txt
  )
}
