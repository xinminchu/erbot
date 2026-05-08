########################################
# File: D:/erbot/R/04-features.R
########################################

#' Parse a column of serialized embeddings to a numeric matrix
#'
#' Accepts a character vector where each element is a serialized numeric
#' sequence (e.g., `"[0.1, 0.2, ...]"` or `"0.1 0.2 ..."`), extracts all
#' numeric tokens via regex, and assembles them into a matrix whose column
#' dimension is inferred from the most common token count.
#'
#' @param x Character vector (or coercible to character) of serialized
#'   embeddings. `NA` values are treated as empty strings.
#' @return Numeric matrix with `length(x)` rows and inferred embedding
#'   dimension columns. Rows with no tokens are filled with `NA`.
#' @export
er_safe_parse_embedding_col <- function(x){
  x <- as.character(x); x[is.na(x)] <- ""
  num_pat <- "[-+]?(?:\\d*\\.\\d+|\\d+)(?:[eE][-+]?\\d+)?"
  lst <- regmatches(x, gregexpr(num_pat, x, perl=TRUE))
  lens <- lengths(lst); if (all(lens==0L)) stop("No numeric tokens found in embedding col.")
  tab <- sort(table(lens[lens>0L]), decreasing=TRUE); d <- if (length(tab)) as.integer(names(tab)[1]) else max(lens)
  m <- matrix(NA_real_, nrow=length(lst), ncol=d)
  for (i in seq_along(lst)) { v <- suppressWarnings(as.numeric(lst[[i]])); if (length(v)) m[i, seq_len(min(length(v), d))] <- v[seq_len(min(length(v), d))] }
  storage.mode(m) <- "double"; m
}

#' TF-IDF + truncated SVD features
#'
#' Builds a TF-IDF document-term matrix from `text_vec` using **text2vec**,
#' then reduces it to a dense embedding via truncated SVD (**irlba**).
#'
#' @param text_vec Character vector of strings to embed (one per record).
#' @param svd_dim Integer. Target number of SVD dimensions. Clipped to
#'   `min(dim(DTM)) - 1` if too large, with a minimum of 2. Default `100`.
#' @return Dense numeric matrix with `length(text_vec)` rows and up to
#'   `svd_dim` columns (rows × left singular vectors × singular values).
#' @export
er_features_tfidf_svd <- function(text_vec, svd_dim=100){
  it <- text2vec::itoken(text_vec, tokenizer=text2vec::word_tokenizer, progressbar=FALSE)
  vocab <- text2vec::create_vocabulary(it)
  vec <- text2vec::vocab_vectorizer(vocab)
  dtm <- text2vec::create_dtm(it, vec)
  tfidf <- text2vec::TfIdf$new(); Xtf <- tfidf$fit_transform(dtm)
  k_dim <- max(2L, min(svd_dim, min(dim(Xtf))-1L))
  set.seed(42); svd_res <- irlba::irlba(Xtf, nv=k_dim)
  svd_res$u %*% diag(svd_res$d)
}
