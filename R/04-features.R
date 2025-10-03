########################################

#' Parse a column of serialized embeddings to a numeric matrix
#' @param x character/any
#' @return matrix (rows = records, cols = inferred dim)
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
#' @param text_vec character vector
#' @param svd_dim target dimension
#' @return dense numeric matrix
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


########################################
