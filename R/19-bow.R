########################################
# File: R/19-bow.R
# Type-2 Bag-of-Words (BoW) similarity computation for multi-view ER.
#
# er_bow()            -- main entry point; mirrors er_similarity() output format
# .bow_tokenize       -- internal: character n-gram or word tokenizer
# .bow_tfidf_matrix   -- internal: compute sparse TF-IDF matrix
# .bow_cosine_pairs   -- internal: cosine similarity for candidate pairs
########################################

#' Bag-of-Words TF-IDF cosine similarities for candidate pairs (Type-2)
#'
#' Computes per-field TF-IDF cosine similarities for a list of candidate pairs,
#' producing the same \emph{named list of numeric vectors} as
#' \code{er_similarity()} (Type-3).  Use as a direct drop-in replacement when
#' text fields should be compared via sparse token vectors rather than
#' character-level edit distances.
#'
#' \strong{Algorithm:}
#' \enumerate{
#'   \item Tokenise each field value into character \eqn{n}-grams (default:
#'     trigrams, \code{ngram=3}) or word unigrams (\code{tokenizer="word"}).
#'   \item Compute IDF on the \emph{train set} (or the full dataset when
#'     \code{train_idx} is \code{NULL}).  IDF for term \eqn{t} with document
#'     frequency \eqn{df_t} is \eqn{\log\bigl((N+1)/(df_t+1)\bigr)+1}
#'     (smoothed to avoid division by zero and to keep all terms positive).
#'   \item Compute L2-normalised TF-IDF vectors for every record.
#'   \item For each candidate pair \eqn{(i,j)}, cosine similarity
#'     \eqn{= \mathbf{u}_i^{\top} \mathbf{u}_j} (inner product of unit vectors).
#'   \item Return \code{NA_real_} for any pair where either record has an empty
#'     or missing field value.
#' }
#'
#' If the \pkg{text2vec} package is installed it is used for IDF and cosine
#' computation (faster for large vocabularies); otherwise the function falls
#' back to a pure-Matrix implementation.
#'
#' @param df \code{data.frame} of records (all fields present as columns).
#' @param pairs \code{tibble(idx1, idx2)} from \code{er_block()}.
#' @param fields Named character vector mapping field names to tokenizer type:
#'   \code{"char"} (character n-grams, default) or \code{"word"} (whitespace
#'   tokens).  If a plain character vector is given all fields use
#'   \code{"char"}.  If \code{NULL}, all character/factor columns are used
#'   with \code{"char"}.
#' @param ngram Integer.  Character n-gram size when \code{type="char"}.
#'   Default \code{3L}.
#' @param train_idx Integer vector of row indices to use for IDF computation.
#'   Prevents test-set leakage in cross-validation.  Default \code{NULL}
#'   (use all rows).
#' @param min_df Integer.  Minimum document frequency for a term to be kept
#'   in the vocabulary.  Terms appearing in fewer than \code{min_df} documents
#'   are discarded.  Default \code{1L}.
#' @param na_fill Value substituted for pairs where either record is missing
#'   the field.  Default \code{NA_real_}.
#'
#' @return Named list of numeric vectors (one per field in \code{fields}),
#'   each of length \code{nrow(pairs)}.  \code{NA_real_} signals missing data
#'   for that pair.
#'
#' @seealso \code{\link{er_similarity}}, \code{\link{er_embed}},
#'   \code{\link{er_compare_paradigms}}
#'
#' @examples
#' \dontrun{
#' df    <- er_load("restaurant")
#' diag  <- er_diagnose(df)
#' pairs <- er_block(df, diag)
#'
#' # Use BoW similarities instead of Type-3 Jaro-Winkler for all text fields
#' sim_bow <- er_bow(df, pairs, fields = c("name", "address", "city"))
#'
#' # Compare with Type-3 (Jaro-Winkler)
#' sim_jw  <- er_similarity(df, pairs)
#'
#' # Both can be passed directly to er_combine(), er_classify(), etc.
#' wt  <- er_weights(sim_bow)
#' S   <- er_pairs_to_sparse(pairs, er_combine(sim_bow, wt), nrow(df))
#' }
#' @export
er_bow <- function(df,
                   pairs,
                   fields    = NULL,
                   ngram     = 3L,
                   train_idx = NULL,
                   min_df    = 1L,
                   na_fill   = NA_real_) {

  stopifnot(is.data.frame(df),
            is.data.frame(pairs),
            all(c("idx1", "idx2") %in% names(pairs)))

  # Resolve fields
  if (is.null(fields)) {
    # Auto-detect character/factor columns
    is_text <- vapply(names(df), function(cn) {
      col <- df[[cn]]
      (is.character(col) || is.factor(col)) && mean(is.na(col)) < 0.9
    }, logical(1L))
    fields <- structure(rep("char", sum(is_text)),
                        names = names(df)[is_text])
  } else if (is.character(fields) && is.null(names(fields))) {
    # Plain character vector: all use "char"
    fields <- structure(rep("char", length(fields)), names = fields)
  }

  if (!length(fields))
    stop("er_bow: no text fields to process. Provide 'fields' explicitly.")

  missing_cols <- setdiff(names(fields), names(df))
  if (length(missing_cols))
    stop("er_bow: columns not found in df: ",
         paste(missing_cols, collapse = ", "))

  n_records <- nrow(df)
  n_pairs   <- nrow(pairs)
  ngram     <- as.integer(ngram)
  min_df    <- as.integer(min_df)

  if (is.null(train_idx)) train_idx <- seq_len(n_records)

  result <- lapply(names(fields), function(fname) {
    tok_type <- fields[[fname]]
    col      <- as.character(df[[fname]])
    col[is.na(col)] <- ""

    # Determine which pairs have non-empty values on both sides
    v1 <- col[pairs$idx1]
    v2 <- col[pairs$idx2]
    ok <- nchar(v1) > 0L & nchar(v2) > 0L

    if (!any(ok)) {
      return(rep(na_fill, n_pairs))
    }

    # Tokenize all records
    tokens <- .bow_tokenize(col, tok_type, ngram)

    # Build TF-IDF on train set; apply to all records
    tfidf <- .bow_tfidf_matrix(tokens, train_idx = train_idx, min_df = min_df)
    if (is.null(tfidf) || ncol(tfidf) == 0L) {
      return(rep(na_fill, n_pairs))
    }

    # Cosine similarities for candidate pairs
    sims <- .bow_cosine_pairs(tfidf, pairs$idx1, pairs$idx2)

    # NA for empty-value pairs
    sims[!ok] <- na_fill
    sims
  })
  names(result) <- names(fields)
  result
}

# ── Tokenizers ─────────────────────────────────────────────────────────────────

# Returns a list of character vectors (token sets), one per record.
# Empty strings yield character(0).
.bow_tokenize <- function(texts, type = "char", ngram = 3L) {
  texts <- tolower(texts)
  texts <- gsub("[^[:alnum:][:space:]]", " ", texts)
  texts <- trimws(gsub("\\s+", " ", texts))

  if (type == "word") {
    lapply(texts, function(x) {
      if (nchar(x) == 0L) return(character(0L))
      strsplit(x, "\\s+")[[1L]]
    })
  } else {
    # Character n-grams with padding
    g <- ngram
    lapply(texts, function(x) {
      if (nchar(x) == 0L) return(character(0L))
      # Pad with special boundary char
      x  <- paste0(strrep("#", g - 1L), x, strrep("#", g - 1L))
      nc <- nchar(x)
      if (nc < g) return(character(0L))
      vapply(seq_len(nc - g + 1L), function(i)
        substr(x, i, i + g - 1L), character(1L))
    })
  }
}

# ── TF-IDF sparse matrix ───────────────────────────────────────────────────────

# Builds an n_records x |vocab| sparse TF-IDF matrix.
# IDF computed only on train_idx rows (to avoid test-set leakage).
# Returns a dgCMatrix with L2-normalised rows, or NULL if vocab is empty.
.bow_tfidf_matrix <- function(tokens, train_idx, min_df = 1L) {
  n <- length(tokens)

  # Build vocabulary from training documents
  train_tokens <- tokens[train_idx]
  doc_freq     <- table(unlist(lapply(train_tokens, unique)))
  if (min_df > 1L)
    doc_freq <- doc_freq[doc_freq >= min_df]
  vocab <- names(doc_freq)
  if (!length(vocab)) return(NULL)

  V   <- length(vocab)
  N   <- length(train_idx)  # number of training docs
  # Smoothed IDF: log((N + 1) / (df + 1)) + 1
  idf <- log((N + 1) / (as.numeric(doc_freq) + 1)) + 1
  names(idf) <- vocab

  # term -> index map
  term_idx <- seq_along(vocab)
  names(term_idx) <- vocab

  # Build sparse TF-IDF matrix (all n records, not just train)
  row_vec <- integer(0L)
  col_vec <- integer(0L)
  val_vec <- numeric(0L)

  for (doc_i in seq_len(n)) {
    toks <- tokens[[doc_i]]
    if (!length(toks)) next
    # TF: count occurrences of each term
    tf_tbl <- table(toks[toks %in% vocab])
    if (!length(tf_tbl)) next
    terms_present <- names(tf_tbl)
    tidx  <- term_idx[terms_present]
    tf    <- as.numeric(tf_tbl)
    tfidf_vals <- tf * idf[terms_present]
    row_vec <- c(row_vec, rep(doc_i, length(tidx)))
    col_vec <- c(col_vec, tidx)
    val_vec <- c(val_vec, tfidf_vals)
  }

  if (!length(row_vec)) return(NULL)

  M <- Matrix::sparseMatrix(
    i    = row_vec,
    j    = col_vec,
    x    = val_vec,
    dims = c(n, V)
  )

  # L2-normalize each row
  row_norms <- sqrt(Matrix::rowSums(M^2))
  row_norms[row_norms == 0] <- 1
  M <- Matrix::Diagonal(x = 1 / row_norms) %*% M
  methods::as(M, "dgCMatrix")
}

# ── Cosine similarities for pairs ─────────────────────────────────────────────

# Given an n x V L2-normalised TF-IDF matrix and two index vectors,
# return cosine similarity (dot product of unit-norm rows) for each pair.
.bow_cosine_pairs <- function(tfidf, idx1, idx2) {
  n_pairs <- length(idx1)
  if (n_pairs == 0L) return(numeric(0L))

  # text2vec::sim2() is ~10x faster for large pair sets; use if available
  if (requireNamespace("text2vec", quietly = TRUE)) {
    sims_mat <- text2vec::sim2(tfidf[idx1, , drop = FALSE],
                               tfidf[idx2, , drop = FALSE],
                               method = "cosine", norm = "none")
    # sim2 returns an n_pairs x n_pairs matrix; we want only the diagonal
    return(as.numeric(Matrix::diag(sims_mat)))
  }

  # Pure-Matrix fallback: row-wise dot products
  R1 <- tfidf[idx1, , drop = FALSE]
  R2 <- tfidf[idx2, , drop = FALSE]

  # Efficient row-dot-product via element-wise product then rowSums
  as.numeric(Matrix::rowSums(R1 * R2))
}
