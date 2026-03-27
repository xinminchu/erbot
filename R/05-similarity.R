########################################
# File: R/05-similarity.R
# Per-field similarity computation.
# Returns NA for pairs where either record is missing a field value.
# NA values are NEVER imputed here; they propagate to er_combine().
#
# Supported field types and their similarity functions:
#   jw          Jaro-Winkler (short text, names)
#   lv          Normalised Levenshtein (short text, alternative to jw)
#   jaccard     Token-Jaccard (medium text)
#   bow         IDF-weighted Bag-of-Words cosine similarity (long text)
#   categorical Exact match in {0, 1}
#   numeric     Exponential decay: exp(-|a - b| / (tau * range))
#   year        Tolerance: 1 if |a - b| <= 1, else 0
########################################

# ── Atomic similarity functions ───────────────────────────────────────────────
# All return NA_real_ when either argument is NA or empty string.

#' Jaro-Winkler similarity between two strings
#' @param a,b Character scalars.
#' @return Numeric in \eqn{[0,1]}, or \code{NA_real_}.
#' @keywords internal
.sim_jw <- function(a, b) {
  if (is.na(a) || is.na(b) || a == "" || b == "") return(NA_real_)
  1 - stringdist::stringdist(a, b, method = "jw")
}

#' Normalised Levenshtein similarity between two strings
#' @param a,b Character scalars.
#' @return Numeric in \eqn{[0,1]}, or \code{NA_real_}.
#' @keywords internal
.sim_lv <- function(a, b) {
  if (is.na(a) || is.na(b) || a == "" || b == "") return(NA_real_)
  m <- max(nchar(a), nchar(b))
  if (m == 0L) return(NA_real_)
  1 - stringdist::stringdist(a, b, method = "lv") / m
}

#' Token-Jaccard similarity between two strings
#' @param a,b Character scalars.
#' @return Numeric in \eqn{[0,1]}, or \code{NA_real_}.
#' @keywords internal
.sim_jaccard <- function(a, b) {
  if (is.na(a) || is.na(b) || a == "" || b == "") return(NA_real_)
  tok <- function(x) {
    x <- tolower(gsub("[^[:alnum:]]+", " ", x))
    x <- stringr::str_squish(x)
    if (identical(x, "")) character(0L) else unique(strsplit(x, "\\s+")[[1]])
  }
  A <- tok(a); B <- tok(b)
  if (!length(A) && !length(B)) return(NA_real_)
  inter <- length(intersect(A, B)); uni <- length(union(A, B))
  if (uni == 0L) return(NA_real_)
  inter / uni
}

#' Exact-match categorical similarity
#' @param a,b Character scalars (or coercible to character).
#' @return \code{1} if \code{a == b}, \code{0} if not, \code{NA_real_} if
#'   either is \code{NA}.
#' @keywords internal
.sim_categorical <- function(a, b) {
  if (is.na(a) || is.na(b)) return(NA_real_)
  as.numeric(identical(as.character(a), as.character(b)))
}

#' Exponential-decay numeric similarity
#' @param a,b Numeric scalars.
#' @param range_max Numeric; value range of the field (used for normalisation).
#' @param tau Numeric; decay parameter (default \code{1}).
#' @return Numeric in \eqn{(0,1]}, or \code{NA_real_}.
#' @keywords internal
.sim_numeric <- function(a, b, range_max, tau = 1) {
  if (is.na(a) || is.na(b)) return(NA_real_)
  a <- suppressWarnings(as.numeric(a))
  b <- suppressWarnings(as.numeric(b))
  if (is.na(a) || is.na(b)) return(NA_real_)
  if (!is.finite(range_max) || range_max <= 0) return(NA_real_)
  d <- abs(a - b) / range_max
  exp(-d / tau)
}

#' Year-tolerance similarity (exact or off-by-one)
#' @param a,b Numeric (or coercible) scalars representing years.
#' @return \code{1} if \eqn{|a - b| \le 1}, else \code{0}, or \code{NA_real_}.
#' @keywords internal
.sim_year <- function(a, b) {
  if (is.na(a) || is.na(b)) return(NA_real_)
  a <- suppressWarnings(as.numeric(a))
  b <- suppressWarnings(as.numeric(b))
  if (is.na(a) || is.na(b)) return(NA_real_)
  as.numeric(abs(a - b) <= 1)
}

#' Build corpus-level IDF table for BoW similarity
#'
#' Tokenizes every document in \code{corpus} and computes smoothed inverse
#' document frequency for each term:
#' \deqn{\mathrm{IDF}(t) = \log\!\left(\frac{N+1}{\mathrm{df}(t)+1}\right) + 1}
#' where \eqn{N} is the number of non-empty documents and \eqn{\mathrm{df}(t)}
#' is the number of documents containing term \eqn{t}.
#'
#' @param corpus Character vector of all field values (the whole column).
#' @return Named numeric vector of IDF weights (one per unique term).
#' @keywords internal
.bow_idf <- function(corpus) {
  corpus <- corpus[!is.na(corpus) & nzchar(corpus)]
  N <- length(corpus)
  if (N == 0L) return(setNames(numeric(0L), character(0L)))
  tok <- function(x) {
    x <- tolower(gsub("[^[:alnum:]]+", " ", x))
    x <- stringr::str_squish(x)
    if (nzchar(x)) unique(strsplit(x, " ")[[1L]]) else character(0L)
  }
  df_counts <- table(unlist(lapply(corpus, tok)))
  idf <- log((N + 1) / (as.numeric(df_counts) + 1)) + 1
  setNames(as.numeric(idf), names(df_counts))
}

#' IDF-weighted Bag-of-Words cosine similarity
#'
#' Tokenizes \code{a} and \code{b}, builds TF vectors over their union
#' vocabulary, weights each term by its pre-computed IDF, and returns the
#' cosine similarity of the two weighted vectors.
#'
#' @param a,b Character scalars.
#' @param idf Named numeric vector of IDF weights from \code{.bow_idf()}.
#'   Terms absent from \code{idf} receive weight \code{log(2)} (low weight
#'   for corpus-unseen tokens).
#' @return Numeric in \eqn{[0,1]}, or \code{NA_real_} if either string is
#'   empty/\code{NA} or produces no tokens.
#' @keywords internal
.sim_bow <- function(a, b, idf) {
  if (is.na(a) || is.na(b) || !nzchar(a) || !nzchar(b)) return(NA_real_)
  tok <- function(x) {
    x <- tolower(gsub("[^[:alnum:]]+", " ", x))
    x <- stringr::str_squish(x)
    if (nzchar(x)) strsplit(x, " ")[[1L]] else character(0L)
  }
  ta <- tok(a); tb <- tok(b)
  if (!length(ta) || !length(tb)) return(NA_real_)

  tf_a  <- table(ta); tf_b <- table(tb)
  vocab <- union(names(tf_a), names(tf_b))

  # IDF weights: unknown terms get log(2) (conservative low weight)
  w      <- idf[vocab]
  w[is.na(w)] <- log(2)

  va <- as.numeric(tf_a[vocab]); va[is.na(va)] <- 0L; va <- va * w
  vb <- as.numeric(tf_b[vocab]); vb[is.na(vb)] <- 0L; vb <- vb * w

  denom <- sqrt(sum(va^2)) * sqrt(sum(vb^2))
  if (denom == 0) return(NA_real_)
  sum(va * vb) / denom
}

# ── Vectorised per-field similarity over candidate pairs ─────────────────────

#' Compute per-field similarity for all candidate pairs
#'
#' For each field in \code{spec}, computes a similarity value in \eqn{[0,1]}
#' for every candidate pair in \code{pairs}.  Missing values in either record
#' produce \code{NA} for that pair-field combination; they are \emph{never}
#' imputed here.
#'
#' @param data A \code{data.frame} of records (all rows; lower-cased names).
#' @param pairs A \code{tibble(idx1, idx2)} from \code{er_block()}.
#' @param spec A list of field specifications. Each element is a named list
#'   with at minimum \code{name} (column name) and \code{type} (one of
#'   \code{"jw"}, \code{"lv"}, \code{"jaccard"}, \code{"bow"},
#'   \code{"categorical"}, \code{"numeric"}, \code{"year"}).
#'   Optional: \code{tau} for \code{"numeric"} (default 1).
#'   \code{"bow"} computes IDF-weighted cosine similarity; IDF is built once
#'   per field from all values in \code{data}.
#'   If \code{NULL}, the spec is built automatically from \code{diag}.
#' @param diag Optional list from \code{er_diagnose()}, used to build a spec
#'   when \code{spec} is \code{NULL}.
#'
#' @return A named list: one numeric vector per field (length = \code{nrow(pairs)},
#'   values in \eqn{[0,1]} or \code{NA}).  Also contains attribute
#'   \code{"n_records"} (nrow(data)) and \code{"n_pairs"} (nrow(pairs)).
#' @export
er_similarity <- function(data, pairs, spec = NULL, diag = NULL) {

  df <- tibble::as_tibble(data)
  names(df) <- tolower(names(df))
  n      <- nrow(df)
  n_pairs <- nrow(pairs)

  # ── Build spec from diag if not supplied ──────────────────────────────────
  if (is.null(spec)) {
    if (!is.null(diag) && nrow(diag$fields)) {
      spec <- lapply(seq_len(nrow(diag$fields)), function(i) {
        row <- diag$fields[i, , drop = FALSE]
        list(name = row$name, type = row$sim_method)
      })
    } else {
      # No info: use all character columns with jw
      char_cols <- names(df)[vapply(df, is.character, logical(1L))]
      spec <- lapply(char_cols, function(cn) list(name = cn, type = "jw"))
    }
  }

  # Filter spec to columns actually present in data
  spec <- spec[vapply(spec, function(s) s$name %in% names(df), logical(1L))]
  if (!length(spec)) return(list())

  if (n_pairs == 0L) {
    out <- lapply(spec, function(s) numeric(0L))
    names(out) <- vapply(spec, `[[`, character(1L), "name")
    attr(out, "n_records") <- n; attr(out, "n_pairs") <- 0L
    return(out)
  }

  idx1 <- pairs$idx1; idx2 <- pairs$idx2

  # ── Pre-compute numeric ranges (for "numeric" type) ────────────────────────
  num_ranges <- list()
  for (sp in spec) {
    if (sp$type == "numeric") {
      vv <- suppressWarnings(as.numeric(df[[sp$name]]))
      vv <- vv[is.finite(vv)]
      num_ranges[[sp$name]] <- if (length(vv) >= 2L) diff(range(vv)) else NA_real_
    }
  }

  # ── Compute similarities field by field ───────────────────────────────────
  out <- lapply(spec, function(sp) {
    fname <- sp$name
    ftype <- sp$type
    col   <- df[[fname]]

    switch(ftype,

      # Vectorised: stringdist() accepts two equal-length character vectors and
      # computes element-wise distances — no per-pair function call overhead.
      "jw" = {
        col_c  <- as.character(col); col_c[col_c == ""] <- NA_character_
        a_vals <- col_c[idx1]; b_vals <- col_c[idx2]
        mask   <- !is.na(a_vals) & !is.na(b_vals)
        result <- rep(NA_real_, n_pairs)
        if (any(mask))
          result[mask] <- 1 - stringdist::stringdist(
            a_vals[mask], b_vals[mask], method = "jw")
        result
      },

      "lv" = {
        col_c  <- as.character(col); col_c[col_c == ""] <- NA_character_
        a_vals <- col_c[idx1]; b_vals <- col_c[idx2]
        mask   <- !is.na(a_vals) & !is.na(b_vals)
        result <- rep(NA_real_, n_pairs)
        if (any(mask)) {
          a_m  <- a_vals[mask]; b_m <- b_vals[mask]
          lens <- pmax(nchar(a_m), nchar(b_m))
          dists <- stringdist::stringdist(a_m, b_m, method = "lv")
          result[mask] <- ifelse(lens > 0L, 1 - dists / lens, NA_real_)
        }
        result
      },

      # Jaccard and BoW require per-pair set operations; keep scalar vapply.
      "jaccard" = {
        col_c <- as.character(col)
        col_c[col_c == ""] <- NA_character_
        vapply(seq_len(n_pairs), function(k)
          .sim_jaccard(col_c[idx1[k]], col_c[idx2[k]]), numeric(1L))
      },

      "bow" = {
        col_c <- as.character(col)
        col_c[col_c == ""] <- NA_character_
        idf <- .bow_idf(col_c)
        vapply(seq_len(n_pairs), function(k)
          .sim_bow(col_c[idx1[k]], col_c[idx2[k]], idf), numeric(1L))
      },

      # Vectorised: exact string equality is natively vectorised in R.
      "categorical" = {
        col_c  <- as.character(col)
        a_vals <- col_c[idx1]; b_vals <- col_c[idx2]
        result <- as.numeric(a_vals == b_vals)
        result[is.na(a_vals) | is.na(b_vals)] <- NA_real_
        result
      },

      # Vectorised: arithmetic and exp() are already vectorised.
      "numeric" = {
        rng    <- num_ranges[[fname]]
        tau    <- sp$tau %||% 1
        a_vals <- as.numeric(col[idx1]); b_vals <- as.numeric(col[idx2])
        mask   <- !is.na(a_vals) & !is.na(b_vals) &
                  is.finite(rng) & rng > 0
        result <- rep(NA_real_, n_pairs)
        if (any(mask))
          result[mask] <- exp(-abs(a_vals[mask] - b_vals[mask]) / (rng * tau))
        result
      },

      "year" = {
        a_vals <- suppressWarnings(as.numeric(col[idx1]))
        b_vals <- suppressWarnings(as.numeric(col[idx2]))
        mask   <- !is.na(a_vals) & !is.na(b_vals)
        result <- rep(NA_real_, n_pairs)
        if (any(mask))
          result[mask] <- as.numeric(abs(a_vals[mask] - b_vals[mask]) <= 1)
        result
      },

      # Unknown type: all NA
      rep(NA_real_, n_pairs)
    )
  })

  names(out) <- vapply(spec, `[[`, character(1L), "name")
  attr(out, "n_records") <- n
  attr(out, "n_pairs")   <- n_pairs
  out
}

# ── Build a sparse n×n similarity matrix from pair similarities ───────────────

#' Build sparse similarity matrix from pair similarities
#'
#' Takes the combined similarity vector (output of \code{er_combine()}) and
#' assembles a symmetric sparse n×n matrix, which can then be passed directly
#' to graph-based clustering methods.
#'
#' @param pairs tibble(idx1, idx2) from \code{er_block()}.
#' @param sim_vec Numeric vector of combined similarities (length =
#'   \code{nrow(pairs)}).
#' @param n Integer. Total number of records.
#' @param na_fill Numeric. Value to substitute for \code{NA} similarities
#'   (default 0 — missing pairs treated as unrelated).
#'
#' @return A symmetric \code{dgCMatrix} with diagonal 1.
#' @export
er_pairs_to_sparse <- function(pairs, sim_vec, n, na_fill = 0) {
  sim_vec[is.na(sim_vec)] <- na_fill
  er_sparse_from_pairs(pairs$idx1, pairs$idx2, sim_vec, n)
}
