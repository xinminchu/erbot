########################################
# File: R/00-utils.R
# Merged utilities: internal helpers + shared math primitives.
# Replaces old 00-utils.R + 05-internal-helpers.R.
########################################

#' erbot: A Unified Entity Resolution Pipeline
#'
#' @description
#' End-to-end entity resolution (deduplication and record linkage) for tabular
#' data. One call to \code{\link{er_run}()} runs the full nine-stage pipeline:
#' loading, diagnosis, blocking, per-field similarity, NA-aware combination,
#' clustering, merging, evaluation, and reporting.
#'
#' @docType package
#' @name erbot-package
#' @aliases erbot
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr bind_rows bind_cols transmute filter mutate distinct
#'   left_join arrange slice desc
#' @importFrom tibble tibble as_tibble as_tibble_row
#' @importFrom stringr str_replace_all str_trim str_squish
#' @importFrom stringi stri_trans_nfkc
#' @importFrom Matrix sparseMatrix Diagonal summary nnzero diag
#' @importFrom igraph graph_from_data_frame components E V membership
#'   cluster_louvain cluster_louvain cluster_leiden cluster_label_prop
#'   add_vertices make_empty_graph simplify ecount vcount is.igraph
#'   induced_subgraph modularity ends mst graph_from_edgelist
#' @importFrom stats kmeans hclust cutree as.dist var sd median mad
#'   dbeta setNames
#' @importFrom cluster pam silhouette
#' @importFrom stringdist stringdist stringdistmatrix
#' @importFrom irlba irlba
#' @importFrom readr write_csv read_delim read_lines
#' @importFrom readxl read_excel
#' @importFrom data.table fread rbindlist
#' @importFrom tools file_ext
#' @importFrom utils head tail
#' @importFrom methods is
NULL

# ── Null-coalescing ────────────────────────────────────────────────────────────

#' Null-coalescing operator
#'
#' Returns \code{a} if non-\code{NULL}, otherwise \code{b}.
#'
#' @param a Any R object.
#' @param b Fallback value returned when \code{a} is \code{NULL}.
#' @return \code{a} if \code{!is.null(a)}, else \code{b}.
#' @keywords internal
`%||%` <- function(a, b) if (!is.null(a)) a else b

#' Silent try
#'
#' Wraps \code{\link[base]{try}} with \code{silent = TRUE}.
#'
#' @param expr Expression to evaluate.
#' @return The result of \code{expr}, or a \code{try-error} object.
#' @keywords internal
try_silent <- function(expr) try(expr, silent = TRUE)

# ── Cosine distance ────────────────────────────────────────────────────────────

#' Cosine distance matrix
#'
#' Returns a \code{dist} object of pairwise cosine distances between rows of
#' \code{X}. Compatible with \code{stats::hclust}, \code{cluster::pam}, and
#' \code{cluster::silhouette}.
#'
#' @param X Numeric matrix (rows = items).
#' @return \code{dist} object of cosine distances (diagonal = 0).
#' @export
er_cosine_dist <- function(X) {
  X  <- as.matrix(X)
  nr <- sqrt(rowSums(X^2)); nr[nr == 0] <- 1
  X  <- X / nr
  D  <- 1 - (X %*% t(X))
  stats::as.dist(pmax(D, 0))
}

# ── Average silhouette ─────────────────────────────────────────────────────────

#' Average silhouette width
#'
#' Computes the mean silhouette coefficient for a clustering given a
#' precomputed distance matrix or \code{dist} object.
#'
#' @param labels Integer vector of cluster labels.
#' @param D Distance matrix (numeric matrix or \code{dist} object).
#' @return Numeric scalar: mean silhouette width, or \code{NA_real_} if not
#'   computable.
#' @export
er_silhouette_avg <- function(labels, D) {
  labs <- as.integer(labels)
  k    <- length(unique(labs[!is.na(labs)]))
  n    <- length(labs)
  if (k < 2L || k >= n) return(NA_real_)
  if (!inherits(D, "dist")) D <- stats::as.dist(D)
  sil <- tryCatch(cluster::silhouette(labs, D), error = function(e) NULL)
  if (is.null(sil) || !is.matrix(sil)) return(NA_real_)
  mean(sil[, 3L], na.rm = TRUE)
}

# ── Pairwise string distance ───────────────────────────────────────────────────

#' Blocked pairwise string distance matrix
#'
#' Computes an n x n string distance matrix. For large n, uses row/column
#' blocks of size \code{block} to limit peak memory.
#'
#' @param text_vec Character vector of strings.
#' @param method String distance method (default \code{"jw"}).
#' @param block Integer; block size for chunked computation (default 4000).
#' @param progress Optional \code{er_progress} object (unused; kept for API
#'   compatibility).
#' @return Numeric matrix (n x n); diagonal is 0.
#' @export
er_pairwise_stringdist <- function(text_vec,
                                   method   = "jw",
                                   block    = 4000L,
                                   progress = NULL) {
  n <- length(text_vec)
  if (n == 0L) return(matrix(numeric(0L), 0L, 0L))
  if (n <= block) {
    D <- as.matrix(stringdist::stringdistmatrix(text_vec, text_vec,
                                                method = method))
    diag(D) <- 0
    return(D)
  }
  D      <- matrix(0, n, n)
  starts <- seq(1L, n, by = block)
  for (i in starts) {
    ie <- min(i + block - 1L, n)
    for (j in starts[starts >= i]) {
      je  <- min(j + block - 1L, n)
      blk <- as.matrix(stringdist::stringdistmatrix(text_vec[i:ie],
                                                     text_vec[j:je],
                                                     method = method))
      D[i:ie, j:je] <- blk
      if (i != j) D[j:je, i:ie] <- t(blk)
    }
  }
  diag(D) <- 0
  D
}

# ── Auto-detect text fields ────────────────────────────────────────────────────

#' Auto-detect free-text fields
#'
#' Heuristically selects character/factor columns that look like free-text,
#' excluding known ID and embedding columns.
#'
#' @param df data.frame with lower-cased column names.
#' @param id_candidates Character vector of probable ID column names to exclude.
#' @param embed_candidates Character vector of embedding column names to exclude.
#' @param max_auto_fields Maximum number of fields to return (default 5).
#' @return Character vector of selected field names.
#' @export
er_guess_text_fields <- function(df, id_candidates, embed_candidates,
                                 max_auto_fields = 5L) {
  nms     <- names(df)
  exclude <- union(tolower(id_candidates), tolower(embed_candidates))
  cands   <- nms[!nms %in% exclude]
  if (!length(cands)) return(character(0L))
  is_text <- vapply(cands, function(cn) {
    col <- df[[cn]]
    (is.character(col) || is.factor(col)) && mean(is.na(col)) < 0.9
  }, logical(1L))
  text_cols <- cands[is_text]
  if (!length(text_cols)) return(character(0L))
  if (length(text_cols) > max_auto_fields) {
    avg_len <- vapply(text_cols, function(cn) {
      mean(nchar(as.character(df[[cn]])), na.rm = TRUE)
    }, numeric(1L))
    text_cols <- text_cols[order(avg_len, decreasing = TRUE)[seq_len(max_auto_fields)]]
  }
  text_cols
}

# ── Sparse matrix helpers ──────────────────────────────────────────────────────

#' Build a symmetric sparse similarity matrix from pair indices and values
#'
#' @param i Integer vector of row indices (1-based).
#' @param j Integer vector of column indices (1-based).
#' @param x Numeric vector of similarity values.
#' @param n Total number of records (matrix dimension).
#' @return A symmetric \code{dgCMatrix} with diagonal 1.
#' @export
er_sparse_from_pairs <- function(i, j, x, n) {
  keep <- !is.na(x) & is.finite(x)
  if (!any(keep)) return(Matrix::Diagonal(n = n, x = 1))
  i <- i[keep]; j <- j[keep]; x <- x[keep]
  # symmetrize
  both_i <- c(i, j); both_j <- c(j, i); both_x <- c(x, x)
  S <- Matrix::sparseMatrix(i = both_i, j = both_j, x = both_x,
                             dims = c(n, n), repr = "C")
  Matrix::diag(S) <- 1
  S
}

# ── Misc string utilities ──────────────────────────────────────────────────────

#' Normalise a text vector
#'
#' Applies NFKC Unicode normalisation, lower-casing, and whitespace squishing.
#' Empty strings are replaced with a single space.
#'
#' @param x Character vector to normalise.
#' @return Character vector of the same length as \code{x}.
#' @keywords internal
er_normalize_text <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- stringi::stri_trans_nfkc(x)
  x <- tolower(x)
  x <- stringr::str_replace_all(x, "\\s+", " ")
  x <- stringr::str_trim(x)
  x[x == ""] <- " "
  x
}
