########################################
# File: R/20-embed.R
# Type-1 embedding-based similarities for multi-view ER.
#
# er_embed()          -- main entry point; mirrors er_similarity() output format
# .embed_text_field   -- internal: encode one field with BERT and compute cosine
# .embed_cache_path   -- internal: .rds cache file path helper
# .embed_cosine_pairs -- internal: cosine similarity from dense matrix rows
########################################

#' BERT embedding cosine similarities for candidate pairs (Type-1)
#'
#' Computes per-field cosine similarities using pre-trained transformer
#' embeddings (default: \code{bert-base-uncased}, CLS-token pooling,
#' \eqn{d=768}), producing the same \emph{named list of numeric vectors} as
#' \code{er_similarity()} (Type-3) and \code{er_bow()} (Type-2).
#'
#' \strong{Dependencies:}
#' \itemize{
#'   \item The \pkg{text} R package (\code{install.packages("text")}) and its
#'     Python back-end (\code{text::textrpp_install()}).
#'   \item A CUDA-capable GPU is recommended for datasets with \eqn{n > 1000};
#'     CPU fallback is supported but slow.
#' }
#'
#' \strong{Caching:}
#' Embeddings are expensive to compute.  When \code{cache_dir} is set, the
#' function saves each field's embedding matrix to
#' \code{<cache_dir>/<dataset>_<field>_embed.rds} and reloads it on
#' subsequent calls (checking that the number of records matches).  Set
#' \code{force_recompute = TRUE} to bypass the cache.
#'
#' \strong{Algorithm:}
#' \enumerate{
#'   \item For each field, pass all \eqn{n} text values through the transformer
#'     model, extracting the CLS-token embedding (dimension \eqn{d}).
#'   \item L2-normalize each embedding row.
#'   \item For each candidate pair \eqn{(i,j)}, cosine similarity =
#'     \eqn{\mathbf{e}_i^{\top}\mathbf{e}_j} (inner product of unit vectors).
#'   \item Return \code{NA_real_} for pairs where either record has an empty
#'     or missing field value.
#' }
#'
#' @param df \code{data.frame} of records.
#' @param pairs \code{tibble(idx1, idx2)} from \code{er_block()}.
#' @param fields Character vector of column names to embed.  If \code{NULL},
#'   all character/factor columns are used.
#' @param model Character.  Hugging Face model name or local path.
#'   Default \code{"bert-base-uncased"}.
#' @param layers Integer.  Which transformer layer to pool from.  Passed to
#'   \code{text::textEmbed()}.  \code{-2} (second-to-last) is often better
#'   than \code{-1} (last) for similarity tasks.  Default \code{-2L}.
#' @param pooling Character.  Pooling strategy: \code{"cls"} (CLS token,
#'   default) or \code{"mean"} (mean of all tokens).
#' @param batch_size Integer.  Records per inference batch.  Reduce if GPU OOM.
#'   Default \code{32L}.
#' @param cache_dir Character.  Directory to cache embedding .rds files.
#'   \code{NULL} disables caching (default).
#' @param dataset_tag Character.  Short tag included in the cache file name.
#'   Default \code{"dataset"}.
#' @param force_recompute Logical.  Ignore existing cache and recompute.
#'   Default \code{FALSE}.
#' @param na_fill Value for pairs where either record is missing the field.
#'   Default \code{NA_real_}.
#' @param verbose Logical.  Print per-field progress.  Default \code{TRUE}.
#'
#' @return Named list of numeric vectors (one per field in \code{fields}),
#'   each of length \code{nrow(pairs)}.  \code{NA_real_} signals missing data.
#'
#' @seealso \code{\link{er_similarity}}, \code{\link{er_bow}},
#'   \code{\link{er_compare_paradigms}}
#'
#' @examples
#' \dontrun{
#' # Requires: install.packages("text"); text::textrpp_install()
#' df    <- er_load("restaurant")
#' diag  <- er_diagnose(df)
#' pairs <- er_block(df, diag)
#'
#' sim_embed <- er_embed(
#'   df        = df,
#'   pairs     = pairs,
#'   fields    = c("name", "address"),
#'   cache_dir = "cache/",
#'   dataset_tag = "restaurant"
#' )
#'
#' # Plug directly into the Strategy A pipeline
#' wt  <- er_weights(sim_embed)
#' S   <- er_pairs_to_sparse(pairs, er_combine(sim_embed, wt), nrow(df))
#' M   <- er_classify(S, method = "gmm")
#' labs <- er_cluster(M, method = "threshold_cc", threshold = 0.5)
#' }
#' @export
er_embed <- function(df,
                     pairs,
                     fields          = NULL,
                     model           = "bert-base-uncased",
                     layers          = -2L,
                     pooling         = c("cls", "mean"),
                     batch_size      = 32L,
                     cache_dir       = NULL,
                     dataset_tag     = "dataset",
                     force_recompute = FALSE,
                     na_fill         = NA_real_,
                     verbose         = TRUE) {

  pooling <- match.arg(pooling)

  stopifnot(is.data.frame(df),
            is.data.frame(pairs),
            all(c("idx1", "idx2") %in% names(pairs)))

  if (!requireNamespace("text", quietly = TRUE))
    stop(
      "er_embed requires the 'text' package.\n",
      "  Install:  install.packages(\"text\")\n",
      "  Setup:    text::textrpp_install()\n",
      "  Activate: text::textrpp_initialize()"
    )

  # Resolve fields
  if (is.null(fields)) {
    is_text <- vapply(names(df), function(cn) {
      col <- df[[cn]]
      (is.character(col) || is.factor(col)) && mean(is.na(col)) < 0.9
    }, logical(1L))
    fields <- names(df)[is_text]
  }

  if (!length(fields))
    stop("er_embed: no text fields to embed. Provide 'fields' explicitly.")

  missing_cols <- setdiff(fields, names(df))
  if (length(missing_cols))
    stop("er_embed: columns not found in df: ",
         paste(missing_cols, collapse = ", "))

  n_records <- nrow(df)
  n_pairs   <- nrow(pairs)

  # Optionally create cache directory
  if (!is.null(cache_dir) && !dir.exists(cache_dir))
    dir.create(cache_dir, recursive = TRUE)

  result <- lapply(fields, function(fname) {
    if (verbose) message(sprintf("[er_embed] Field '%s' ...", fname))

    col <- as.character(df[[fname]])
    col[is.na(col)] <- ""

    ok_pairs <- nchar(col[pairs$idx1]) > 0L & nchar(col[pairs$idx2]) > 0L

    # Attempt to load from cache
    embed_mat <- NULL
    cache_path <- if (!is.null(cache_dir))
      .embed_cache_path(cache_dir, dataset_tag, fname)
    else
      NULL

    if (!force_recompute && !is.null(cache_path) && file.exists(cache_path)) {
      cached <- tryCatch(readRDS(cache_path), error = function(e) NULL)
      if (!is.null(cached) && is.matrix(cached) && nrow(cached) == n_records) {
        if (verbose) message(sprintf("  Loaded from cache: %s", cache_path))
        embed_mat <- cached
      } else {
        if (verbose) message(sprintf("  Cache mismatch; recomputing."))
      }
    }

    # Compute embeddings if not cached
    if (is.null(embed_mat)) {
      embed_mat <- tryCatch(
        .embed_text_field(col, model = model, layers = layers,
                          pooling = pooling, batch_size = batch_size,
                          verbose = verbose),
        error = function(e) {
          warning(sprintf("er_embed: embedding failed for field '%s': %s",
                          fname, conditionMessage(e)))
          NULL
        }
      )

      if (!is.null(embed_mat) && !is.null(cache_path)) {
        tryCatch(saveRDS(embed_mat, cache_path),
                 error = function(e)
                   warning("er_embed: could not save cache: ", conditionMessage(e)))
        if (verbose) message(sprintf("  Saved to cache: %s", cache_path))
      }
    }

    if (is.null(embed_mat)) {
      return(rep(na_fill, n_pairs))
    }

    # Cosine similarities for candidate pairs
    sims <- .embed_cosine_pairs(embed_mat, pairs$idx1, pairs$idx2)
    sims[!ok_pairs] <- na_fill
    sims
  })

  names(result) <- fields
  result
}

# ── Internal: encode a single field via the text package ──────────────────────

# Returns an n_records x d numeric matrix (L2-normalised rows).
# Each row is the CLS or mean-pooled embedding for one record.
.embed_text_field <- function(texts, model, layers, pooling, batch_size,
                               verbose) {
  n <- length(texts)

  # text::textEmbed() can handle batching internally; we just call it.
  # It returns a list with element 'x' (n x d data.frame).
  if (verbose) message(sprintf("  Encoding %d texts with model '%s'...", n, model))

  embed_df <- text::textEmbed(
    texts      = texts,
    model      = model,
    layers     = layers,
    aggregation_from_layers_to_tokens = if (pooling == "cls") "all"
                                        else "mean",
    aggregation_from_tokens_to_texts  = if (pooling == "cls") "CLS"
                                        else "mean",
    batch_size = batch_size,
    show_progress = verbose
  )

  # text::textEmbed returns a list; the embedding matrix is in embed_df$x
  # (a data.frame of numeric columns named "Dim1", "Dim2", ...).
  raw <- embed_df$x
  if (is.data.frame(raw)) raw <- as.matrix(raw)
  if (!is.matrix(raw) || nrow(raw) != n)
    stop(sprintf("er_embed: unexpected output from text::textEmbed (nrow=%d, expected %d).",
                 nrow(raw), n))

  # L2-normalize each row
  row_norms <- sqrt(rowSums(raw^2))
  row_norms[row_norms == 0] <- 1
  raw / row_norms
}

# ── Internal: cosine similarities for candidate pairs ─────────────────────────

# Takes an n x d L2-normalised matrix and two index vectors.
# Returns cosine similarities (dot products of unit-norm rows).
.embed_cosine_pairs <- function(embed_mat, idx1, idx2) {
  n_pairs <- length(idx1)
  if (n_pairs == 0L) return(numeric(0L))

  # Row-wise dot products via element-wise product + rowSums
  rowSums(embed_mat[idx1, , drop = FALSE] * embed_mat[idx2, , drop = FALSE])
}

# ── Internal: cache file path ─────────────────────────────────────────────────

.embed_cache_path <- function(cache_dir, dataset_tag, field_name) {
  tag   <- gsub("[^[:alnum:]_-]", "_", dataset_tag)
  fname <- gsub("[^[:alnum:]_-]", "_", field_name)
  file.path(cache_dir, sprintf("%s_%s_embed.rds", tag, fname))
}
