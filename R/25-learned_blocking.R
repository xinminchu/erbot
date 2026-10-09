########################################
# File: R/25-learned_blocking.R
# Learned blocking via a neural retriever (base R only, no new deps).
#
# Research idea: train the retriever for the *blocking* objective
# (recall@K -- don't miss true duplicates in each record's top-K),
# not for pairwise classification accuracy.
#
# er_char_ngrams()    -- hashed character n-gram record vectors
# er_train_retriever()-- MLP encoder + InfoNCE training on dup pairs
# er_embed_records()  -- forward pass -> L2-normalized embeddings
# er_block_embed()    -- top-K cosine retrieval -> candidate pairs
# er_recall_at_k()    -- reporting metric (truth used once, at the end)
#
# Honesty discipline:
#   training may use truth-labeled pairs (like any supervised model);
#   K is chosen from the pair budget alone -- truth never selects K;
#   recall@K is reported once, at the end.
########################################

#' Hashed character n-gram record vectors
#'
#' Each record's text fields are concatenated, split into character
#' n-grams, and hashed into a fixed-width count vector (hashing trick),
#' with sublinear TF scaling.
#'
#' @param texts Character vector of record texts (length n).
#' @param n Integer. n-gram order. Default 3.
#' @param dim Integer. Hash width. Default 512.
#' @return Numeric matrix (n x dim).
#' @export
er_char_ngrams <- function(texts, n = 3L, dim = 512L) {
  n <- as.integer(n); dim <- as.integer(dim)
  nr <- length(texts)
  X <- matrix(0, nr, dim)
  for (r in seq_len(nr)) {
    t <- texts[[r]]
    if (is.na(t) || !nzchar(t)) next
    t <- paste0(" ", tolower(t), " ")
    L <- nchar(t)
    if (L < n) next
    seen <- integer(0)
    for (i in seq_len(L - n + 1L)) {
      g <- substr(t, i, i + n - 1L)
      h <- 0
      for (ch in strsplit(g, "", fixed = TRUE)[[1L]]) {
        h <- (h * 31 + utf8ToInt(ch)) %% 4294967296
      }
      seen <- c(seen, as.integer(h %% dim) + 1L)
    }
    if (length(seen)) {
      tb <- tabulate(seen, nbins = dim)
      X[r, ] <- log1p(tb)
    }
  }
  X
}

#' Train a neural retriever for blocking
#'
#' Trains a small MLP encoder (dim -> hidden -> dim_out, tanh, L2-normalized
#' output) with the InfoNCE loss: each duplicate pair should rank each other
#' at the top of the batch. This optimizes recall@K, the blocking objective --
#' not classification accuracy.
#'
#' @param X Numeric matrix (n x dim_in) of record vectors, e.g. from
#'   \code{er_char_ngrams()}.
#' @param dup_i,dup_j Integer vectors of known-duplicate pair indices
#'   (1-based, same length).
#' @param dim_out Integer. Embedding dimension. Default 64.
#' @param hidden Integer. Hidden layer width. Default 128.
#' @param epochs Integer. Training epochs. Default 20.
#' @param batch Integer. Batch size. Default 256.
#' @param lr Numeric. Learning rate. Default 0.05.
#' @param temperature Numeric. InfoNCE temperature. Default 0.1.
#' @param seed Integer. RNG seed.
#' @return A model list (weights + dims) for \code{er_embed_records()}.
#' @export
er_train_retriever <- function(X, dup_i, dup_j,
                               dim_out = 64L, hidden = 128L,
                               epochs = 20L, batch = 256L,
                               lr = 0.05, temperature = 0.1,
                               seed = 42L) {
  stopifnot(is.matrix(X), length(dup_i) == length(dup_j))
  if (length(dup_i) < 4L)
    stop("retriever needs >= 4 duplicate pairs to train (got ",
         length(dup_i), ").")
  set.seed(seed)
  n <- nrow(X); d_in <- ncol(X)
  hidden <- as.integer(hidden); dim_out <- as.integer(dim_out)

  W1 <- matrix(rnorm(d_in * hidden, sd = sqrt(1 / d_in)), d_in, hidden)
  b1 <- rep(0, hidden)
  W2 <- matrix(rnorm(hidden * dim_out, sd = sqrt(1 / hidden)), hidden, dim_out)
  b2 <- rep(0, dim_out)

  # adjacency: known duplicates per record
  pos_of <- vector("list", n)
  for (t in seq_along(dup_i)) {
    i <- dup_i[t]; j <- dup_j[t]
    pos_of[[i]] <- c(pos_of[[i]], j)
    pos_of[[j]] <- c(pos_of[[j]], i)
  }

  fwd <- function(Xb) {
    z1 <- sweep(Xb %*% W1, 2L, b1, "+")
    a1 <- tanh(z1)
    z2 <- sweep(a1 %*% W2, 2L, b2, "+")
    nrm <- sqrt(rowSums(z2^2)) + 1e-9
    list(a1 = a1, e = z2 / nrm)
  }

  for (ep in seq_len(epochs)) {
    idx <- sample.int(n)
    for (s in seq(1L, n, by = batch)) {
      anch <- idx[seq(s, min(s + batch - 1L, n))]
      anch <- anch[vapply(pos_of[anch], function(v) length(v) > 0L, logical(1L))]
      if (length(anch) < 2L) next
      pos <- vapply(anch, function(a) {
        v <- pos_of[[a]]; v[sample.int(length(v), 1L)]
      }, integer(1L))
      B <- length(anch)
      fa <- fwd(X[anch, , drop = FALSE])
      fp <- fwd(X[pos, , drop = FALSE])
      S <- (fa$e %*% t(fp$e)) / temperature
      # InfoNCE: softmax over columns, diagonal = positives
      m <- apply(S, 1L, max)
      E <- exp(sweep(S, 1L, m, "-"))
      P <- E / rowSums(E)
      dS <- P
      dS[cbind(seq_len(B), seq_len(B))] <- dS[cbind(seq_len(B), seq_len(B))] - 1
      dS <- dS / B / temperature
      # backprop into both encodings
      for (fwd_out in list(list(e = fa$e, a1 = fa$a1, Xb = X[anch, , drop = FALSE]),
                           list(e = fp$e, a1 = fp$a1, Xb = X[pos, , drop = FALSE]))) {
        e_ <- fwd_out$e; a1_ <- fwd_out$a1; Xb <- fwd_out$Xb
        de <- if (identical(e_, fa$e)) dS %*% fp$e else t(dS) %*% fa$e
        z2pre <- sweep(a1_ %*% W2, 2L, b2, "+")
        nrm <- sqrt(rowSums(z2pre^2)) + 1e-9
        dz2 <- sweep(de - e_ * rowSums(e_ * de), 1L, nrm, "/")
        dW2 <- t(a1_) %*% dz2
        db2 <- colSums(dz2)
        da1 <- dz2 %*% t(W2)
        dz1 <- da1 * (1 - a1_^2)
        dW1 <- t(Xb) %*% dz1
        db1 <- colSums(dz1)
        W2 <<- W2 - lr * dW2
        b2 <<- b2 - lr * db2
        W1 <<- W1 - lr * dW1
        b1 <<- b1 - lr * db1
      }
    }
  }
  list(W1 = W1, b1 = b1, W2 = W2, b2 = b2,
       dim_in = d_in, hidden = hidden, dim_out = dim_out,
       trained = TRUE)
}

#' Embed records with a trained retriever
#'
#' @param model Model list from \code{er_train_retriever()}.
#' @param X Numeric matrix of record vectors.
#' @return Numeric matrix (n x dim_out) of L2-normalized embeddings.
#' @export
er_embed_records <- function(model, X) {
  z1 <- sweep(X %*% model$W1, 2L, model$b1, "+")
  a1 <- tanh(z1)
  z2 <- sweep(a1 %*% model$W2, 2L, model$b2, "+")
  z2 / (sqrt(rowSums(z2^2)) + 1e-9)
}

#' K for a pair budget (honest: no truth used)
#'
#' Expected pairs ~= n*K/2 <= budget -> K = floor(2*budget / n).
#' @export
er_k_for_budget <- function(n, budget) {
  max(1L, as.integer((2 * budget) %/% max(n, 1L)))
}

#' Blocking via learned-retriever top-K search
#'
#' Embeds all records and retrieves each record's top-K cosine neighbors as
#' candidate pairs. K comes from \code{max_pairs} via \code{er_k_for_budget()}
#' unless \code{k} is given explicitly.
#'
#' @param data A \code{data.frame} of records.
#' @param fields Character vector of text columns to encode.
#' @param model Model list from \code{er_train_retriever()}, or \code{NULL}
#'   for untrained n-gram cosine retrieval (no learning, still valid).
#' @param k Integer. Neighbors per record. Default from \code{max_pairs}.
#' @param max_pairs Numeric. Pair budget. Default 2e6.
#' @param ngram_dim Integer. Hash width for \code{er_char_ngrams()}.
#' @return A \code{tibble} with \code{idx1}, \code{idx2} (1-based, idx1 < idx2).
#' @export
er_block_embed <- function(data, fields, model = NULL,
                           k = NULL, max_pairs = 2e6,
                           ngram_dim = 512L) {
  df <- as.data.frame(data, stringsAsFactors = FALSE)
  n <- nrow(df)
  if (n < 2L) {
    return(tibble::tibble(idx1 = integer(0), idx2 = integer(0)))
  }
  texts <- do.call(paste, c(lapply(fields, function(f) {
    v <- df[[f]]; v[is.na(v)] <- ""; as.character(v)
  }), sep = " "))
  X <- er_char_ngrams(texts, dim = ngram_dim)
  E <- if (is.null(model)) {
    E0 <- X / (sqrt(rowSums(X^2)) + 1e-9)
    E0[is.nan(E0)] <- 0
    E0
  } else {
    er_embed_records(model, X)
  }
  if (is.null(k)) k <- er_k_for_budget(n, max_pairs)
  k <- max(1L, min(as.integer(k), n - 1L))

  # chunked top-K (n = 10k stays in memory)
  chunk <- 1000L
  seen <- new.env(hash = TRUE, parent = emptyenv())
  for (s in seq(1L, n, by = chunk)) {
    e <- min(s + chunk - 1L, n)
    S <- E[s:e, , drop = FALSE] %*% t(E)
    rows <- s:e
    S[cbind(seq_along(rows), rows)] <- -Inf
    kk <- min(k, n - 1L)
    for (r in seq_along(rows)) {
      top <- order(S[r, ], decreasing = TRUE)[seq_len(kk)]
      i <- rows[r]
      for (j in top) {
        a <- min(i, j); b <- max(i, j)
        seen[[paste(a, b, sep = "-")]] <- TRUE
      }
    }
  }
  keys <- ls(seen)
  if (!length(keys)) {
    return(tibble::tibble(idx1 = integer(0), idx2 = integer(0)))
  }
  parts <- strsplit(keys, "-", fixed = TRUE)
  tibble::tibble(idx1 = as.integer(vapply(parts, `[[`, "", 1L)),
                 idx2 = as.integer(vapply(parts, `[[`, "", 2L)))
}

#' Recall@K of a blocking (reporting metric)
#'
#' Fraction of true duplicate pairs present in the candidate pairs.
#' Truth is used once here, at the end -- never for selecting K.
#'
#' @param pairs A \code{data.frame} with \code{idx1}, \code{idx2}.
#' @param truth_vec Integer vector of ground-truth entity labels.
#' @return Numeric in [0, 1], or \code{NA} if no duplicate pairs exist.
#' @export
er_recall_at_k <- function(pairs, truth_vec) {
  if (!nrow(pairs)) return(NA_real_)
  cand <- paste(pmin(pairs$idx1, pairs$idx2), pmax(pairs$idx1, pairs$idx2), sep = "-")
  dup <- .dup_pairs_of(truth_vec)
  if (is.null(dup) || !length(dup)) return(NA_real_)
  mean(dup %in% cand)
}

#' All true duplicate pair keys from a truth label vector
#' @keywords internal
.dup_pairs_of <- function(truth_vec) {
  tl <- split(seq_along(truth_vec), truth_vec)
  unlist(lapply(tl, function(m) {
    if (length(m) < 2L) return(NULL)
    cmb <- utils::combn(m, 2L)
    paste(pmin(cmb[1L, ], cmb[2L, ]), pmax(cmb[1L, ], cmb[2L, ]), sep = "-")
  }))
}

#' Honest blocking evaluation: entity-level train/test split
#'
#' Trains the retriever on train entities' duplicate pairs, embeds all
#' records, retrieves top-K over the full data (as in production), and
#' reports recall@K on test entities' duplicate pairs only.
#'
#' This is the number you can put in a paper: it answers "a retriever
#' trained on old labeled data, how well does it block new data?"
#' In-sample recall (train and report on the same pairs) is optimistic
#' and must not be reported as blocking quality.
#'
#' @param X Numeric matrix of record vectors (e.g. from
#'   \code{er_char_ngrams()}).
#' @param entity_labels Integer vector of ground-truth entity labels.
#' @param k Integer. Neighbors per record for retrieval.
#' @param test_size Numeric. Fraction of entities held out for evaluation.
#' @param seed Integer. RNG seed.
#' @param ... Passed to \code{er_train_retriever()}.
#' @return A named list: recall, k, n_pairs, n_train/test_pairs/entities.
#' @export
er_blocking_heldout <- function(X, entity_labels, k,
                                test_size = 0.3, seed = 42L, ...) {
  stopifnot(is.matrix(X))
  entities <- sort(unique(entity_labels[!is.na(entity_labels)]))
  if (length(entities) < 2L)
    stop("held-out eval needs >= 2 truth entities.")
  set.seed(seed)
  n_test <- max(1L, as.integer(length(entities) * test_size))
  test_ents <- sample(entities, n_test)

  tl <- split(seq_along(entity_labels), entity_labels)
  tl <- tl[names(tl) != "NA" & !is.na(names(tl))]
  test_names <- as.character(test_ents)
  train_pairs <- list(); test_pairs <- list()
  for (e in names(tl)) {
    m <- tl[[e]]
    if (length(m) < 2L) next
    cmb <- utils::combn(m, 2L)
    pl <- lapply(seq_len(ncol(cmb)), function(j) c(cmb[1L, j], cmb[2L, j]))
    if (e %in% test_names) test_pairs <- c(test_pairs, pl)
    else train_pairs <- c(train_pairs, pl)
  }
  if (length(train_pairs) < 4L)
    stop("too few train duplicate pairs (", length(train_pairs), ").")
  if (!length(test_pairs))
    stop("no test duplicate pairs; increase data or test_size.")

  train_i <- vapply(train_pairs, `[`, integer(1L), 1L)
  train_j <- vapply(train_pairs, `[`, integer(1L), 2L)
  model <- er_train_retriever(X, train_i, train_j, seed = seed, ...)
  E <- er_embed_records(model, X)
  n <- nrow(X)
  k <- max(1L, min(as.integer(k), n - 1L))
  # chunked top-K retrieval over all records (as in production)
  chunk <- 1000L
  seen <- new.env(hash = TRUE, parent = emptyenv())
  for (s in seq(1L, n, by = chunk)) {
    e <- min(s + chunk - 1L, n)
    S <- E[s:e, , drop = FALSE] %*% t(E)
    rows <- s:e
    S[cbind(seq_along(rows), rows)] <- -Inf
    kk <- min(k, n - 1L)
    for (r in seq_along(rows)) {
      top <- order(S[r, ], decreasing = TRUE)[seq_len(kk)]
      i <- rows[r]
      for (j in top) {
        a <- min(i, j); b <- max(i, j)
        seen[[paste(a, b, sep = "-")]] <- TRUE
      }
    }
  }
  cand <- ls(seen)
  test_keys <- vapply(test_pairs, function(p) {
    paste(min(p), max(p), sep = "-")
  }, character(1L))
  list(recall = mean(test_keys %in% cand),
       k = k,
       n_pairs = length(cand),
       n_train_pairs = length(train_pairs),
       n_test_pairs = length(test_pairs),
       n_train_entities = length(entities) - n_test,
       n_test_entities = n_test)
}
