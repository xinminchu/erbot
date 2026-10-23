# RELATED-METHODS COMPARISON v1: meta-blocking, adaptive blocking, and other
# published blocking families, run on the SAME protocol as the E1/E2 v2
# harness so the numbers can sit in one table.
#
# Methods (all evaluated on the TEST split, once):
#   M0  BASELINE fixed-k            (the v2 baseline; defines budget B)
#   M1  GLOBAL-TOPB                 global similarity threshold: rank the
#                                   whole candidate pool by cosine, take
#                                   top B. (The "pool mechanics" control.)
#   M2  META-BLOCKING               token blocking -> block graph, edge
#                                   weights CBS (common-block count) and
#                                   JS (Jaccard of block sets); oversized
#                                   blocks purged (df > 5% of the subset).
#                                   Reported @B by weight rank (CEP-style)
#                                   for both schemes, plus native WNP
#                                   (per-node above-mean pruning, CBS).
#   M3  ADAPTIVE-K                  per-record relative threshold: keep pool
#                                   neighbours with w >= alpha * (record's
#                                   best w). alpha tuned on TRAIN so the
#                                   train pair count matches B_train.
#   M4  SORTED NEIGHBORHOOD         sort records by normalised text key,
#                                   sliding window; window tuned on TRAIN
#                                   to match B_train.
#   M5  CANOPY                      greedy canopies on cosine (loose T2,
#                                   tight T1 = T2 + 0.05); T2 tuned on
#                                   TRAIN to match B_train.
#   M6  MARG-ALLOC A/B              the E2 waterfill treatments, re-run
#                                   in-script (no permutation here -- the
#                                   attribution test lives in
#                                   exp-e2-waterfill-v2.R; baselines make
#                                   no attribution claim, PC@B only).
#
# Every method reports NATIVE (pair count, PC) and, where it differs,
# ALIGNED PC at exactly B pairs (trim by the method's own score, top up
# from the pool by cosine -- the v2 alignment discipline).
#
# Harness identity with exp-e2-waterfill-v2.R: sections 0-5 below are
# copied verbatim from v2.2 (loaders/shims, text/shingles, split seed
# 20261016, k* tuning grid, both calibrations, waterfill). Self-check on
# cora: BASELINE pairs/PC and MARG-ALLOC PC must reproduce the archived
# v2 numbers exactly (20898 pairs, PC 0.5865; PC 0.9092 / 0.9092).
#
# Usage (from the erbot package root):
#   Rscript experiments/exp-related-baselines-v1.R cora
#   Rscript experiments/exp-related-baselines-v1.R d10k
#   Rscript experiments/exp-related-baselines-v1.R musicbrainz20k

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1L) args[1L] else "cora"
SEED <- 20261016L
M_POOL <- 200L   # per-record candidate pool water-filling selects from

PKG_ATTACHED <- FALSE
if (requireNamespace("pkgload", quietly = TRUE)) {
  suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
  PKG_ATTACHED <- TRUE
} else if (requireNamespace("erbot", quietly = TRUE)) {
  suppressPackageStartupMessages(library(erbot))
  PKG_ATTACHED <- TRUE
}
if (!PKG_ATTACHED) {
  # Standalone mode (this VM has R but not the installed package / its full
  # Imports). Replicas of exactly the three package entry points this
  # harness calls, copied from the package sources so file reads, gold
  # partitions and metric semantics are identical:
  #   er_load        R/01-load.R  (cora + d10k branches only)
  #   er_load_gold   R/14-datasets.R + er_pairs_to_clusters (R/09-evaluate.R)
  #   er_block_stats R/03-block.R (verbatim)
  # Partition note: cluster *labels* may differ from igraph's numbering;
  # the split and all metrics depend only on the partition (see header).
  message("standalone mode: local replicas of er_load / er_load_gold / er_block_stats")
  .find_extdata <- function(fname, data_names = character()) {
    for (p in c(file.path("inst", "extdata", fname),
                file.path("data", fname),
                file.path("data", data_names))) if (file.exists(p)) return(p)
    stop("file not found: ", fname)
  }
  er_load <- function(data, sheet = NULL) {
    key <- trimws(tolower(data))
    if (key == "cora") {
      df <- tibble::as_tibble(data.table::fread(
        .find_extdata("cora.csv"), showProgress = FALSE))
      names(df) <- tolower(names(df))
      return(df)
    }
    if (key == "d10k") {
      path <- .find_extdata("d10k.csv", c("10Kfull.csv"))
      df <- tryCatch(
        data.table::fread(path, sep = "|",
                          select = c("Id", "Aggregate Value"),
                          showProgress = FALSE),
        error = function(e)
          data.table::fread(path, sep = "|", select = c(1L, 2L),
                            showProgress = FALSE))
      df <- tibble::as_tibble(df)
      names(df) <- tolower(gsub("\\s+", "_", trimws(names(df))))
      if ("aggregate_value" %in% names(df))
        names(df)[names(df) == "aggregate_value"] <- "text"
      return(df)
    }
    stop("standalone er_load: unsupported key '", data, "'")
  }
  er_load_gold <- function(dataset) {
    key <- trimws(tolower(dataset))
    if (key != "cora") stop("standalone er_load_gold: unsupported '", dataset, "'")
    truth <- tibble::as_tibble(data.table::fread(
      .find_extdata("cora_gold.csv"), showProgress = FALSE, data.table = FALSE))
    names(truth) <- tolower(names(truth))
    stopifnot(all(c("id1", "id2") %in% names(truth)))
    id1 <- as.character(truth$id1); id2 <- as.character(truth$id2)
    ok <- !is.na(id1) & !is.na(id2) & id1 != "" & id2 != "" & id1 != id2
    a <- pmin(id1[ok], id2[ok]); b <- pmax(id1[ok], id2[ok])
    keep_e <- !duplicated(paste(a, b, sep = "\r"))
    a <- a[keep_e]; b <- b[keep_e]
    ids <- unique(c(a, b))
    parent <- setNames(as.list(ids), ids)
    find_root <- function(x) {
      r <- x
      while (!identical(parent[[r]], r)) r <- parent[[r]]
      while (!identical(parent[[x]], r)) {
        nxt <- parent[[x]]; parent[[x]] <- r; x <- nxt
      }
      r
    }
    for (i in seq_along(a)) {
      ra <- find_root(a[i]); rb <- find_root(b[i])
      if (!identical(ra, rb)) parent[[rb]] <- ra
    }
    roots <- vapply(ids, find_root, character(1))
    tibble::tibble(id = ids, cluster_id = as.integer(factor(roots)))
  }
  er_block_stats <- function(pairs, n, truth = NULL, id_vec = NULL) {
    n_cand <- nrow(pairs)
    n_all  <- choose(n, 2L)
    rr     <- if (n_all > 0) 1 - n_cand / n_all else 0
    pc <- NA_real_
    if (!is.null(truth)) {
      if (is.vector(truth) && !is.null(names(truth))) {
        truth <- tibble::tibble(id = as.character(names(truth)),
                                cluster_id = as.integer(truth))
      }
      if (is.data.frame(truth)) {
        names(truth) <- tolower(names(truth))
        truth <- truth[, c("id", "cluster_id")]
      }
      if (!is.null(id_vec)) {
        id_to_idx <- setNames(seq_along(id_vec), as.character(id_vec))
        truth_idx <- truth[truth$id %in% names(id_to_idx), , drop = FALSE]
        truth_idx$idx <- id_to_idx[truth_idx$id]
      } else {
        truth_idx <- truth
        truth_idx$idx <- suppressWarnings(as.integer(truth_idx$id))
      }
      cl_groups <- split(truth_idx$idx, truth_idx$cluster_id)
      true_pairs <- do.call(rbind, lapply(cl_groups, function(idx) {
        if (length(idx) < 2L) return(NULL)
        gr <- expand.grid(a = idx, b = idx)
        gr <- gr[gr$a < gr$b, , drop = FALSE]
        data.frame(idx1 = gr$a, idx2 = gr$b)
      }))
      if (!is.null(true_pairs) && nrow(true_pairs)) {
        cand_key <- paste(pairs$idx1, pairs$idx2, sep = "_")
        true_key <- paste(true_pairs$idx1, true_pairs$idx2, sep = "_")
        n_covered <- sum(true_key %in% cand_key)
        pc <- n_covered / nrow(true_pairs)
      } else {
        pc <- NA_real_
      }
    }
    list(
      n_candidate_pairs = n_cand,
      n_all_pairs       = n_all,
      reduction_ratio   = round(rr, 4),
      pair_completeness = if (is.na(pc)) NA_real_ else round(pc, 4)
    )
  }
}
suppressPackageStartupMessages({
  library(Matrix)
})

message("== Related-methods comparison v1 | dataset: ", dataset, " ==")

## ---------- 0. load data + gold, align -------------------------------
find_data_file <- function(fname) {
  for (p in c(file.path("data", fname), file.path("inst", "extdata", fname)))
    if (file.exists(p)) return(p)
  stop("data file not found: ", fname, " (looked in data/ and inst/extdata/)")
}
gold_from_pairs <- function(pairs_path, all_ids) {
  pr <- utils::read.delim(pairs_path, sep = "|", colClasses = "character",
                          quote = "", comment.char = "")
  e1 <- trimws(pr[[1]]); e2 <- trimws(pr[[2]])
  parent <- new.env(hash = TRUE, parent = emptyenv())
  find_root <- function(x) {
    r <- x
    while (!identical(parent[[r]], r)) r <- parent[[r]]
    while (!identical(parent[[x]], r)) {
      nxt <- parent[[x]]; parent[[x]] <- r; x <- nxt
    }
    r
  }
  for (id in all_ids) parent[[id]] <- id
  for (i in seq_along(e1)) {
    a <- e1[i]; b <- e2[i]
    if (is.null(parent[[a]])) parent[[a]] <- a
    if (is.null(parent[[b]])) parent[[b]] <- b
    ra <- find_root(a); rb <- find_root(b)
    if (!identical(ra, rb)) parent[[rb]] <- ra
  }
  roots <- vapply(all_ids, find_root, character(1))
  tibble::tibble(id = all_ids, cluster_id = as.integer(factor(roots)))
}
load_records <- function(dataset) {
  if (identical(dataset, "musicbrainz20k")) {
    df <- tibble::as_tibble(data.table::fread(
      find_data_file("musicbrainz20k.csv"), showProgress = FALSE))
    names(df) <- tolower(names(df))
    return(df)
  }
  er_load(dataset)
}
load_gold <- function(dataset, all_ids) {
  if (identical(dataset, "d10k"))
    return(gold_from_pairs(find_data_file("10Kduplicates.csv"), all_ids))
  if (identical(dataset, "musicbrainz20k")) {
    g <- tibble::as_tibble(data.table::fread(
      find_data_file("musicbrainz20k_gold.csv"), showProgress = FALSE))
    names(g) <- tolower(names(g))
    return(g)
  }
  er_load_gold(dataset)
}
df   <- tryCatch(load_records(dataset), error = function(e)
  stop("loading records for '", dataset, "' failed: ", conditionMessage(e),
       " -- check the dataset files exist (see header)."))
n    <- nrow(df)
row_ids <- if ("id" %in% names(df)) as.character(df$id) else as.character(seq_len(n))
gold <- tryCatch(load_gold(dataset, row_ids), error = function(e)
  stop("loading gold for '", dataset, "' failed: ", conditionMessage(e)))
gvec <- setNames(as.integer(gold$cluster_id), as.character(gold$id))
crow <- unname(gvec[row_ids])
keep <- !is.na(crow)
if (!all(keep)) message("dropping ", sum(!keep), " unlabeled rows")
df <- df[keep, , drop = FALSE]; crow <- crow[keep]; row_ids <- row_ids[keep]
n <- nrow(df)
message("n = ", n, ", entities = ", length(unique(crow)))

## ---------- 1. text + shingles ----------------------------------------
txt_cols <- names(df)[vapply(df, function(c) is.character(c) || is.factor(c), logical(1L))]
txt_cols <- setdiff(txt_cols, c("id", "truth"))
texts <- vapply(seq_len(n), function(i) {
  paste(vapply(txt_cols, function(cn) {
    v <- df[[cn]][i]; if (is.na(v)) "" else as.character(v)
  }, character(1L)), collapse = " ")
}, character(1L))
texts <- tolower(texts)

shingles_of <- function(s) {
  s <- gsub("[^a-z0-9 ]", " ", s)
  s <- gsub("\\s+", " ", s)
  chars <- strsplit(s, "")[[1L]]
  L <- length(chars)
  if (L < 3L) return(if (L == 0L) character(0L) else s)
  unique(vapply(seq_len(L - 2L), function(i) paste(chars[i:(i + 2L)], collapse = ""),
                character(1L)))
}
sh_list <- lapply(texts, shingles_of)

## sparse 3-gram DTM (for cosine baseline)
all_sh <- unique(unlist(sh_list))
sh_id <- setNames(seq_along(all_sh), all_sh)
ii <- rep(seq_len(n), times = vapply(sh_list, length, integer(1L)))
jj <- unlist(lapply(sh_list, function(s) unname(sh_id[s])))
X <- Matrix::sparseMatrix(i = ii, j = jj, x = 1, dims = c(n, length(all_sh)))
rs <- sqrt(Matrix::rowSums(X ^ 2)); rs[rs == 0] <- 1
Xn <- Matrix::Diagonal(x = 1 / rs) %*% X   # L2-normalized rows

bow_topk <- function(Xn, k, chunk = NULL) {
  n <- nrow(Xn)
  if (is.null(chunk)) chunk <- if (n > 5000L) 500L else n
  i1 <- integer(0); i2 <- integer(0); ww <- numeric(0)
  starts <- seq(1L, n, by = chunk)
  for (s in starts) {
    e <- min(s + chunk - 1L, n)
    C <- as.matrix(Xn[s:e, , drop = FALSE] %*% Matrix::t(Xn))
    for (r in seq_len(nrow(C))) {
      row <- C[r, ]; row[s + r - 1L] <- -Inf   # exclude self
      tk <- order(row, decreasing = TRUE)[seq_len(min(k, n - 1L))]
      gi <- s + r - 1L
      i1 <- c(i1, rep(gi, length(tk))); i2 <- c(i2, tk); ww <- c(ww, row[tk])
    }
  }
  tibble::tibble(idx1 = i1, idx2 = i2, w = ww)
}
undirect <- function(p) {
  a <- pmin(p$idx1, p$idx2); b <- pmax(p$idx1, p$idx2)
  p$idx1 <- a; p$idx2 <- b
  p[!duplicated(paste(a, b, sep = "_")), , drop = FALSE]
}

## ---------- 2. entity-disjoint split -----------------------------------
set.seed(SEED)
ents <- unique(crow)
train_ents <- sample(ents, size = floor(0.7 * length(ents)))
tr_idx <- which(crow %in% train_ents)
te_idx <- which(!crow %in% train_ents)
message("train rows: ", length(tr_idx), " | test rows: ", length(te_idx))

gold_tbl <- tibble::tibble(id = row_ids, cluster_id = crow)
is_match_pair <- function(i1, i2, idxs) {
  crow[idxs[i1]] == crow[idxs[i2]]
}

## ---------- 3. TUNE baseline k on train --------------------------------
sub_topk <- function(idxs, k) {
  Xs <- Xn[idxs, , drop = FALSE]
  undirect(bow_topk(Xs, k))
}
pc_of <- function(pairs, idxs) {
  st <- er_block_stats(pairs, n = length(idxs), truth = gold_tbl[idxs, , drop = FALSE],
                       id_vec = row_ids[idxs])
  st$pair_completeness
}

message("-- tuning baseline k on train --")
k_grid <- c(10L, 20L, 50L)
pc_k <- vapply(k_grid, function(k) pc_of(sub_topk(tr_idx, k), tr_idx), numeric(1L))
k_star <- k_grid[which.max(pc_k)]
message("k* = ", k_star, " (PC: ", paste(round(pc_k, 4), collapse = ", "), ")")
B_train <- nrow(sub_topk(tr_idx, k_star))

## ---------- 4. calibrations, fit on TRAIN only --------------------------
message("-- fitting calibrations on train --")
Xs_tr <- Xn[tr_idx, , drop = FALSE]
pool_tr <- bow_topk(Xs_tr, M_POOL)
y_tr <- as.integer(is_match_pair(pool_tr$idx1, pool_tr$idx2, tr_idx))
message("train pool pairs: ", nrow(pool_tr), " | positives: ", sum(y_tr),
        " (", round(100 * mean(y_tr), 2), "%)")

# (A) labeled: Platt logistic P(match | cosine)
sw_tr <- pool_tr$w
fitA <- suppressWarnings(glm(y_tr ~ sw_tr, family = binomial()))
margA <- function(w) as.numeric(predict(fitA,
                 newdata = data.frame(sw_tr = w), type = "response"))
message("Platt coef: (int) ", round(coef(fitA)[1L], 3),
        " slope ", round(coef(fitA)[2L], 3))

# (B) unlabeled: 2-component Gaussian mixture on train similarities (manual EM)
em_mix2 <- function(x, iters = 200L) {
  km <- stats::kmeans(cbind(x), centers = 2L, nstart = 10L)
  mu <- as.numeric(km$centers); ord <- order(mu); mu <- mu[ord]
  cl <- km$cluster
  s1 <- stats::sd(x[cl == ord[1L]]); s2 <- stats::sd(x[cl == ord[2L]])
  sd <- c(ifelse(is.na(s1) || s1 < 1e-6, 1e-6, s1),
          ifelse(is.na(s2) || s2 < 1e-6, 1e-6, s2))
  w <- c(mean(cl == ord[1L]), mean(cl == ord[2L]))
  for (it in seq_len(iters)) {
    d1 <- w[1L] * stats::dnorm(x, mu[1L], sd[1L])
    d2 <- w[2L] * stats::dnorm(x, mu[2L], sd[2L])
    g <- d2 / (d1 + d2 + 1e-300)                       # posterior of HIGH comp
    n1 <- sum(1 - g); n2 <- sum(g)
    if (n1 < 1e-9 || n2 < 1e-9) break
    w <- c(n1, n2) / (n1 + n2)
    mu <- c(sum((1 - g) * x) / n1, sum(g * x) / n2)
    sd <- c(sqrt(sum((1 - g) * (x - mu[1L])^2) / n1),
            sqrt(sum(g * (x - mu[2L])^2) / n2))
    sd[sd < 1e-6] <- 1e-6
  }
  list(w = w, mu = mu, sd = sd)
}
mixB <- em_mix2(pool_tr$w)
message("mixture: w=", paste(round(mixB$w, 3), collapse = "/"),
        " mu=", paste(round(mixB$mu, 3), collapse = "/"),
        " sd=", paste(round(mixB$sd, 3), collapse = "/"))
margB <- function(w) {
  d1 <- mixB$w[1L] * stats::dnorm(w, mixB$mu[1L], mixB$sd[1L])
  d2 <- mixB$w[2L] * stats::dnorm(w, mixB$mu[2L], mixB$sd[2L])
  d2 / (d1 + d2 + 1e-300)
}

## ---------- 5. water-filling allocator ----------------------------------
waterfill <- function(idxs, B, marg_fn, M = M_POOL) {
  Xs <- Xn[idxs, , drop = FALSE]
  ns <- length(idxs)
  pool <- bow_topk(Xs, M)
  pool$marg <- marg_fn(pool$w)
  pool <- pool[order(pool$idx1, -pool$w), , drop = FALSE]  # rank within record
  pool$rank <- unlist(lapply(split(seq_len(nrow(pool)), pool$idx1),
                             function(rr) seq_along(rr)))
  floor_p <- pool[pool$rank == 1L, ]
  rest <- pool[pool$rank > 1L, ]
  rest <- rest[order(-rest$marg), , drop = FALSE]
  need <- B - nrow(floor_p)
  take <- if (need > 0L) head(rest, need) else rest[0L, , drop = FALSE]
  sel <- undirect(rbind(floor_p[, c("idx1", "idx2", "w", "marg")],
                        take[, c("idx1", "idx2", "w", "marg")]))
  guard <- 0L
  while (nrow(sel) < B && guard < 5L) {
    guard <- guard + 1L
    used <- paste(pmin(sel$idx1, sel$idx2), pmax(sel$idx1, sel$idx2), sep = "_")
    cand <- rest[!(paste(pmin(rest$idx1, rest$idx2), pmax(rest$idx1, rest$idx2),
                         sep = "_") %in% used), , drop = FALSE]
    if (!nrow(cand)) break
    add <- head(cand, B - nrow(sel))
    sel <- undirect(rbind(sel, add[, c("idx1", "idx2", "w", "marg")]))
  }
  sel
}

## ---------- 6. TEST-split evaluation battery ----------------------------
message("-- final on TEST split --")
base_pairs <- sub_topk(te_idx, k_star)
B_test <- nrow(base_pairs)
message("B_test (baseline pairs) = ", B_test, " | B_train = ", B_train)

eval_pc <- function(pairs) {
  st <- er_block_stats(pairs[, c("idx1", "idx2")], n = length(te_idx),
                       truth = gold_tbl[te_idx, , drop = FALSE],
                       id_vec = row_ids[te_idx])
  st$pair_completeness
}
RESULTS <- new.env(hash = TRUE)
emit <- function(tag, pairs_native, pc_native, pairs_al = NULL, pc_al = NULL) {
  RESULTS[[tag]] <- list(native_n = pairs_native, native_pc = pc_native,
                         aligned_pc = pc_al)
  if (is.null(pc_al)) {
    message(sprintf("RESULT %-26s native pairs=%-8d PC=%.4f", tag, pairs_native, pc_native))
  } else {
    message(sprintf("RESULT %-26s native pairs=%-8d PC=%.4f | aligned@B PC=%.4f",
                    tag, pairs_native, pc_native, pc_al))
  }
}

# cosine between arbitrary GLOBAL row pairs (vectorised sparse row products)
cos_pairs <- function(g1, g2) {
  as.numeric(Matrix::rowSums(Xn[g1, , drop = FALSE] * Xn[g2, , drop = FALSE]))
}

# test pool (directed + undirected), local test indices
pool_te_dir <- bow_topk(Xn[te_idx, , drop = FALSE], M_POOL)
pool_u <- undirect(pool_te_dir)

# align a scored selection to exactly B pairs: trim by score (tie: cosine),
# top up from the undirected pool by cosine.
align_B <- function(sel, score, B) {
  sel$score <- score
  # v1.3: reuse a caller-supplied cosine column when present (meta-blocking
  # edges carry exact cosines from the blocked cross-product; recomputing
  # them via cos_pairs on 1.2M edges OOM-killed the MusicBrainz run).
  if (!("w" %in% names(sel)))
    sel$w <- cos_pairs(te_idx[sel$idx1], te_idx[sel$idx2])
  sel <- sel[order(-sel$score, -sel$w), , drop = FALSE]
  if (nrow(sel) > B) sel <- sel[seq_len(B), , drop = FALSE]
  if (nrow(sel) < B) {
    used <- paste(sel$idx1, sel$idx2, sep = "_")
    cand <- pool_u[!(paste(pool_u$idx1, pool_u$idx2, sep = "_") %in% used), , drop = FALSE]
    cand <- cand[order(-cand$w), , drop = FALSE]
    add <- head(cand, B - nrow(sel))
    sel <- rbind(sel[, c("idx1", "idx2")], add[, c("idx1", "idx2")])
  }
  tibble::tibble(idx1 = sel$idx1, idx2 = sel$idx2)
}

## M0 baseline + M6 marg-alloc (same-split anchors)
pc_base <- eval_pc(base_pairs)
emit("M0 fixed-k baseline", nrow(base_pairs), pc_base)
wfA <- waterfill(te_idx, B_test, margA)
wfB <- waterfill(te_idx, B_test, margB)
emit("M6 marg-alloc A (Platt)", nrow(wfA), eval_pc(wfA))
emit("M6 marg-alloc B (mixture)", nrow(wfB), eval_pc(wfB))

## M1 GLOBAL-TOPB: rank the whole pool by cosine, take top B
gt <- pool_u[order(-pool_u$w), , drop = FALSE][seq_len(B_test), , drop = FALSE]
emit("M1 global top-B", nrow(gt), eval_pc(gt))

## M2 META-BLOCKING -------------------------------------------------------
message("-- meta-blocking --")
tok_of <- function(s) {
  s <- gsub("[^a-z0-9 ]", " ", s)
  t <- strsplit(s, "\\s+")[[1L]]
  unique(t[nchar(t) >= 2L])
}
meta_edges <- function(idxs) {
  tl <- lapply(texts[idxs], tok_of)
  vocab <- unique(unlist(tl))
  vid <- setNames(seq_along(vocab), vocab)
  ns <- length(idxs)
  ii <- rep(seq_len(ns), times = vapply(tl, length, integer(1L)))
  jj <- unlist(lapply(tl, function(t) unname(vid[t])))
  Tm <- Matrix::sparseMatrix(i = ii, j = jj, x = 1, dims = c(ns, length(vocab)))
  dfv <- Matrix::colSums(Tm)
  cap <- max(50, floor(0.05 * ns))
  keepv <- which(dfv >= 2 & dfv <= cap)
  message("meta-blocking: vocab=", length(vocab), " kept after purge=", length(keepv),
          " (df cap ", cap, ")")
  Tm <- Tm[, keepv, drop = FALSE]
  rs_i <- Matrix::rowSums(Tm)
  # Blocked cross-products (v1.2): the full Tm %*% t(Tm) on ~6k records
  # OOM-killed the first MusicBrainz run on this 7 GB VM. Process 400-row
  # blocks; extract upper-triangle triplets per block; cosine for each
  # edge comes from the same block's Xn cross-product.
  Xs <- Xn[idxs, , drop = FALSE]
  BLK <- 400L
  out1 <- integer(0); out2 <- integer(0); outc <- numeric(0); outw <- numeric(0)
  for (s in seq(1L, ns, by = BLK)) {
    e <- min(s + BLK - 1L, ns)
    Cb <- Matrix::summary(Tm[s:e, , drop = FALSE] %*% Matrix::t(Tm))
    gi <- Cb$i + (s - 1L)
    up <- gi < Cb$j
    if (!any(up)) next
    Cw <- Matrix::summary(Xs[s:e, , drop = FALSE] %*% Matrix::t(Xs))
    wkey <- paste(Cw$i + (s - 1L), Cw$j, sep = "_")
    wvec <- setNames(Cw$x, wkey)
    ek <- paste(gi[up], Cb$j[up], sep = "_")
    wv <- unname(wvec[ek]); wv[is.na(wv)] <- 0
    out1 <- c(out1, gi[up]); out2 <- c(out2, Cb$j[up])
    outc <- c(outc, Cb$x[up]); outw <- c(outw, wv)
  }
  e <- data.frame(idx1 = out1, idx2 = out2, cbs = outc, w = outw)
  e$js <- e$cbs / (rs_i[e$idx1] + rs_i[e$idx2] - e$cbs)
  e
}
invisible(gc())
me <- meta_edges(te_idx)
message("meta-blocking edges: ", nrow(me))
# Budget alignment (v1.1 fix): the block graph can hold FEWER edges than B
# (sparse token overlap); rank by the scheme's weight and top up from the
# pool by cosine -- the same alignment discipline as every other method.
me_cbs_al <- align_B(me, me$cbs, B_test)
me_js_al  <- align_B(me, me$js, B_test)
emit("M2 meta-blocking CBS aligned@B", nrow(me_cbs_al), eval_pc(me_cbs_al))
emit("M2 meta-blocking JS aligned@B", nrow(me_js_al), eval_pc(me_js_al))
# native WNP (CBS weights; keep edge if >= BOTH endpoints' mean incident weight)
inc_sum <- tapply(c(me$cbs, me$cbs), c(me$idx1, me$idx2), sum)
inc_cnt <- tapply(rep(1, 2 * nrow(me)), c(me$idx1, me$idx2), sum)
mean_i <- (inc_sum / inc_cnt)[as.character(me$idx1)]
mean_j <- (inc_sum / inc_cnt)[as.character(me$idx2)]
wnp <- me[me$cbs >= mean_i & me$cbs >= mean_j, , drop = FALSE]
emit("M2 meta-blocking WNP native", nrow(wnp),
     eval_pc(tibble::tibble(idx1 = wnp$idx1, idx2 = wnp$idx2)))

## M3 ADAPTIVE-K -----------------------------------------------------------
message("-- adaptive-k --")
adapt_sel <- function(pool_dir, alpha) {
  wmax <- tapply(pool_dir$w, pool_dir$idx1, max)
  keep <- pool_dir$w >= alpha * unname(wmax[as.character(pool_dir$idx1)])
  undirect(pool_dir[keep, , drop = FALSE])
}
alpha_grid <- seq(0.30, 0.95, by = 0.05)
alpha_counts <- vapply(alpha_grid, function(a) nrow(adapt_sel(pool_tr, a)), integer(1L))
alpha_star <- alpha_grid[which.min(abs(alpha_counts - B_train))]
message("adaptive-k: alpha* = ", alpha_star, " (train pairs ",
        alpha_counts[which.min(abs(alpha_counts - B_train))], " vs B_train ", B_train, ")")
ad_native <- adapt_sel(pool_te_dir, alpha_star)
ad_al <- align_B(ad_native[, c("idx1", "idx2")], ad_native$w, B_test)
emit("M3 adaptive-k", nrow(ad_native), eval_pc(ad_native), NULL, eval_pc(ad_al))

## M4 SORTED NEIGHBORHOOD ---------------------------------------------------
message("-- sorted neighborhood --")
norm_text <- gsub("\\s+", " ", gsub("[^a-z0-9 ]", " ", texts))
norm_text <- trimws(norm_text)
sn_pairs <- function(idxs, win) {
  ord <- order(norm_text[idxs])
  ns <- length(idxs)
  a <- integer(0); b <- integer(0)
  for (d in seq_len(win - 1L)) {
    a <- c(a, ord[seq_len(ns - d)]); b <- c(b, ord[(d + 1L):ns])
  }
  undirect(tibble::tibble(idx1 = a, idx2 = b, w = 0))
}
win_grid <- c(3L, 5L, 10L, 20L, 40L, 80L, 120L)
win_counts <- vapply(win_grid, function(w) nrow(sn_pairs(tr_idx, w)), integer(1L))
win_star <- win_grid[which.min(abs(win_counts - B_train))]
message("sorted-neighborhood: window* = ", win_star, " (train pairs ",
        win_counts[which.min(abs(win_counts - B_train))], " vs B_train ", B_train, ")")
sn_native <- sn_pairs(te_idx, win_star)
sn_w <- cos_pairs(te_idx[sn_native$idx1], te_idx[sn_native$idx2])
sn_al <- align_B(sn_native[, c("idx1", "idx2")], sn_w, B_test)
emit("M4 sorted neighborhood", nrow(sn_native), eval_pc(sn_native), NULL, eval_pc(sn_al))

## M5 CANOPY -----------------------------------------------------------------
message("-- canopy --")
canopy_pairs <- function(idxs, T2, T1, count_only = FALSE) {
  Xs <- Xn[idxs, , drop = FALSE]
  ns <- length(idxs)
  removed <- logical(ns)
  seen <- new.env(hash = TRUE, parent = emptyenv())
  approx <- 0
  for (j in seq_len(ns)) {
    if (removed[j]) next
    sims <- as.numeric(Xs %*% Matrix::t(Xs[j, , drop = FALSE]))
    mem <- which(sims >= T2)
    if (length(mem) >= 2L) {
      if (count_only) {
        approx <- approx + choose(length(mem), 2L)   # upper bound (overlaps ignored)
      } else {
        if (length(mem) > 5000L)
          stop("canopy_pairs: single canopy of ", length(mem),
               " members at T2=", T2, " -- too large to enumerate")
        pr <- utils::combn(mem, 2L)
        for (cc in seq_len(ncol(pr))) {
          key <- paste(pr[1L, cc], pr[2L, cc], sep = "_")
          if (is.null(seen[[key]])) seen[[key]] <- TRUE
        }
      }
    }
    removed[sims >= T1] <- TRUE
  }
  if (count_only) return(as.integer(min(approx, .Machine$integer.max)))
  keys <- ls(seen)
  ab <- do.call(rbind, strsplit(keys, "_", fixed = TRUE))
  tibble::tibble(idx1 = as.integer(ab[, 1]), idx2 = as.integer(ab[, 2]), w = 0)
}
t2_grid <- stats::quantile(pool_tr$w, probs = seq(0.50, 0.98, by = 0.04), names = FALSE)
t2_counts <- vapply(t2_grid, function(t2) canopy_pairs(tr_idx, t2, min(0.99, t2 + 0.05),
                                                       count_only = TRUE),
                    integer(1L))
t2_star <- t2_grid[which.min(abs(t2_counts - B_train))]
message("canopy: T2* = ", round(t2_star, 4), " (train pairs ",
        t2_counts[which.min(abs(t2_counts - B_train))], " vs B_train ", B_train, ")")
ca_native <- canopy_pairs(te_idx, t2_star, min(0.99, t2_star + 0.05))
ca_w <- cos_pairs(te_idx[ca_native$idx1], te_idx[ca_native$idx2])
ca_al <- align_B(ca_native[, c("idx1", "idx2")], ca_w, B_test)
emit("M5 canopy", nrow(ca_native), eval_pc(ca_native), NULL, eval_pc(ca_al))

## ---------- 7. summary -------------------------------------------------------
message("== SUMMARY | dataset: ", dataset, " | B_test = ", B_test,
        " | baseline PC = ", round(pc_base, 4), " ==")
for (tg in ls(RESULTS)) {
  r <- RESULTS[[tg]]
  message(sprintf("SUMMARY %-26s native=%-8d PC=%.4f alignedPC=%s",
                  tg, r$native_n, r$native_pc,
                  if (is.null(r$aligned_pc)) "-" else sprintf("%.4f", r$aligned_pc)))
}
