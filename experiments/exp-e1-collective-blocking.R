# E1 prototype: collective blocking vs per-record top-k (budget-aligned)
#
# Tests creative idea 2: MinHash kNN graph -> Leiden communities -> community-level
# candidates (+ cross-community insurance) vs per-record top-k baseline at the
# SAME candidate-pair budget. Reports blocking PC + end-to-end ARI.
#
# Usage (from the erbot package root, on a machine with R + package deps):
#   Rscript experiments/exp-e1-collective-blocking.R cora        # main (1879 rows)
#   Rscript experiments/exp-e1-collective-blocking.R restaurant  # smoke test (35 rows)
#
# Honesty discipline baked in:
#   * entity-disjoint 70/30 split: hyperparams (k grid, leiden resolution) tuned on
#     TRAIN entities only; final numbers reported on TEST entities, once.
#   * budget alignment: both arms trimmed/extended to the same pair count (+-5% here;
#     tighten to +-2% for the paper run).
#   * kill line printed at the end: treatment PC <= baseline PC  =>  idea dead.

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1L) args[1L] else "cora"
SEED <- 20261012L

if (requireNamespace("pkgload", quietly = TRUE)) {
  suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
} else {
  for (f in list.files("R", pattern = "\\.[rR]$", full.names = TRUE)) source(f)
}
suppressPackageStartupMessages({
  library(Matrix)
})

message("== E1 prototype | dataset: ", dataset, " ==")

## ---------- 0. load data + gold, align -------------------------------
df   <- er_load(dataset)
gold <- er_load_gold(dataset)          # tibble(id, cluster_id)
n    <- nrow(df)
row_ids <- if ("id" %in% names(df)) as.character(df$id) else as.character(seq_len(n))
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

pair_cosine <- function(Xn, pairs) {
  Matrix::rowSums(Xn[pairs$idx1, , drop = FALSE] * Xn[pairs$idx2, , drop = FALSE])
}

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

## ---------- 3. TUNE on train -------------------------------------------
sub_topk <- function(idxs, k) {
  # per-record top-k restricted to a row subset (indices are LOCAL to subset)
  Xs <- Xn[idxs, , drop = FALSE]
  p <- undirect(bow_topk(Xs, k))
  p
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

## ---------- 4. treatment: MinHash -> LSH -> Leiden ----------------------
minhash_block <- function(idxs, P = 128L, bands = 16L, res_grid = c(0.5, 1.0, 1.5),
                          cap = 200L, ins_k = 5L, budget = NULL) {
  Xs <- Xn[idxs, , drop = FALSE]
  ns <- length(idxs)
  sh_sub <- sh_list[idxs]
  # hash every unique shingle once
  ush <- unique(unlist(sh_sub))
  hb <- t(vapply(ush, function(s) {
    b <- as.integer(charToRaw(s)); w <- seq_along(b)
    c(sum(b * w) %% 2147483647, sum(rev(b) * w) %% 2147483647)
  }, numeric(2L)))
  rownames(hb) <- ush
  set.seed(SEED)
  A <- sample.int(1e6, P); Bp <- sample.int(1e6, P); Cc <- sample.int(1e6, P)
  M <- 2147483647
  PV <- (outer(A, hb[, 1]) + outer(Bp, hb[, 2]) + Cc) %% M   # P x U
  shmat <- lapply(sh_sub, function(s) match(s, ush))
  sig <- vapply(shmat, function(cols) {
    if (!length(cols)) return(rep(M, P))
    apply(PV[, cols, drop = FALSE], 1L, min)
  }, numeric(P))                                                    # P x ns
  sig <- t(sig)                                                     # ns x P
  # LSH banding
  rows_per_band <- P %/% bands
  cand <- list()
  for (b in seq_len(bands)) {
    cols <- ((b - 1L) * rows_per_band + 1L):(b * rows_per_band)
    key <- apply(sig[, cols, drop = FALSE], 1L, paste, collapse = "|")
    bk <- split(seq_len(ns), key)
    for (members in bk) {
      if (length(members) < 2L) next
      if (length(members) > 300L) members <- sample(members, 300L)  # anti-blowup cap
      gr <- utils::combn(members, 2L)
      cand[[length(cand) + 1L]] <- gr
    }
  }
  if (!length(cand)) {
    empty <- tibble::tibble(idx1 = integer(), idx2 = integer(), w = numeric())
    return(list(pc = 0, res = res_grid[1L], pairs = empty))
  }
  CM <- do.call(cbind, cand)
  i1 <- pmin(CM[1L, ], CM[2L, ]); i2 <- pmax(CM[1L, ], CM[2L, ])
  key <- paste(i1, i2, sep = "_"); dup <- duplicated(key)
  i1 <- i1[!dup]; i2 <- i2[!dup]
  # estimated Jaccard = fraction of agreeing minhashes
  w <- vapply(seq_along(i1), function(t) mean(sig[i1[t], ] == sig[i2[t], ]), numeric(1L))
  S <- Matrix::sparseMatrix(i = c(i1, i2), j = c(i2, i1), x = c(w, w), dims = c(ns, ns))

  best <- NULL
  for (res in res_grid) {
    memb <- tryCatch(er_cluster(S, method = "leiden", resolution = res),
                     error = function(e) er_cluster(S, method = "louvain", resolution = res))
    # within-community pairs
    comps <- split(seq_len(ns), memb)
    pi1 <- integer(0); pi2 <- integer(0); pw <- numeric(0)
    for (members in comps) {
      m <- length(members)
      if (m < 2L) next
      if (m <= cap) {
        gr <- utils::combn(members, 2L)
        a <- gr[1L, ]; b <- gr[2L, ]
        pi1 <- c(pi1, a); pi2 <- c(pi2, b)
        pw <- c(pw, vapply(seq_along(a), function(t) mean(sig[a[t], ] == sig[b[t], ]), numeric(1L)))
      } else {
        # big community: per-record top-cap by minhash weight
        for (r in members) {
          nbrs <- setdiff(members, r)
          ww <- vapply(nbrs, function(q) mean(sig[r, ] == sig[q, ]), numeric(1L))
          tk <- nbrs[order(ww, decreasing = TRUE)[seq_len(min(cap, length(nbrs)))]]
          pi1 <- c(pi1, rep(r, length(tk))); pi2 <- c(pi2, tk)
          pw <- c(pw, ww[order(ww, decreasing = TRUE)[seq_len(min(cap, length(nbrs)))]])
        }
      }
    }
    # insurance: cross-community top-ins_k by bow cosine
    tk200 <- bow_topk(Xs, 200L)
    ins <- tk200[!(paste(pmin(tk200$idx1, tk200$idx2), pmax(tk200$idx1, tk200$idx2), sep = "_") %in%
                     paste(pmin(pi1, pi2), pmax(pi1, pi2), sep = "_")), , drop = FALSE]
    ins <- ins[memb[ins$idx1] != memb[ins$idx2], , drop = FALSE]
    ins <- ins[order(ins$idx1, -ins$w), , drop = FALSE]
    # per-record top-ins_k of insurance
    ins_keep <- unlist(lapply(split(seq_len(nrow(ins)), ins$idx1),
                              function(rr) head(rr, ins_k)))
    ins <- ins[ins_keep, , drop = FALSE]
    pairs <- tibble::tibble(idx1 = c(pi1, ins$idx1), idx2 = c(pi2, ins$idx2),
                            w = c(pw, ins$w))
    pairs <- undirect(pairs)
    # budget align
    if (!is.null(budget)) {
      if (nrow(pairs) > budget * 1.05) {
        pairs <- pairs[order(pairs$w, decreasing = TRUE)[seq_len(budget)], , drop = FALSE]
      }
    }
    pc <- pc_of(pairs[, c("idx1", "idx2")], idxs)
    if (is.null(best) || pc > best$pc) best <- list(pc = pc, res = res, pairs = pairs)
  }
  if (is.null(best)) best <- list(pc = 0, res = res_grid[1L],
                                  pairs = tibble::tibble(idx1 = integer(), idx2 = integer(),
                                                         w = numeric()))
  best
}

message("-- tuning treatment (leiden resolution) on train --")
B_train_pairs <- nrow(sub_topk(tr_idx, k_star))
tune <- minhash_block(tr_idx, budget = B_train_pairs)
message("res* = ", tune$res, " | train PC(treatment) = ", round(tune$pc, 4))

## ---------- 5. FINAL: test split, once ----------------------------------
message("-- final on TEST split --")
base_pairs <- sub_topk(te_idx, k_star)
B_test <- nrow(base_pairs)
treat <- minhash_block(te_idx, res_grid = tune$res, budget = B_test)

st_b <- er_block_stats(base_pairs, n = length(te_idx), truth = gold_tbl[te_idx, , drop = FALSE],
                       id_vec = row_ids[te_idx])
st_t <- er_block_stats(treat$pairs[, c("idx1", "idx2")], n = length(te_idx),
                       truth = gold_tbl[te_idx, , drop = FALSE], id_vec = row_ids[te_idx])
message(sprintf("BASELINE  k=%d pairs=%d PC=%.4f RR=%.4f",
                k_star, st_b$n_candidate_pairs, st_b$pair_completeness, st_b$reduction_ratio))
message(sprintf("TREATMENT res=%.1f pairs=%d PC=%.4f RR=%.4f",
                tune$res, st_t$n_candidate_pairs, st_t$pair_completeness, st_t$reduction_ratio))

## end-to-end: leiden on candidate similarities -> ARI
run_e2e <- function(pairs, idxs, tag) {
  Xs <- Xn[idxs, , drop = FALSE]
  w <- pair_cosine(Xs, pairs)
  S <- Matrix::sparseMatrix(i = c(pairs$idx1, pairs$idx2), j = c(pairs$idx2, pairs$idx1),
                            x = c(w, w), dims = c(length(idxs), length(idxs)))
  memb <- er_cluster(S, method = "leiden", resolution = 1)
  ev <- er_evaluate(memb, truth = gold_tbl[idxs, , drop = FALSE], id_vec = row_ids[idxs])
  message("-- end-to-end ", tag, " --"); print(ev)
  invisible(ev)
}
run_e2e(base_pairs, te_idx, "baseline")
run_e2e(treat$pairs[, c("idx1", "idx2")], te_idx, "treatment")

## ---------- 6. kill line --------------------------------------------------
dpc <- st_t$pair_completeness - st_b$pair_completeness
message(sprintf("== KILL LINE: dPC (treatment - baseline) = %+.4f ==", dpc))
if (is.na(dpc) || dpc <= 0) {
  message("VERDICT: idea dead on this dataset (treatment PC <= baseline PC).")
} else {
  message("VERDICT: signal alive — proceed to full protocol (tighter budget +-2%, ",
          "permutation test, D10K + MusicBrainz).")
}
