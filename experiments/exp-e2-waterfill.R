# E2 prototype: marginal-allocation blocking ("water-filling") vs per-record fixed top-k
#
# Tests creative idea 1: treat the pair budget B as capital and allocate it
# across records by MARGINAL expected recall (marginal allocation), instead of
# giving every record the same fixed k. Two calibration variants:
#   (A) labeled   -- P(match | similarity) via Platt (logistic) fit on TRAIN entities
#   (B) unlabeled -- P(match | similarity) via 2-component Gaussian mixture on the
#                    TRAIN similarity distribution (no gold needed)
# Key trick: marginal values decrease with rank within each record, so the
# global top-B by marginal value IS the marginal-allocation (greedy = optimal)
# solution for the nested knapsack max E[recall] s.t. sum k_i = B.
#
# Usage (from the erbot package root, on a machine with R + package deps):
#   Rscript experiments/exp-e2-waterfill.R cora        # main (1879 rows)
#   Rscript experiments/exp-e2-waterfill.R restaurant  # smoke test (35 rows)
#
# Honesty discipline baked in:
#   * entity-disjoint 70/30 split: baseline k AND both calibrations fit on
#     TRAIN entities only; final numbers reported on TEST entities, once.
#   * budget alignment: both treatment arms trimmed to exactly B_test pairs
#     (the baseline's pair count).
#   * per-record floor: every record gets >= 1 pair (no starvation).
#   * TWO kill lines printed at the end, one per calibration variant.

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1L) args[1L] else "cora"
SEED <- 20261016L
M_POOL <- 200L   # per-record candidate pool water-filling selects from

if (requireNamespace("pkgload", quietly = TRUE)) {
  suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
} else {
  for (f in list.files("R", pattern = "\\.[rR]$", full.names = TRUE)) source(f)
}
suppressPackageStartupMessages({
  library(Matrix)
})

message("== E2 prototype | dataset: ", dataset, " ==")

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
# sanity: mixture must separate (else variant B is uninformative -> will die honestly)
sepB <- mixB$mu[2L] - mixB$mu[1L]
message("mixture separation (mu_high - mu_low) = ", round(sepB, 4))

## ---------- 5. water-filling allocator ----------------------------------
# global top-B by marginal value == the marginal-allocation ("water-filling")
# optimum (marginals decrease with rank inside each record). Per-record floor
# of 1 pair first.
waterfill <- function(idxs, B, marg_fn, M = M_POOL) {
  Xs <- Xn[idxs, , drop = FALSE]
  ns <- length(idxs)
  pool <- bow_topk(Xs, M)
  pool$marg <- marg_fn(pool$w)
  pool <- pool[order(pool$idx1, -pool$w), , drop = FALSE]  # rank within record
  pool$rank <- unlist(lapply(split(seq_len(nrow(pool)), pool$idx1),
                             function(rr) seq_along(rr)))
  # floor: rank-1 for every record
  floor_p <- pool[pool$rank == 1L, ]
  rest <- pool[pool$rank > 1L, ]
  rest <- rest[order(-rest$marg), , drop = FALSE]
  need <- B - nrow(floor_p)
  take <- if (need > 0L) head(rest, need) else rest[0L, , drop = FALSE]
  sel <- undirect(rbind(floor_p[, c("idx1", "idx2", "w", "marg")],
                        take[, c("idx1", "idx2", "w", "marg")]))
  # top-up if dedup dropped us below B
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

## ---------- 6. FINAL: test split, once ----------------------------------
message("-- final on TEST split --")
base_pairs <- sub_topk(te_idx, k_star)
B_test <- nrow(base_pairs)

wfA <- waterfill(te_idx, B_test, margA)
wfB <- waterfill(te_idx, B_test, margB)

rep <- function(pairs, tag) {
  st <- er_block_stats(pairs[, c("idx1", "idx2")], n = length(te_idx),
                       truth = gold_tbl[te_idx, , drop = FALSE],
                       id_vec = row_ids[te_idx])
  message(sprintf("%-28s pairs=%d PC=%.4f RR=%.4f", tag, st$n_candidate_pairs,
                  st$pair_completeness, st$reduction_ratio))
  st$pair_completeness
}
pc_base <- rep(base_pairs, "BASELINE (fixed k)")
pc_A <- rep(wfA, "MARG-ALLOC labeled-Platt")
pc_B <- rep(wfB, "MARG-ALLOC unlabeled-mixture")

# diagnostics: where did the budget go?
kdist <- function(pairs, tag) {
  kk <- table(factor(c(pairs$idx1, pairs$idx2), levels = seq_len(length(te_idx))))
  message(sprintf("[%s] per-record pairs: mean=%.1f max=%d <k* share=%.2f",
                  tag, mean(kk), max(kk), mean(kk < k_star)))
}
kdist(base_pairs, "baseline")
kdist(wfA, "margalloc-A")
kdist(wfB, "margalloc-B")

## ---------- 7. kill lines (one per variant) ------------------------------
dA <- pc_A - pc_base
dB <- pc_B - pc_base
message(sprintf("== KILL LINE (A, labeled Platt)   : dPC = %+.4f ==", dA))
if (is.na(dA) || dA <= 0) {
  message("VERDICT A: dead on this dataset (marginal allocation <= fixed-k).")
} else {
  message("VERDICT A: signal alive — proceed to full protocol (+-2% budget, ",
          "permutation test, D10K + MusicBrainz).")
}
message(sprintf("== KILL LINE (B, unlabeled mix)   : dPC = %+.4f ==", dB))
if (is.na(dB) || dB <= 0) {
  message("VERDICT B: dead on this dataset (marginal allocation <= fixed-k).")
} else {
  message("VERDICT B: signal alive — proceed to full protocol.")
}
