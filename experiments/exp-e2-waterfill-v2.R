# E2 FULL PROTOCOL v2: marginal-allocation blocking ("water-filling")
# v2.1 fix (2026-10-20): the test-report helper was named `rep`, shadowing base::rep;
# bow_topk() calls rep() internally, so the permutation section crashed with
# "incorrect number of dimensions". Helper renamed to report_pc. No logic change.
#
# The prototype (exp-e2-waterfill.R, 2026-10-20) passed both kill lines on
# cora with a large effect (dPC = +0.3227 at -1.5% budget, inside the +-2%
# paper tolerance). v2 adds the pre-registered missing piece:
#
#   Permutation test for the ALLOCATION claim. The claim is not "a big
#   candidate pool helps" but "WHO gets the budget matters". Null: permute
#   the observed per-record budget profile (pair degrees of the treatment
#   result) across records, re-select each record's pairs by marginal value
#   under its permuted budget, dedup + top-up/trim to exactly B with the
#   same rules as waterfill(), recompute PC. R = 199 reps per variant;
#   empirical p for the observed dPC reported next to each kill line.
#
# Usage (from the erbot package root, on a machine with R + package deps):
#   Rscript experiments/exp-e2-waterfill-v2.R cora
#   Rscript experiments/exp-e2-waterfill-v2.R d10k
#   Rscript experiments/exp-e2-waterfill-v2.R musicbrainz20k
# (d10k: records from data/10Kfull.csv via er_load(); gold derived from
#  data/10Kduplicates.csv pairs by the section-0 shim. musicbrainz20k: run
#  experiments/build-musicbrainz20k.R once first to build the data files.)
# v2.2 (2026-10-20): section-0 dataset shims (d10k gold from duplicate
# pairs; musicbrainz20k from builder output). Cora path unchanged from v2.1.
#
# Honesty discipline (unchanged): entity-disjoint 70/30 split, baseline k
# AND both calibrations fit on TRAIN entities only, TEST reported once,
# treatment trimmed to exactly B_test pairs, per-record floor >= 1 pair,
# TWO kill lines (one per calibration variant).

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1L) args[1L] else "cora"
SEED <- 20261016L
M_POOL <- 200L   # per-record candidate pool water-filling selects from
R_PERM <- 199L

if (requireNamespace("pkgload", quietly = TRUE)) {
  suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
} else {
  for (f in list.files("R", pattern = "\\.[rR]$", full.names = TRUE)) source(f)
}
suppressPackageStartupMessages({
  library(Matrix)
})

message("== E2 full protocol v2 | dataset: ", dataset, " ==")

## ---------- 0. load data + gold, align -------------------------------
# v2.2 dataset shims. The package loaders do not cover every dataset:
# er_load_gold() supports only cora/restaurant/dblp_acm, and er_load() has
# no musicbrainz20k key (there is no er_build_musicbrainz() in the package).
#   d10k           records via er_load(); gold derived here from the
#                  duplicate-PAIRS file data/10Kduplicates.csv (Entity1|Entity2)
#                  as connected components; unpaired records are singletons.
#   musicbrainz20k records + gold from data/musicbrainz20k.csv and
#                  data/musicbrainz20k_gold.csv, built once by
#                  experiments/build-musicbrainz20k.R.
#   anything else  package loaders, unchanged (cora path identical to v2.1).
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
sepB <- mixB$mu[2L] - mixB$mu[1L]
message("mixture separation (mu_high - mu_low) = ", round(sepB, 4))

## ---------- 5. water-filling allocator ----------------------------------
# global top-B by marginal value == the marginal-allocation ("water-filling")
# optimum (marginals decrease with rank inside each record). Per-record floor
# of 1 pair first. (Prototype code, unchanged.)
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

## ---------- 6. FINAL: test split, once ----------------------------------
message("-- final on TEST split --")
base_pairs <- sub_topk(te_idx, k_star)
B_test <- nrow(base_pairs)

wfA <- waterfill(te_idx, B_test, margA)
wfB <- waterfill(te_idx, B_test, margB)

report_pc <- function(pairs, tag) {
  st <- er_block_stats(pairs[, c("idx1", "idx2")], n = length(te_idx),
                       truth = gold_tbl[te_idx, , drop = FALSE],
                       id_vec = row_ids[te_idx])
  message(sprintf("%-28s pairs=%d PC=%.4f RR=%.4f", tag, st$n_candidate_pairs,
                  st$pair_completeness, st$reduction_ratio))
  st$pair_completeness
}
pc_base <- report_pc(base_pairs, "BASELINE (fixed k)")
pc_A <- report_pc(wfA, "MARG-ALLOC labeled-Platt")
pc_B <- report_pc(wfB, "MARG-ALLOC unlabeled-mixture")

# diagnostics: where did the budget go?
kdist <- function(pairs, tag) {
  kk <- table(factor(c(pairs$idx1, pairs$idx2), levels = seq_len(length(te_idx))))
  message(sprintf("[%s] per-record pairs: mean=%.1f max=%d <k* share=%.2f",
                  tag, mean(kk), max(kk), mean(kk < k_star)))
}
kdist(base_pairs, "baseline")
kdist(wfA, "margalloc-A")
kdist(wfB, "margalloc-B")

## ---------- 6b. permutation test: allocation profile --------------------
# Null: only the total budget B and the candidate pool matter; the observed
# per-record budget profile (pair degrees of the treatment result) is
# exchangeable across records. Selection under a permuted profile uses the
# same rules as waterfill(): per-record top by marginal value, then
# dedup + top-up by marginal value (trim by marginal value if over B).
perm_test_alloc <- function(idxs, B, marg_fn, alloc_obs, pc_base, seed) {
  Xs <- Xn[idxs, , drop = FALSE]
  ns <- length(idxs)
  pool <- bow_topk(Xs, M_POOL)
  pool$marg <- marg_fn(pool$w)
  pool <- pool[order(pool$idx1, -pool$marg), , drop = FALSE]
  rows_by_rec <- split(seq_len(nrow(pool)), pool$idx1)
  select_by_alloc <- function(alloc) {
    take <- unlist(lapply(seq_len(ns), function(i) {
      rr <- rows_by_rec[[as.character(i)]]
      if (is.null(rr)) integer(0) else head(rr, alloc[i])
    }))
    sel <- undirect(pool[take, c("idx1", "idx2", "w", "marg"), drop = FALSE])
    if (nrow(sel) > B) {
      sel <- sel[order(-sel$marg), , drop = FALSE][seq_len(B), , drop = FALSE]
    }
    guard <- 0L
    while (nrow(sel) < B && guard < 5L) {
      guard <- guard + 1L
      used <- paste(sel$idx1, sel$idx2, sep = "_")
      cand <- pool[!(paste(pmin(pool$idx1, pool$idx2),
                           pmax(pool$idx1, pool$idx2), sep = "_") %in% used),
                   , drop = FALSE]
      cand <- cand[order(-cand$marg), , drop = FALSE]
      if (!nrow(cand)) break
      sel <- undirect(rbind(sel, head(cand, B - nrow(sel))[, c("idx1", "idx2", "w", "marg")]))
    }
    sel
  }
  set.seed(seed)
  null_pc <- numeric(R_PERM)
  for (r in seq_len(R_PERM)) {
    sp <- select_by_alloc(sample(alloc_obs))
    null_pc[r] <- pc_of(sp[, c("idx1", "idx2")], idxs)
    if (r %% 50L == 0L) message("  perm ", r, " / ", R_PERM)
  }
  null_pc
}

alloc_of <- function(pairs) {
  as.integer(table(factor(c(pairs$idx1, pairs$idx2),
                          levels = seq_len(length(te_idx)))))
}
message("-- permutation test: allocation profile, variant A (R = ", R_PERM, ") --")
nullA <- perm_test_alloc(te_idx, B_test, margA, alloc_of(wfA), pc_base, SEED + 7L)
p_A <- (1 + sum((nullA - pc_base) >= (pc_A - pc_base))) / (1 + R_PERM)
message(sprintf("perm A: null dPC mean = %+.4f (sd %.4f) | observed dPC = %+.4f | empirical p = %.4f",
                mean(nullA - pc_base), stats::sd(nullA), pc_A - pc_base, p_A))
message("-- permutation test: allocation profile, variant B (R = ", R_PERM, ") --")
nullB <- perm_test_alloc(te_idx, B_test, margB, alloc_of(wfB), pc_base, SEED + 13L)
p_B <- (1 + sum((nullB - pc_base) >= (pc_B - pc_base))) / (1 + R_PERM)
message(sprintf("perm B: null dPC mean = %+.4f (sd %.4f) | observed dPC = %+.4f | empirical p = %.4f",
                mean(nullB - pc_base), stats::sd(nullB), pc_B - pc_base, p_B))

## ---------- 7. kill lines (one per variant) ------------------------------
dA <- pc_A - pc_base
dB <- pc_B - pc_base
message(sprintf("== KILL LINE (A, labeled Platt)   : dPC = %+.4f ==", dA))
if (is.na(dA) || dA <= 0) {
  message("VERDICT A: dead on this dataset (marginal allocation <= fixed-k).")
} else if (p_A >= 0.05) {
  message("VERDICT A: dPC positive BUT permutation p = ", round(p_A, 4),
          " >= 0.05 -- the allocation profile is not distinguishable from ",
          "a random profile at this budget. Treat as WEAK on ", dataset, ".")
} else {
  message("VERDICT A: signal alive AND permutation-supported (p = ",
          round(p_A, 4), ") on ", dataset, ".")
}
message(sprintf("== KILL LINE (B, unlabeled mix)   : dPC = %+.4f ==", dB))
if (is.na(dB) || dB <= 0) {
  message("VERDICT B: dead on this dataset (marginal allocation <= fixed-k).")
} else if (p_B >= 0.05) {
  message("VERDICT B: dPC positive BUT permutation p = ", round(p_B, 4),
          " >= 0.05 -- treat as WEAK on ", dataset, ".")
} else {
  message("VERDICT B: signal alive AND permutation-supported (p = ",
          round(p_B, 4), ") on ", dataset, ".")
}
