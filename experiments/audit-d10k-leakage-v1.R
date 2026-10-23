# AUDIT v1: why is the d10k baseline PC = 1.0000? Truth leakage or a
# trivially easy dataset?
#
# Standalone audit for the E1/E2 full-protocol v2.2 d10k runs. It reuses
# the experiments' exact pipeline pieces (section-0 gold shim, char 3-gram
# BoW cosine, entity-disjoint 70/30 split, SEED 20261012) and measures
# gold against similarities WITHOUT ever letting gold near the similarity
# construction -- same discipline as the experiments themselves.
#
# What would count as leakage, decided in advance:
#   (L1) a "signature token": a token that appears in EVERY member of a
#        gold cluster and NOWHERE else, and looks planted (long digit
#        string / long opaque token) rather than like a natural value
#        (a shared surname is not leakage; a shared 10-digit code is).
#   (L2) record Id strings embedded in the Aggregate Value text.
# What would count as "dataset is just easy":
#   (E1) test gold pairs are near-copies (cosine ~ 1), and/or PC is
#        already ~1 at tiny k (k = 5), so k = 50 was never the binding
#        constraint and no hidden channel is needed to explain PC = 1.
#
# Usage (from the erbot package root, where data/10Kfull.csv lives):
#   Rscript experiments/audit-d10k-leakage-v1.R
# Output is compact on purpose. Final line is the VERDICT.

SEED <- 20261012L

if (requireNamespace("pkgload", quietly = TRUE)) {
  suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
} else {
  for (f in list.files("R", pattern = "\\.[rR]$", full.names = TRUE)) source(f)
}
suppressPackageStartupMessages(library(Matrix))

## ---------- 0. load records + gold (verbatim shim from E1/E2 v2.2) -----
find_data_file <- function(fname) {
  for (p in c(file.path("data", fname), file.path("inst", "extdata", fname)))
    if (file.exists(p)) return(p)
  stop("data file not found: ", fname)
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

df <- er_load("d10k")
n <- nrow(df)
row_ids <- as.character(df$id)
gold <- gold_from_pairs(find_data_file("10Kduplicates.csv"), row_ids)
gvec <- setNames(as.integer(gold$cluster_id), as.character(gold$id))
crow <- unname(gvec[row_ids])
keep <- !is.na(crow)
df <- df[keep, , drop = FALSE]; crow <- crow[keep]; row_ids <- row_ids[keep]
n <- nrow(df)
texts <- as.character(df$text)

cat("== A. GOLD STRUCTURE ==\n")
sz <- table(table(crow))
cat("records:", n, " entities:", length(unique(crow)), "\n")
cat("cluster sizes (size x #clusters):",
    paste(names(sz), "x", sz, collapse = ", "), "\n")
tp_all <- sum(choose(table(crow), 2))
cat("total true pairs (within-cluster):", tp_all, "\n")

## ---------- split: identical to E1/E2 v2.2 ------------------------------
set.seed(SEED)
ents <- unique(crow)
train_ents <- sample(ents, size = floor(0.7 * length(ents)))
tr_idx <- which(crow %in% train_ents)
te_idx <- which(!crow %in% train_ents)
tp_of <- function(idxs) sum(choose(table(crow[idxs]), 2))
cat("train rows:", length(tr_idx), " true pairs:", tp_of(tr_idx),
    " | test rows:", length(te_idx), " true pairs:", tp_of(te_idx), "\n")

## ---------- B. EXACT-TEXT DUPLICATION -----------------------------------
cat("\n== B. EXACT-TEXT DUPLICATION ==\n")
dup_text <- duplicated(texts) | duplicated(texts, fromLast = TRUE)
cat("records whose Aggregate Value is byte-identical to another record's:",
    sum(dup_text), "/", n, sprintf(" (%.1f%%)\n", 100 * mean(dup_text)))
cl_members <- split(seq_len(n), crow)
gold_pairs <- do.call(rbind, lapply(cl_members, function(m) {
  if (length(m) < 2L) return(NULL)
  gr <- utils::combn(m, 2L); t(gr)
}))
same_text <- texts[gold_pairs[, 1]] == texts[gold_pairs[, 2]]
cat("gold pairs with byte-identical text:", sum(same_text), "/",
    nrow(gold_pairs), sprintf(" (%.1f%%)\n", 100 * mean(same_text)))

## ---------- C. ID-IN-TEXT + SIGNATURE TOKENS ----------------------------
cat("\n== C. ID-IN-TEXT AND CLUSTER-SIGNATURE TOKENS ==\n")
id_hit <- vapply(seq_len(n), function(i) grepl(row_ids[i], texts[i], fixed = TRUE),
                 logical(1))
cat("records whose own Id string appears inside their text:", sum(id_hit), "\n")

tok <- strsplit(tolower(texts), "[^a-z0-9]+")
tok <- lapply(tok, function(x) unique(x[nzchar(x)]))
dfreq <- table(unlist(tok))
multi <- cl_members[lengths(cl_members) >= 2L]
sig_found <- list()
for (cid in names(multi)) {
  m <- multi[[cid]]
  common <- Reduce(intersect, tok[m])
  if (!length(common)) next
  rare <- common[dfreq[common] == length(m)]   # in all members, nowhere else
  if (length(rare)) sig_found[[cid]] <- rare
}
cat("multi-record clusters:", length(multi),
    " | clusters with >=1 signature token (in all members, nowhere else):",
    length(sig_found), "\n")
planted_like <- function(tk) grepl("^[0-9]{4,}$", tk) || (nchar(tk) >= 12 & !grepl("[aeiou]", tk))
n_planted <- 0L
if (length(sig_found)) {
  ex <- head(sig_found, 10)
  for (cid in names(ex)) {
    tk <- ex[[cid]]
    flag <- any(vapply(tk, planted_like, logical(1)))
    if (flag) n_planted <- n_planted + 1L
    cat("  cluster", cid, "size", length(multi[[cid]]), "tokens:",
        paste(head(tk, 6), collapse = ", "), if (flag) " <-- PLANTED-LIKE" else "", "\n")
  }
}
cat("clusters with planted-like signature tokens (shown range):", n_planted, "\n")

## ---------- D. TEST GOLD-PAIR COSINES + RANKS ---------------------------
cat("\n== D. TEST GOLD PAIRS: COSINE AND NEIGHBOR RANK ==\n")
shingles_of <- function(s) {
  s <- gsub("[^a-z0-9 ]", " ", tolower(s)); s <- gsub("\\s+", " ", s)
  chars <- strsplit(s, "")[[1L]]; L <- length(chars)
  if (L < 3L) return(if (L == 0L) character(0L) else s)
  unique(vapply(seq_len(L - 2L), function(i) paste(chars[i:(i + 2L)], collapse = ""), character(1L)))
}
sh_list <- lapply(texts, shingles_of)
all_sh <- unique(unlist(sh_list)); sh_id <- setNames(seq_along(all_sh), all_sh)
ii <- rep(seq_len(n), times = vapply(sh_list, length, integer(1L)))
jj <- unlist(lapply(sh_list, function(s) unname(sh_id[s])))
X <- Matrix::sparseMatrix(i = ii, j = jj, x = 1, dims = c(n, length(all_sh)))
rs <- sqrt(Matrix::rowSums(X ^ 2)); rs[rs == 0] <- 1
Xn <- Matrix::Diagonal(x = 1 / rs) %*% X

Xte <- Xn[te_idx, , drop = FALSE]
Cte <- as.matrix(Xte %*% Matrix::t(Xte))
diag(Cte) <- -Inf
pos_in_te <- match(gold_pairs[, 1], te_idx); pos2_in_te <- match(gold_pairs[, 2], te_idx)
ok <- !is.na(pos_in_te) & !is.na(pos2_in_te)
gp <- cbind(pos_in_te[ok], pos2_in_te[ok])
gcos <- Cte[cbind(gp[, 1], gp[, 2])]
rank_of <- function(a, b, mat) sum(mat[a, ] > mat[a, b]) + 1L
grank <- pmax(mapply(rank_of, gp[, 1], gp[, 2], MoreArgs = list(mat = Cte)),
              mapply(rank_of, gp[, 2], gp[, 1], MoreArgs = list(mat = Cte)))
cat("test gold pairs:", length(gcos), "\n")
cat("cosine quantiles: min", sprintf("%.4f", min(gcos)),
    " p1", sprintf("%.4f", stats::quantile(gcos, 0.01)),
    " p5", sprintf("%.4f", stats::quantile(gcos, 0.05)),
    " median", sprintf("%.4f", stats::median(gcos)), "\n")
cat("worst mate-rank across endpoints: max", max(grank),
    " | pairs with rank > 50:", sum(grank > 50), "\n")
worst <- order(gcos)[seq_len(min(10L, length(gcos)))]
for (w in worst) {
  a <- te_idx[gp[w, 1]]; b <- te_idx[gp[w, 2]]
  cat(sprintf("  LOW-COS pair ids %s / %s cos=%.4f rank=%d\n", row_ids[a], row_ids[b], gcos[w], grank[w]))
  cat("    A:", substr(texts[a], 1, 300), "\n")
  cat("    B:", substr(texts[b], 1, 300), "\n")
}

## ---------- E. PC(k) CURVE ON TEST (baseline only) ----------------------
cat("\n== E. BASELINE PC AT SMALL k (TEST) ==\n")
bow_topk <- function(Xn, k, chunk = 500L) {
  nn <- nrow(Xn); i1 <- integer(0); i2 <- integer(0); ww <- numeric(0)
  for (s in seq(1L, nn, by = chunk)) {
    e <- min(s + chunk - 1L, nn)
    Cm <- as.matrix(Xn[s:e, , drop = FALSE] %*% Matrix::t(Xn))
    for (r in seq_len(nrow(Cm))) {
      row <- Cm[r, ]; row[s + r - 1L] <- -Inf
      tk <- order(row, decreasing = TRUE)[seq_len(min(k, nn - 1L))]
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
gold_tbl <- tibble::tibble(id = row_ids, cluster_id = crow)
pc_at_k <- function(k) {
  prs <- undirect(bow_topk(Xte, k))
  st <- er_block_stats(prs, n = length(te_idx),
                       truth = gold_tbl[te_idx, , drop = FALSE],
                       id_vec = row_ids[te_idx])
  st$pair_completeness
}
pc_k <- vapply(c(2L, 5L, 10L, 20L, 50L), pc_at_k, numeric(1))
cat("PC by k:", paste(c(2, 5, 10, 20, 50), "=", sprintf("%.4f", pc_k), collapse = "  "), "\n")

## ---------- F. E2 TRAIN-POOL POSITIVES RECOUNT --------------------------
cat("\n== F. E2 TRAIN POOL RECOUNT (M_POOL = 200) ==\n")
pool_tr <- bow_topk(Xn[tr_idx, , drop = FALSE], 200L)
y_tr <- as.integer(crow[tr_idx[pool_tr$idx1]] == crow[tr_idx[pool_tr$idx2]])
cat("train pool pairs:", nrow(pool_tr), " positives:", sum(y_tr),
    sprintf(" (%.2f%%)\n", 100 * mean(y_tr)))
cat("(E2 v2.2 archive reported: 1,410,400 pairs, 0.89% positives)\n")

## ---------- VERDICT ------------------------------------------------------
cat("\n== VERDICT ==\n")
if (sum(id_hit) > 0) {
  cat("VERDICT: LEAK (L2) -- record Ids are embedded in the text field.\n")
} else if (n_planted > 0) {
  cat("VERDICT: POSSIBLE LEAK (L1) -- planted-like signature tokens found;",
      "inspect section C examples by eye before trusting PC values.\n")
} else if (pc_k[2] >= 0.999) {
  cat("VERDICT: NO LEAK FOUND -- the dataset saturates at k=5 already;",
      "PC = 1.0000 at k = 50 needs no hidden channel. d10k is a ceiling",
      "case: duplicates are near-copies in one rich text field.\n")
} else if (min(gcos) >= 0.8) {
  cat("VERDICT: NO LEAK FOUND -- every test gold pair is a near-copy",
      "(min cosine >=", sprintf("%.3f", min(gcos)), "); PC = 1.0000 is",
      "explained by duplicate similarity, not by gold access.\n")
} else {
  cat("VERDICT: INCONCLUSIVE -- no signature tokens, but some test gold",
      "pairs have low cosine (min", sprintf("%.3f", min(gcos)), ").",
      "Eyeball the LOW-COS pairs in section D: if their texts share an",
      "opaque code, treat as L1; if they are short/noisy texts, the rank",
      "column shows whether top-50 inclusion is still plausible.\n")
}
cat("Sample records (first 2 Aggregate Values):\n")
cat("  [1]", substr(texts[1], 1, 300), "\n")
cat("  [2]", substr(texts[2], 1, 300), "\n")
