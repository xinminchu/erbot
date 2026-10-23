# Label-leakage guardrails (BUG-22 fix, 2026-06-12).
# Run via: Rscript tests/run_label_leakage_tests.R
# Prevents truth/label columns (cluster_id, class, entity_id, ...) from ever
# re-entering the similarity feature set. See BUG22_LEAKAGE_ANALYSIS.md /
# BUG22_REMEDIATION_REPORT.md.

# ── Helper: is a per-pair similarity vector ~1.0 on ALL gold pairs? ────────────
.field_perfect_on_gold <- function(sim_vec, gold_mask, tol = 1e-6) {
  g <- sim_vec[gold_mask]; g <- g[!is.na(g)]
  length(g) > 0 && all(abs(g - 1) < tol)
}

# ── Helper: does a field LOOK LIKE a truth leak? ──────────────────────────────
# Leak signature = perfect on every gold pair AND id-like (high distinct-value
# ratio, i.e. the value essentially IS the entity id). This distinguishes a
# leaked label (cluster_id: card ratio ~ n_entities/n_records) from a
# legitimately strong exact-match field (year: ~10 distinct values, card ratio
# ~ 0.002, can also be perfect on gold without leaking).
.field_leaks_on_gold <- function(sim_vec, gold_mask, col_values,
                                 card_thresh = 0.3) {
  card <- length(unique(col_values)) / max(length(col_values), 1L)
  .field_perfect_on_gold(sim_vec, gold_mask) && card > card_thresh
}

# ── 1. .er_is_label_column unit tests ─────────────────────────────────────────
test_that(".er_is_label_column flags truth columns, case/punct-insensitive", {
  lab <- c("cluster_id", "class", "entity_id", "label", "truth",
           "gold", "gold_id", "match_id", "cluster")
  expect_true(all(erbot:::.er_is_label_column(lab)))
  # case + punctuation insensitivity
  expect_true(all(erbot:::.er_is_label_column(
    c("Cluster_ID", "CLASS", "Entity-Id", "match.id", "Gold"))))
  # legitimate fields must NOT be flagged
  legit <- c("name", "addr", "city", "phone", "type", "title", "authors",
             "venue", "year", "address", "journal", "institution")
  expect_false(any(erbot:::.er_is_label_column(legit)))
})

test_that(".er_is_label_column honours the extra= argument", {
  expect_false(erbot:::.er_is_label_column("my_secret_label"))
  expect_true(erbot:::.er_is_label_column("my_secret_label",
                                          extra = "my_secret_label"))
})

# ── 2. er_diagnose excludes label columns (regression guard) ──────────────────
test_that("er_diagnose drops cluster_id/class from Restaurant fields", {
  d <- suppressWarnings(er_diagnose(er_load("restaurant")))
  expect_false(any(erbot:::.er_is_label_column(d$fields$name)))
  expect_true(all(c("class", "cluster_id") %in% d$label_dropped))
  expect_setequal(d$fields$name, c("name", "addr", "city", "phone", "type"))
})

test_that("er_diagnose drops cluster_id from DBLP-ACM fields", {
  d <- suppressWarnings(er_diagnose(er_load("dblp_acm")))
  expect_false(any(erbot:::.er_is_label_column(d$fields$name)))
  expect_true("cluster_id" %in% d$label_dropped)
  expect_setequal(d$fields$name, c("title", "authors", "venue", "year"))
})

test_that("er_diagnose leaves CORA (leak-free) unchanged", {
  d <- suppressWarnings(er_diagnose(er_load("cora")))
  expect_equal(length(d$label_dropped), 0L)
  expect_equal(nrow(d$fields), 15L)
  expect_false(any(erbot:::.er_is_label_column(d$fields$name)))
})

test_that("er_diagnose emits a warning when it drops a label column", {
  expect_warning(er_diagnose(er_load("dblp_acm")), "BUG-22|label")
})

test_that("explicit text_cols cannot force a label column back in", {
  # Even if the caller asks for cluster_id, the guard removes it.
  d <- suppressWarnings(er_diagnose(
    er_load("dblp_acm"),
    text_cols = c("title", "authors", "cluster_id")))
  expect_false("cluster_id" %in% d$fields$name)
  expect_true("cluster_id" %in% d$label_dropped)
})

# ── 3. Diagnosed field names never contain truth identifiers ──────────────────
test_that("no diagnosed field name is a truth identifier (all benchmarks)", {
  for (ds in c("cora", "restaurant", "dblp_acm")) {
    d <- suppressWarnings(er_diagnose(er_load(ds)))
    expect_false(any(erbot:::.er_is_label_column(d$fields$name)),
                 label = sprintf("%s has a label field", ds))
  }
})

# ── 4. No diagnosed field is perfect on all gold pairs (the real guardrail) ───
test_that("Restaurant: leaked field looks like a leak; diagnosed fields do NOT", {
  df    <- er_load("restaurant")
  pairs <- er_block(df, method = "prefix", block_key = "name",
                    source_col = "source", max_pairs = 2e6)
  gmask <- df$cluster_id[pairs$idx1] == df$cluster_id[pairs$idx2]
  expect_gt(sum(gmask), 0)

  # the leaked field, forced in via an explicit spec, is flagged as a leak
  sim_leak <- er_similarity(df, pairs,
                            spec = list(list(name = "cluster_id", type = "numeric")))
  expect_true(.field_leaks_on_gold(sim_leak[["cluster_id"]], gmask, df$cluster_id))

  # the DEFAULT (post-fix) field set contains NO leak-like field
  sim_def <- er_similarity(df, pairs, diag = suppressWarnings(er_diagnose(df)))
  leaks <- vapply(names(sim_def),
                  function(fn) .field_leaks_on_gold(sim_def[[fn]], gmask, df[[fn]]),
                  logical(1))
  expect_false(any(leaks), label = paste("leak-like field present:",
                                         paste(names(sim_def)[leaks], collapse = ",")))
})

test_that("DBLP-ACM: diagnosed field set has no leak-like field (year is legit)", {
  df    <- er_load("dblp_acm")
  pairs <- er_block(df, method = "prefix", block_key = "title",
                    source_col = "source", max_pairs = 2e6)
  gmask <- df$cluster_id[pairs$idx1] == df$cluster_id[pairs$idx2]
  expect_gt(sum(gmask), 0)
  sim_leak <- er_similarity(df, pairs,
                            spec = list(list(name = "cluster_id", type = "numeric")))
  expect_true(.field_leaks_on_gold(sim_leak[["cluster_id"]], gmask, df$cluster_id))
  sim_def <- er_similarity(df, pairs, diag = suppressWarnings(er_diagnose(df)))
  leaks <- vapply(names(sim_def),
                  function(fn) .field_leaks_on_gold(sim_def[[fn]], gmask, df[[fn]]),
                  logical(1))
  # year is perfect-on-gold but low-cardinality -> NOT flagged; cluster_id is gone
  expect_false(any(leaks))
})

# ── 5. Source parse guard ─────────────────────────────────────────────────────
test_that("modified R/02-diagnose.R parses", {
  expect_no_error(parse(file.path(.erbot_root, "R", "02-diagnose.R")))
})
