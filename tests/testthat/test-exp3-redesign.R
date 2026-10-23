# Exp 3 redesign guardrails (2026-06-13). Run via:
#   Rscript tests/run_exp3_redesign_tests.R
# Validates the linkage-preserving NCVR builder and the redesigned run script:
# party retained, truth/key excluded from features, cross-party-only blocking,
# block stats, party-balanced sampling, and no use of the old pooled file.

source(file.path(.erbot_root, "experiments", "prep_ncvr_linkage.R"))

# small reproducible 2-party build for the tests
.build <- function() er_build_ncvr_linkage(2L, records_per_party = 300L, seed = 7L,
                                           read_nrows = 30000L)

test_that("source_id (party) is retained", {
  d <- .build()$data
  expect_true("source_id" %in% names(d))
  expect_setequal(sort(unique(d$source_id)), c(0L, 1L))
})

test_that("entity_id (truth) is present and party-balanced sampling holds", {
  b <- .build(); d <- b$data
  expect_true("entity_id" %in% names(d))
  tab <- table(d$source_id)
  expect_true(all(tab == 300L))                 # exactly records_per_party per party
})

test_that("feature set excludes entity_id and source_id", {
  expect_false("entity_id" %in% NCVR_FEATURE_FIELDS)
  expect_false("source_id" %in% NCVR_FEATURE_FIELDS)
  expect_false(erbot:::.er_is_label_column("entity_id") == FALSE)  # entity_id is a label
  # the diagnosed/used features are exactly the 4 content fields
  expect_setequal(NCVR_FEATURE_FIELDS, c("given_name","surname","suburb","postcode"))
})

test_that("blocking with source_col yields ONLY cross-party candidate pairs", {
  d <- .build()$data
  pairs <- er_block(d, method = "prefix", block_key = "surname", prefix_len = 6L,
                    source_col = "source_id", max_pairs = 2e6)
  expect_gt(nrow(pairs), 0)
  same_src <- d$source_id[pairs$idx1] == d$source_id[pairs$idx2]
  expect_false(any(same_src))                   # no same-party pairs survive
})

test_that("true cross-party pairs exist in the sample", {
  d <- .build()$data
  ent_np <- tapply(d$source_id, d$entity_id, function(s) length(unique(s)))
  expect_gt(sum(ent_np >= 2L), 0)               # some entities appear in both parties
})

test_that("block stats compute (RR, PC) against cross-party gold", {
  d <- .build()$data
  id_vec <- as.character(d$id)
  gold <- data.frame(id = id_vec, cluster_id = as.integer(factor(d$entity_id)))
  pairs <- er_block(d, method = "prefix", block_key = "surname", prefix_len = 6L,
                    source_col = "source_id", max_pairs = 2e6)
  bs <- er_block_stats(pairs, nrow(d), truth = gold, id_vec = id_vec)
  expect_true(is.finite(bs$reduction_ratio))
  expect_true(is.finite(bs$pair_completeness))
  expect_gte(bs$pair_completeness, 0); expect_lte(bs$pair_completeness, 1)
})

test_that("entity-aware sampling preserves a non-trivial overlap rate", {
  m <- .build()$manifest
  expect_gt(m$n_true_pairs, 0)
  expect_gt(m$overlap_rate_pct, 1)              # meaningfully above the random-collision floor
})

test_that("redesigned script does NOT reference the old pooled ncvr_2party_300k", {
  for (f in c("run_exp3_redesigned_scalability.R", "prep_ncvr_linkage.R",
              "run_exp3_redesigned_plan.R")) {
    src <- paste(readLines(file.path(.erbot_root, "experiments", f)), collapse = "\n")
    expect_false(grepl("ncvr_2party_300k", src), label = paste(f, "uses old pooled file"))
  }
})

test_that("redesigned run script enforces source_col and excludes truth/key", {
  src <- paste(readLines(file.path(.erbot_root, "experiments",
                                   "run_exp3_redesigned_scalability.R")), collapse = "\n")
  expect_true(grepl("source_col\\s*=\\s*NCVR_SOURCE_COL", src))
  expect_true(grepl("NCVR_FEATURE_FIELDS", src))
})

test_that("modified/new files parse", {
  for (f in c("experiments/prep_ncvr_linkage.R",
              "experiments/run_exp3_redesigned_scalability.R",
              "experiments/run_exp3_redesigned_plan.R"))
    expect_no_error(parse(file.path(.erbot_root, f)))
})
