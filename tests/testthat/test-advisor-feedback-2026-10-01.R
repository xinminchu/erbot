# Advisor-feedback implementation tests (2026-10-01).
# Run via: Rscript tests/run_advisor_feedback_tests.R
# Covers Items 1, 3, 4a, 4b without requiring the real D10K / MusicBrainz
# downloads: toy inputs only. These tests validate interfaces and invariants,
# not benchmark performance.

test_that("scenario presets are named and complete", {
  presets <- er_scenario_presets()
  expect_true(all(c("smoke", "screening", "full", "unsupervised_quick") %in% presets$scenario))
  expect_true(all(c("scenario", "purpose", "cluster_methods", "weights", "merge") %in% names(presets)))
})

test_that("er_explain_error gives stage, reason, and next step", {
  ex <- er_explain_error(simpleError("Built-in dataset not found: data/musicbrainz20k.csv"))
  expect_true(nzchar(ex$stage))
  expect_true(grepl("could not be found", ex$reason))
  expect_true(grepl("er_build_musicbrainz", ex$next_step))
})

test_that("stratified three-way split keeps entities whole and reports positives", {
  ids <- as.character(1:12)
  truth <- tibble::tibble(id = ids, cluster_id = rep(1:6, each = 2L))
  sp <- er_stratified_three_way_split(
    ids, truth,
    fractions = c(fit = 1/3, validation = 1/3, test = 1/3),
    min_positive_pairs = c(fit = 1, validation = 1, test = 1),
    seed = 42L
  )
  expect_equal(sum(sp$report$n_records), 12L)
  expect_true(all(sp$report$positive_pairs >= 1))
  # Entity indivisibility: both records of entity 1 share one partition.
  p1 <- sp$assignment$partition[sp$assignment$id %in% c("1", "2")]
  expect_equal(length(unique(p1)), 1L)
})

test_that("Fit-only augmentation reaches the target ratio and never touches Validation/Test", {
  pairs <- tibble::tibble(idx1 = c(1L, 1L, 3L), idx2 = c(2L, 3L, 4L))
  truth_vec <- c(1L, 1L, 2L, 2L)
  aug <- er_augment_fit_pairs(pairs, truth_vec, fit_idx = 1:4,
                              target_ratio = 0.8, seed = 42L)
  expect_true(isTRUE(attr(aug$pairs, "simulated_prevalence")))
  expect_equal(aug$report$simulated_positive_pair_ratio, 0.8)
  expect_true(all(aug$pairs$source_partition == "fit"))
  expect_equal(aug$report$natural_positive_pair_ratio, 2/3)
})

test_that("augmentation refuses a Fit partition with zero positives", {
  pairs <- tibble::tibble(idx1 = c(1L, 2L), idx2 = c(3L, 4L))
  truth_vec <- c(1L, 2L, 3L, 4L)
  expect_error(
    er_augment_fit_pairs(pairs, truth_vec, fit_idx = 1:4, target_ratio = 0.5),
    "zero positive"
  )
})

test_that("D10K aggregate parser splits key=value structure and keeps unstructured text whole", {
  kv <- er_parse_d10k_aggregate(c(
    "name=John Smith; year=1970; city=Oslo",
    "name=Jon Smith; year=1970; city=Oslo"
  ))
  expect_true(all(c("id", "name", "year", "city") %in% names(kv)))

  plain <- er_parse_d10k_aggregate(c("just one free text value"))
  expect_true("text" %in% names(plain))
})

test_that("D10K similarity returns a sparse matrix on a toy input", {
  df <- data.frame(
    id = 1:4,
    text = c("john smith oslo", "john smith oslo",
             "mary jones bergen", "mary jones bergen"),
    stringsAsFactors = FALSE
  )
  pairs <- tibble::tibble(idx1 = c(1L, 1L, 3L), idx2 = c(2L, 3L, 4L))
  out <- er_d10k_similarity(df, pairs = pairs)
  expect_equal(dim(out$S), c(4L, 4L))
  expect_gt(out$S[1, 2], out$S[1, 3])
})

test_that("MusicBrainz builder keeps CID in gold only (toy .dapo)", {
  dapo <- file.path(tempdir(), "musicbrainz_toy.csv")
  utils::write.csv(
    data.frame(
      TID = 1:3, CID = c(10L, 10L, 11L), CTID = c(1L, 2L, 1L),
      SourceID = c("s1", "s2", "s1"), id = c("a", "b", "c"),
      number = c(1L, 1L, 2L), title = c("Song", "Song", "Other"),
      length = c(200L, 201L, 180L), artist = c("Artist", "Artist", "Other"),
      album = c("Album", "Album", "Other"), year = c(2000L, 2000L, 2001L),
      language = c("en", "en", "en"),
      stringsAsFactors = FALSE
    ),
    dapo, row.names = FALSE
  )
  out_dir <- file.path(tempdir(), "musicbrainz_toy_out")
  out <- er_build_musicbrainz(dapo, out_dir = out_dir, verbose = FALSE)
  expect_false(any(c("cid", "cluster_id", "ctid") %in% names(out$combined)))
  expect_true(all(c("id", "source", "title", "artist") %in% names(out$combined)))
  expect_equal(out$gold$cluster_id, c(10L, 10L, 11L))
  expect_true(file.exists(file.path(out_dir, "musicbrainz20k.csv")))
  expect_true(file.exists(file.path(out_dir, "musicbrainz20k_gold.csv")))
})

test_that("new advisor-feedback R files parse cleanly", {
  for (f in c("R/21-scenarios.R", "R/22-positive_ratio.R",
              "R/23-d10k_similarity.R", "R/14-datasets.R",
              "R/01-load.R", "scripts/run_scenario.R")) {
    parsed <- tryCatch({
      parse(file.path(.erbot_root, f))
      TRUE
    }, error = function(e) FALSE)
    expect_true(parsed, info = f)
  }
})
