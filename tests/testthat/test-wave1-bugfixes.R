# Wave 1 bugfix validation (2026-06-12, BUGFIX_EXECUTION_PLAN / BUGFIX_VALIDATION_REPORT)
# Run via: Rscript tests/run_wave1_validation.R
# Covers BUG-01, BUG-02, BUG-06, BUG-08, BUG-11(logging), BUG-18, BUG-19.

# ── BUG-06: NMI/VI no longer always NA on a toy clustering ────────────────────
test_that("er_evaluate returns finite NMI and VI on a toy clustering", {
  pred <- c(1L, 1L, 2L, 2L, 3L, 3L)
  gold <- setNames(c(1L, 1L, 2L, 3L, 3L, 3L), as.character(1:6))
  ev <- er_evaluate(pred, gold, id_vec = as.character(1:6))
  expect_true(is.finite(ev$NMI[1]))
  expect_true(is.finite(ev$VI[1]))
  expect_gte(ev$NMI[1], 0); expect_lte(ev$NMI[1], 1)
  expect_gte(ev$VI[1], 0)
})

test_that("NMI/VI are exact on identical and on independent partitions", {
  ids <- as.character(1:8)
  same <- rep(1:4, each = 2L)
  ev_same <- er_evaluate(as.integer(same), setNames(as.integer(same), ids), id_vec = ids)
  expect_equal(ev_same$NMI[1], 1, tolerance = 1e-6)
  expect_equal(ev_same$VI[1], 0, tolerance = 1e-6)

  # direct helper: independent partitions -> I = 0 -> NMI 0, VI = Hx + Hy
  a <- c(1, 1, 2, 2); b <- c(1, 2, 1, 2)
  mv <- erbot:::.er_nmi_vi(a, b)
  expect_equal(unname(mv["NMI"]), 0, tolerance = 1e-9)
  expect_equal(unname(mv["VI"]), 2 * log(2), tolerance = 1e-9)
})

test_that("direct NMI matches aricode sum-variant when available", {
  skip_if_not_installed("aricode")
  set.seed(1)
  a <- sample(1:5, 60, replace = TRUE)
  b <- sample(1:4, 60, replace = TRUE)
  mv <- erbot:::.er_nmi_vi(a, b)
  expect_equal(unname(mv["NMI"]), aricode::NMI(a, b, variant = "sum"),
               tolerance = 1e-8)
})

# ── Backward compatibility: er_evaluate schema unchanged ─────────────────────
test_that("er_evaluate still returns ARI/AMI/Bcubed/H/C/V (and PairF)", {
  pred <- c(1L, 1L, 2L, 2L, 3L, 3L)
  gold <- setNames(c(1L, 1L, 2L, 3L, 3L, 3L), as.character(1:6))
  ev <- er_evaluate(pred, gold, id_vec = as.character(1:6))
  needed <- c("ARI", "AMI", "NMI", "VI", "Bcubed_P", "Bcubed_R", "Bcubed_F",
              "Homogeneity", "Completeness", "Vmeasure",
              "PairF_P", "PairF_R", "PairF_F")
  expect_true(all(needed %in% names(ev)))
  expect_true(is.finite(ev$ARI[1]))
  expect_true(is.finite(ev$AMI[1]))
  expect_true(is.finite(ev$Bcubed_F[1]))
  expect_true(is.finite(ev$Vmeasure[1]))
})

# ── BUG-02 / BUG-01: forbidden patterns removed from exp3 script ─────────────
test_that("run_exp3_scalability.R no longer contains the scoping/unit bugs", {
  src <- readLines(file.path(.erbot_root, "experiments", "run_exp3_scalability.R"))
  code <- src[!grepl("^\\s*#", src)]   # ignore comment lines
  expect_false(any(grepl("ari_val\\s*<<-", code)))
  expect_false(any(grepl("\\*\\s*1024L", code)))
  expect_false(any(grepl("\\.mem_bytes\\s*<-", code)))
  expect_true(any(grepl("er_gc_peak_mb", code)))
  expect_true(any(grepl("gc\\(reset = TRUE", code)))
})

# ── BUG-01: corrected probe detects a known allocation ───────────────────────
test_that("er_gc_peak_mb registers a large transient allocation", {
  invisible(gc(reset = TRUE, verbose = FALSE))
  base <- er_gc_peak_mb()
  x <- numeric(50e6)        # ~400 MB transient
  x[1] <- 1
  peak <- er_gc_peak_mb() - base
  rm(x)
  expect_gt(peak, 300)      # detects at least ~300 MB of the 400 MB
})

# ── BUG-11: exp1 logs errors instead of swallowing them ──────────────────────
test_that("run_exp1_main.R captures condition errors and writes an error log", {
  src <- paste(readLines(file.path(.erbot_root, "experiments", "run_exp1_main.R")),
               collapse = "\n")
  expect_false(grepl("error = function\\(e\\) NULL", src))
  expect_true(grepl("exp1_condition_errors\\.csv", src))
  expect_true(grepl("conditionMessage\\(e\\)", src))
})

# ── BUG-08: all experiment scripts persist the full metric set ───────────────
test_that("experiment row writers persist NMI/VI/PairF/Bcubed_P/Bcubed_R", {
  for (f in c("run_exp1_main.R", "run_exp2_ablation_cora.R",
              "run_exp4_missingness.R", "run_exp5_types.R",
              "run_exp3_scalability.R")) {
    src <- paste(readLines(file.path(.erbot_root, "experiments", f)), collapse = "\n")
    for (col in c("NMI", "VI", "PairF_P", "PairF_R", "PairF_F",
                  "Bcubed_P", "Bcubed_R"))
      expect_true(grepl(col, src, fixed = TRUE),
                  label = sprintf("%s persists %s", f, col))
  }
})

# ── BUG-18: er_safe_write_csv refuses in-place overwrite ─────────────────────
test_that("er_safe_write_csv diverts instead of overwriting", {
  tmpd <- file.path(tempdir(), paste0("wave1_", as.integer(runif(1, 1, 1e6))))
  dir.create(tmpd)
  on.exit(unlink(tmpd, recursive = TRUE), add = TRUE)
  p <- file.path(tmpd, "out.csv")
  df1 <- data.frame(a = 1); df2 <- data.frame(a = 2)
  w1 <- er_safe_write_csv(df1, p)
  expect_equal(normalizePath(w1), normalizePath(p))
  w2 <- suppressMessages(er_safe_write_csv(df2, p))   # exists -> divert
  expect_false(normalizePath(w2) == normalizePath(p))
  expect_true(file.exists(w2))
  expect_equal(read.csv(p)$a, 1)                       # original intact
  expect_equal(read.csv(w2)$a, 2)
  w3 <- er_safe_write_csv(df2, p, overwrite = TRUE)    # explicit overwrite
  expect_equal(read.csv(p)$a, 2)
})

# ── BUG-08/18: experiment scripts use the safe writer for results ────────────
test_that("experiment scripts route result CSVs through er_safe_write_csv", {
  for (f in c("run_exp1_main.R", "run_exp2_ablation_cora.R",
              "run_exp3_scalability.R", "run_exp4_missingness.R",
              "run_exp5_types.R")) {
    src <- readLines(file.path(.erbot_root, "experiments", f))
    code <- src[!grepl("^\\s*#", src)]
    expect_true(any(grepl("er_safe_write_csv\\(", code)),
                label = sprintf("%s uses er_safe_write_csv", f))
    # remaining write.csv calls allowed only for the error log (exp1)
    raw_writes <- grep("write\\.csv\\(", code, value = TRUE)
    raw_writes <- raw_writes[!grepl("er_safe_write_csv", raw_writes)]
    expect_true(all(grepl("errors_df|exp1_condition_errors", raw_writes)),
                label = sprintf("%s has no unsafe result writes", f))
  }
})

# ── BUG-19: library functions no longer clobber the caller RNG ───────────────
test_that("er_split preserves the caller's global RNG state", {
  set.seed(999)
  before <- .Random.seed
  ids <- as.character(1:50)
  truth <- tibble::tibble(id = ids, cluster_id = rep(1:25, each = 2L))
  f1 <- er_split(ids, truth = truth, k = 5L, entity_disjoint = TRUE, seed = 42L)
  expect_identical(.Random.seed, before)
  # determinism: same seed -> same folds
  f2 <- er_split(ids, truth = truth, k = 5L, entity_disjoint = TRUE, seed = 42L)
  expect_identical(lapply(f1, `[[`, "test_idx"), lapply(f2, `[[`, "test_idx"))
})

test_that("er_inject_missing preserves caller RNG and stays deterministic", {
  sim <- list(f1 = runif(200), f2 = runif(200))
  set.seed(123)
  before <- .Random.seed
  m1 <- er_inject_missing(sim, rho = 0.3, mechanism = "MCAR", seed = 42L)
  expect_identical(.Random.seed, before)
  m2 <- er_inject_missing(sim, rho = 0.3, mechanism = "MCAR", seed = 42L)
  expect_identical(which(is.na(m1$f1)), which(is.na(m2$f1)))
  expect_gt(sum(is.na(m1$f1)), 0)
})

test_that("er_with_seed restores RNG state and is deterministic", {
  set.seed(7)
  before <- .Random.seed
  v1 <- er_with_seed(42L, rnorm(5))
  expect_identical(.Random.seed, before)
  v2 <- er_with_seed(42L, rnorm(5))
  expect_identical(v1, v2)
})

# ── Modified R sources all parse (syntax guard) ───────────────────────────────
test_that("all Wave-1-modified files parse cleanly", {
  files <- c("R/00-utils.R", "R/05-similarity.R", "R/07-cluster.R",
             "R/09-evaluate.R", "R/12-cv_protocols.R", "R/18-multiplex.R",
             "experiments/run_exp1_main.R", "experiments/run_exp2_ablation_cora.R",
             "experiments/run_exp3_scalability.R",
             "experiments/run_exp4_missingness.R", "experiments/run_exp5_types.R")
  for (f in files)
    expect_no_error(parse(file.path(.erbot_root, f)))
})
