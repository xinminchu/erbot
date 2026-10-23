#!/usr/bin/env Rscript
# Advisor-feedback Item 1 (2026-10-01): one-command scenario runner.
#
# Usage:
#   Rscript scripts/run_scenario.R --scenario smoke --data cora
#   Rscript scripts/run_scenario.R --scenario full --data data/musicbrainz20k.csv \
#       --truth data/musicbrainz20k_gold.csv --out-dir results/musicbrainz_smoke
#
# On failure the script prints the failing stage, the likely reason, and the
# next repair step (not just a stack trace), and exits with status 1.

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(name, default = NULL) {
  hit <- grep(paste0("^--", name, "="), args, value = TRUE)
  if (length(hit)) return(sub(paste0("^--", name, "="), "", hit[1]))
  idx <- match(paste0("--", name), args)
  if (!is.na(idx) && idx < length(args)) return(args[idx + 1L])
  default
}

scenario <- get_arg("scenario", "smoke")
data_arg <- get_arg("data", "cora")
truth_arg <- get_arg("truth", NULL)
out_dir <- get_arg("out-dir", NULL)
verbose <- !("--quiet" %in% args)

fail <- function(stage, reason, next_step, original = "") {
  cat(sprintf("\n[erbot] Stage '%s' failed.\nReason: %s\nNext step: %s\n",
              stage, reason, next_step))
  if (nzchar(original)) cat(sprintf("Original error: %s\n", original))
  quit(save = "no", status = 1L)
}

# Load erbot: installed package first, otherwise the source tree in this repo.
if (requireNamespace("erbot", quietly = TRUE)) {
  suppressPackageStartupMessages(library(erbot))
} else if (requireNamespace("pkgload", quietly = TRUE) && file.exists("DESCRIPTION")) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  fail(
    "load erbot",
    "The erbot package is not installed and pkgload is not available to load the source tree.",
    "In VS Code, open the erbot-work folder, install the package (or install pkgload), then re-run this command from the repository root.",
    "package 'erbot' not available"
  )
}

cat(sprintf("[erbot] scenario=%s data=%s\n", scenario, data_arg))

result <- tryCatch(
  er_run_scenario(
    scenario = scenario,
    data = data_arg,
    truth = truth_arg,
    out_dir = out_dir,
    verbose = verbose
  ),
  erbot_scenario_error = function(e) {
    fail(e$stage, e$reason, e$next_step, e$original)
  },
  error = function(e) {
    ex <- er_explain_error(e)
    fail(ex$stage, ex$reason, ex$next_step, ex$original)
  }
)

n_clusters <- length(unique(result$clusters))
cat(sprintf("\n[erbot] Done. scenario=%s, %d records, %d clusters.\n",
            scenario, nrow(result$predictions), n_clusters))
if (!is.null(result$performance)) {
  cat("[erbot] Performance (primary metrics):\n")
  show_cols <- intersect(
    c("method", "ARI", "Bcubed_F", "Homogeneity", "Completeness"),
    names(result$performance)
  )
  print(result$performance[, show_cols, drop = FALSE], n = Inf)
}
if (!is.null(out_dir)) {
  cat(sprintf("[erbot] Outputs written to %s\n", out_dir))
}
