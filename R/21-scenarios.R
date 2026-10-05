########################################
# File: R/21-scenarios.R
# Scenario-based entry points.
#
# Goal: anyone can ask for one named situation ("run the smoke scenario on
# CORA", "run the full pipeline on a new dataset") and either get the result
# or get an error that states the failing stage, the likely reason, and the
# next repair step -- not just a stack trace.
#
# This file does NOT replace er_run(). It wraps it with:
#   * er_scenario_presets()  -- named, documented presets
#   * er_run_scenario()      -- one call: preset + data (+ overrides)
#   * er_explain_error()     -- stage / reason / next_step for common failures
#
# A preset name fully determines the pipeline configuration, so "the smoke
# run on CORA" means the same thing to everyone; see er_scenario_presets().
########################################

#' List the named ERBOT scenario presets
#'
#' Each preset is a small, documented bundle of \code{er_run()} arguments.
#' The point is reproducibility and speed of discussion: a preset name fully
#' determines the pipeline configuration, so "the smoke run on CORA" means the
#' same thing to everyone.
#'
#' @return A data.frame with one row per preset: \code{scenario},
#'   \code{purpose}, \code{cluster_methods}, \code{weights}, \code{merge},
#'   and \code{recommended_use}.
#' @export
er_scenario_presets <- function() {
  data.frame(
    scenario = c("smoke", "screening", "full", "unsupervised_quick"),
    purpose = c(
      "Verify the pipeline runs end-to-end on a dataset and produces clusters + metrics.",
      "Broad first pass over clustering methods (closest erbotv2 analogue of a Scenario 1 screen).",
      "Full pipeline with consensus merging; the preset to use for a reportable run.",
      "Fast unsupervised pass when no ground truth is available."
    ),
    cluster_methods = c(
      "threshold_cc,louvain",
      "all",
      "all",
      "threshold_cc,louvain,leiden"
    ),
    weights = c("equal", "auto", "auto", "equal"),
    merge = c("consensus", "consensus", "consensus", "consensus"),
    recommended_use = c(
      "First run on any new dataset; should finish in seconds to a few minutes.",
      "Exploring which methods are admissible before a full run.",
      "Numbers intended for a report or a meeting.",
      "New dataset without labels, or a quick structural look."
    ),
    stringsAsFactors = FALSE
  )
}

#' @rdname er_scenario_presets
#' @export
er_list_scenarios <- er_scenario_presets

.er_preset_args <- function(scenario) {
  scenario <- match.arg(
    scenario,
    c("smoke", "screening", "full", "unsupervised_quick")
  )
  base <- list(
    mode = "auto",
    block = "auto",
    similarity = "auto",
    eval_mode = "labeled_only",
    threshold = 0.5,
    consensus_alpha = 0.5
  )
  extra <- switch(
    scenario,
    smoke = list(
      weights = "equal",
      cluster_methods = c("threshold_cc", "louvain"),
      merge = "consensus"
    ),
    screening = list(
      weights = "auto",
      cluster_methods = "all",
      merge = "consensus"
    ),
    full = list(
      weights = "auto",
      cluster_methods = "all",
      merge = "consensus"
    ),
    unsupervised_quick = list(
      weights = "equal",
      cluster_methods = c("threshold_cc", "louvain", "leiden"),
      merge = "consensus"
    )
  )
  utils::modifyList(base, extra)
}

#' Explain an ERBOT failure in plain, actionable form
#'
#' Converts an error into a structured explanation: which stage failed, the
#' likely reason, and the next repair step. Used by \code{er_run_scenario()},
#' but also useful interactively after any failed pipeline call.
#'
#' @param e An error condition (or an error message string).
#' @param stage Optional stage hint. If \code{NULL}, the stage is inferred
#'   from the message where possible.
#' @return A list with \code{stage}, \code{reason}, \code{next_step}, and
#'   \code{original} (the raw error message).
#' @export
er_explain_error <- function(e, stage = NULL) {
  msg <- if (inherits(e, "condition")) conditionMessage(e) else as.character(e)[1]
  low <- tolower(msg)

  infer_stage <- function() {
    if (grepl("not found|no such file|cannot open|does not exist", low)) return("load")
    if (grepl("block_key|pair budget|candidate pairs", low)) return("block")
    if (grepl("no columns to analyse|similarity|field", low)) return("diagnose/similarity")
    if (grepl("cluster|igraph|louvain|leiden", low)) return("cluster")
    if (grepl("quota|infeasible|positive", low)) return("split design")
    "pipeline"
  }
  if (is.null(stage)) stage <- infer_stage()

  reason <- "The pipeline stopped before producing a result."
  next_step <- "Read the original error below, fix that specific input, and re-run the same scenario (do not change the seed or the data to make the error go away)."

  if (grepl("not found|no such file|cannot open|does not exist", low)) {
    reason <- "A required file or built-in dataset could not be found."
    next_step <- "Check the path, or build the dataset first (for example er_build_restaurant(), er_build_dblp_acm()), then re-run the same command."
  } else if (grepl("no package called|not installed|namespace .* not available|there is no package", low)) {
    reason <- "An R package that this stage needs is not installed in this R environment."
    next_step <- "Install the named package in the same R environment that VS Code / Rscript is using, restart R, and re-run. Confirm with quarto check / Rscript -e 'packageVersion(\"<pkg>\")' if the wrong R is being picked up."
  } else if (grepl("label|leakage|ground-truth|bug-22", low)) {
    reason <- "A ground-truth / label column was about to enter the similarity features, which would leak the answer into the model."
    next_step <- "Keep the label column as truth only. Remove it from text_cols / similarity features (er_diagnose() already drops known label columns); do not rename it to sneak it back in."
  } else if (grepl("no columns to analyse|no similarity|0 field|no fields", low)) {
    reason <- "After removing ID/label columns, no usable similarity fields remained."
    next_step <- "Inspect er_diagnose(data): check id_col and text_cols, and confirm the dataset actually carries comparable attributes rather than only an ID and a label."
  } else if (grepl("pair budget|max_pairs|too many pairs|candidate pairs", low)) {
    reason <- "Blocking generated more candidate pairs than the configured budget allows."
    next_step <- "Use a blocking key (block = list(method = 'prefix' or 'standard', key = <column>)), reduce the dataset for a smoke run, or raise max_pairs deliberately and say so in the report."
  } else if (grepl("all sweep cells returned na|all cells.*na", low)) {
    reason <- "Every model cell in the sweep failed or returned NA, so there is no admissible model to select."
    next_step <- "Check admissibility first (design-matrix width vs positive-pair count; see the mechanism cards), then re-run. Do not treat an empty sweep as a model ranking."
  } else if (grepl("quota|infeasible", low)) {
    reason <- "The requested split quotas cannot be met because entities are indivisible: moving one whole entity would overshoot a partition quota."
    next_step <- "Relax the quota, change the pre-registered fractions, or accept the achieved counts and report them. Never redraw the seed until the numbers look nicer."
  } else if (grepl("must be a data.frame|must be a data frame|'data' must be", low)) {
    reason <- "The data argument is not a data.frame, a file path, or a known benchmark keyword."
    next_step <- "Pass a data.frame, an existing file path, or one of the supported keywords (see ?er_load), then re-run."
  }

  list(
    stage = stage,
    reason = reason,
    next_step = next_step,
    original = msg
  )
}

.er_stop_scenario <- function(scenario, explanation) {
  cond <- structure(
    list(
      message = sprintf(
        "[erbot scenario '%s'] Stage '%s' failed.\nReason: %s\nNext step: %s\nOriginal error: %s",
        scenario, explanation$stage, explanation$reason,
        explanation$next_step, explanation$original
      ),
      call = NULL,
      scenario = scenario,
      stage = explanation$stage,
      reason = explanation$reason,
      next_step = explanation$next_step,
      original = explanation$original
    ),
    class = c("erbot_scenario_error", "error", "condition")
  )
  stop(cond)
}

#' Run one named ERBOT scenario
#'
#' Resolves a preset from \code{\link{er_scenario_presets}}, applies any
#' \code{overrides}, and calls \code{\link{er_run}}. On failure, the error is
#' re-raised as an \code{erbot_scenario_error} whose message contains the
#' failing stage, the likely reason, and the next repair step.
#'
#' @param scenario One of \code{"smoke"}, \code{"screening"}, \code{"full"},
#'   \code{"unsupervised_quick"}.
#' @param data Passed to \code{\link{er_run}}: a data.frame, file path, or
#'   benchmark keyword.
#' @param truth Optional ground truth passed to \code{er_run()}.
#' @param out_dir Optional output directory passed to \code{er_run()}.
#' @param verbose Logical. Print stage-level progress.
#' @param overrides Named list of \code{er_run()} arguments that replace the
#'   preset values (for example \code{list(threshold = 0.7)}).
#'
#' @return An \code{er_result} list, with the scenario name attached as
#'   \code{result$scenario}.
#' @export
er_run_scenario <- function(scenario = "smoke",
                            data,
                            truth = NULL,
                            out_dir = NULL,
                            verbose = TRUE,
                            overrides = list()) {
  scenario <- match.arg(
    scenario,
    c("smoke", "screening", "full", "unsupervised_quick")
  )

  # Input validation with plain-language failures (reason + fix).
  if (is.character(data) && length(data) == 1L) {
    keywords <- c("cora", "affiliation", "d10k", "restaurant", "dblp_acm")
    if (!tolower(data) %in% keywords && !file.exists(data)) {
      .er_stop_scenario(scenario, list(
        stage = "validate inputs",
        reason = sprintf("'%s' is neither a known benchmark keyword nor an existing file.", data),
        next_step = sprintf("Use one of: %s, or pass a data.frame / an existing file path.",
                            paste(keywords, collapse = ", ")),
        original = sprintf("data file not found: %s", data)
      ))
    }
  }

  args <- .er_preset_args(scenario)
  if (length(overrides)) args <- utils::modifyList(args, overrides)
  args$data <- data
  args$truth <- truth
  args$out_dir <- out_dir
  args$verbose <- verbose

  result <- tryCatch(
    do.call(er_run, args),
    error = function(e) {
      .er_stop_scenario(scenario, er_explain_error(e))
    }
  )
  result$scenario <- scenario
  result
}
