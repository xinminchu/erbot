#' Run CORA entity resolution demo
#'
#' Executes the ER pipeline on the CORA dataset with user-specified fields.
#' Produces a cleaned prediction CSV and a PDF report. Optionally writes a
#' rounded “Clustering Agreement — Performance” table to `.csv` or `.txt`.
#'
#' @param fields_cora Character vector. The field names from the CORA dataset
#'   to use for entity resolution (e.g., `c("title", "authors", "address")`).
#' @param out_dir Character. Path to the output directory where results will
#'   be written. Subdirectories `"data"` and `"results"` will be created if needed.
#' @param file_suffix Character or numeric. Suffix appended to output filenames
#'   (default: current Unix timestamp via `as.numeric(Sys.time())`).
#' @param save_perf_file Optional character path to save the performance/agreement
#'   table (must end with `.csv` or `.txt`). Numeric columns are rounded to 5 d.p.
#' @param perf_source Character scalar; which table to save if available:
#'   one of `c("performance","agreement","auto")`. With `"auto"`, the function
#'   prefers `res$performance`, then falls back to `res$agreement`. Default: `"auto"`.
#' @param digits Integer; number of decimals for metrics displayed in the PDF header/tables (default 5).
#'
#' @details
#' The function will:
#' \itemize{
#'   \item Load CORA data and gold labels.
#'   \item Run \code{er_main()} with \code{k_grid = 10:500 by 10}.
#'   \item Save predictions to \code{data/cora_clean_pred_<suffix>.csv}.
#'   \item Render a PDF report to \code{results/cora_report_<suffix>.pdf} and include
#'         total runtime in seconds.
#'   \item Optionally write a rounded performance/agreement table to \code{save_perf_file}.
#' }
#'
#' @return Invisibly returns the result object from \code{er_main()}.
#'
#' @examples
#' \dontrun{
#'   run_cora(
#'     fields_cora   = c("title","authors","address"),
#'     out_dir       = "~/Desktop/erbot_outputs",
#'     save_perf_file= "~/Desktop/erbot_outputs/results/perf_agreement.csv"
#'   )
#' }
#'
#' @seealso \code{\link{er_write_performance}}, \code{\link{make_timestamp_filename}}
#' @export
run_cora <- function(fields_cora,
                     out_dir,
                     file_suffix   = as.numeric(Sys.time()),
                     save_perf_file = NULL,
                     perf_source    = c("auto","performance","agreement"),
                     digits         = 5) {
  perf_source <- match.arg(perf_source)

  message("Running CORA demo...")

  # Ensure output folders exist
  dir.create(file.path(out_dir, "data"),    recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(out_dir, "results"), recursive = TRUE, showWarnings = FALSE)

  # Load CORA
  if (!requireNamespace("cora", quietly = TRUE)) {
    install.packages("cora", repos = "https://cloud.r-project.org")
  }
  suppressPackageStartupMessages(library(cora))

  # Run pipeline with timing
  t0 <- Sys.time()
  res <- er_main(
    data      = "cora",   # your erbot loader should recognize this
    truth     = cora_gold,
    fields    = fields_cora,
    k_grid    = seq(10, 500, by = 10),
    write_csv = file.path(out_dir, "data", paste0("cora_clean_pred_", file_suffix, ".csv"))
  )
  t1 <- Sys.time()
  runtime_sec <- as.numeric(difftime(t1, t0, units = "secs"))

  # Optionally save performance/agreement table (rounded)
  if (!is.null(save_perf_file)) {
    tbl <- NULL
    if (perf_source == "performance") {
      tbl <- res$performance
    } else if (perf_source == "agreement") {
      tbl <- res$agreement
    } else { # auto
      tbl <- if (!is.null(res$performance)) res$performance else res$agreement
    }
    if (is.null(tbl)) {
      warning("No performance/agreement table found in `res`; nothing saved to `save_perf_file`.")
    } else {
      er_write_performance(tbl, save_perf_file, digits = digits)
    }
  }

  # Save PDF report (assumes your er_save_report_pdf supports `digits` and `runtime_sec`)
  er_save_report_pdf(
    res,
    file         = file.path(out_dir, "results", paste0("cora_report_", file_suffix, ".pdf")),
    dataset_name = "CORA",
    top_n        = 5,
    digits       = digits,        # ensure 5 d.p. in the report
    runtime_sec  = runtime_sec    # include runtime in the PDF
  )

  invisible(res)
}



#' Run Affiliation entity resolution demo
#'
#' Executes the ER pipeline on an Affiliation dataset stored in CSV files.
#' Produces a cleaned prediction CSV and a PDF report. Optionally writes a
#' rounded “Clustering Agreement — Performance” table to `.csv` or `.txt`.
#'
#' @param data_path Character. Path to the input CSV with raw records
#'   (e.g., IDs and text fields like \code{affil1}).
#' @param truth_path Character or \code{NULL}. Path to the gold labels CSV
#'   (either id + cluster labels, or a pair-list), used for evaluation.
#'   Set \code{NULL} if no truth is available.
#' @param fields_affil Character vector. Field names to use for ER
#'   (default: \code{"affil1"}). If you’ve parsed/normalized more fields,
#'   pass them here, e.g. \code{c("affil1","affil2")}.
#' @param out_dir Character. Output directory; subfolders \code{"data"} and
#'   \code{"results"} will be created if needed.
#' @param file_suffix Character or numeric. Suffix appended to output filenames
#'   default: \code{as.numeric(Sys.time())}. You can also pass a timestamp
#'   string like \code{format(Sys.time(), "\%Y\%m\%d\%H\%M\%S")}.
#' @param save_perf_file Optional character path to save the performance/agreement
#'   table (must end with \code{.csv} or \code{.txt}). Numeric columns are rounded.
#' @param perf_source One of \code{c("auto","performance","agreement")}. With
#'   \code{"auto"}, prefers \code{res$performance} then falls back to \code{res$agreement}.
#' @param digits Integer. Number of decimals for metrics displayed/saved (default 5).
#' @param top_n Integer. Number of “top items/blocks/etc.” to show in the report (default 5).
#'
#' @return Invisibly returns the result object from \code{er_main()}.
#'
#' @examples
#' \dontrun{
#'   run_affiliation(
#'     data_path    = "D:/erbot/data/affiliationstrings_ids.csv",
#'     truth_path   = "D:/erbot/data/affiliationstrings_mapping.csv",
#'     fields_affil = "affil1",
#'     out_dir      = "D:/erbot/outputs",
#'     save_perf_file = "D:/erbot/outputs/results/affil_perf.csv"
#'   )
#' }
#'
#' @seealso \code{\link{er_write_performance}}, \code{\link{er_save_report_pdf}}
#' @export
run_affiliation <- function(data_path,
                            truth_path    = NULL,
                            fields_affil  = "affil1",
                            out_dir,
                            file_suffix   = as.numeric(Sys.time()),
                            save_perf_file = NULL,
                            perf_source    = c("auto","performance","agreement"),
                            digits         = 5,
                            top_n          = 5) {
  perf_source <- match.arg(perf_source)

  message("Running Affiliation demo...")

  # Ensure output folders exist
  dir.create(file.path(out_dir, "data"),    recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(out_dir, "results"), recursive = TRUE, showWarnings = FALSE)

  # Time the pipeline
  t0 <- Sys.time()
  res <- er_main(
    data      = data_path,          # <- your er_main should read from CSV path
    truth     = truth_path,         # <- or NULL
    fields    = fields_affil,
    k_grid    = seq(10, 500, by = 10),
    write_csv = file.path(out_dir, "data",
                          paste0("affiliation_pred_", file_suffix, ".csv"))
  )
  t1 <- Sys.time()
  runtime_sec <- as.numeric(difftime(t1, t0, units = "secs"))

  # Optionally save performance/agreement table (rounded)
  if (!is.null(save_perf_file)) {
    tbl <- switch(perf_source,
                  performance = res$performance,
                  agreement   = res$agreement,
                  auto        = if (!is.null(res$performance)) res$performance else res$agreement)
    if (is.null(tbl)) {
      warning("No performance/agreement table found in `res`; nothing saved to `save_perf_file`.")
    } else {
      er_write_performance(tbl, save_perf_file, digits = digits)
    }
  }

  # PDF report (no Rmd version or Rmd version—whichever you implemented)
  er_save_report_pdf(
    res,
    file         = file.path(out_dir, "results",
                             paste0("affiliation_report_", file_suffix, ".pdf")),
    dataset_name = "Affiliation",
    top_n        = top_n,
    digits       = digits,
    runtime_sec  = runtime_sec
  )

  invisible(res)
}
