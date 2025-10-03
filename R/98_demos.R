########################################

#' Run CORA entity resolution demo
#' @export
run_cora <- function(fields_cora,
                     out_dir,
                     file_suffix   = as.numeric(Sys.time()),
                     save_perf_file = NULL,
                     perf_source    = c("auto","performance","agreement"),
                     digits         = 5) {
  perf_source <- match.arg(perf_source)
  message("Running CORA demo...")
  dir.create(file.path(out_dir, "data"),    recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(out_dir, "results"), recursive = TRUE, showWarnings = FALSE)

  if (!requireNamespace("cora", quietly = TRUE)) {
    install.packages("cora", repos = "https://cloud.r-project.org")
  }
  suppressPackageStartupMessages(library(cora))

  t0 <- Sys.time()
  res <- er_main(
    data      = "cora",
    truth     = cora_gold,
    fields    = fields_cora,
    k_grid    = seq(10, 500, by = 10),
    write_csv = file.path(out_dir, "data", paste0("cora_clean_pred_", file_suffix, ".csv"))
  )
  t1 <- Sys.time()
  runtime_sec <- as.numeric(difftime(t1, t0, units = "secs"))

  if (!is.null(save_perf_file)) {
    tbl <- if (perf_source == "performance") res$performance else if (perf_source == "agreement") res$agreement else if (!is.null(res$performance)) res$performance else res$agreement
    if (!is.null(tbl)) er_write_performance(tbl, save_perf_file, digits = digits) else warning("No perf/agreement table to save.")
  }

  er_save_report_pdf(
    res,
    file         = file.path(out_dir, "results", paste0("cora_report_", file_suffix, ".pdf")),
    dataset_name = "CORA",
    top_n        = 5,
    digits       = digits,
    runtime_sec  = runtime_sec
  )
  invisible(res)
}

#' Run Affiliation entity resolution demo
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
  dir.create(file.path(out_dir, "data"),    recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(out_dir, "results"), recursive = TRUE, showWarnings = FALSE)

  t0 <- Sys.time()
  res <- er_main(
    data      = data_path,
    truth     = truth_path,
    fields    = fields_affil,
    k_grid    = seq(10, 500, by = 10),
    write_csv = file.path(out_dir, "data", paste0("affiliation_pred_", file_suffix, ".csv"))
  )
  t1 <- Sys.time()
  runtime_sec <- as.numeric(difftime(t1, t0, units = "secs"))

  if (!is.null(save_perf_file)) {
    tbl <- switch(perf_source,
                  performance = res$performance,
                  agreement   = res$agreement,
                  auto        = if (!is.null(res$performance)) res$performance else res$agreement)
    if (!is.null(tbl)) er_write_performance(tbl, save_perf_file, digits = digits) else warning("No perf/agreement table to save.")
  }

  er_save_report_pdf(
    res,
    file         = file.path(out_dir, "results", paste0("affiliation_report_", file_suffix, ".pdf")),
    dataset_name = "Affiliation",
    top_n        = top_n,
    digits       = digits,
    runtime_sec  = runtime_sec
  )
  invisible(res)
}

