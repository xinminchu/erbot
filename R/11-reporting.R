#' Quick base plotting for a tuning curve
#' @export
er_plot_curve <- function(df, x_col, y_col, main="", xlab=NULL, ylab=NULL){
  if (is.null(df) || !all(c(x_col,y_col) %in% names(df))) return(invisible(FALSE))
  x <- df[[x_col]]; y <- df[[y_col]]; if (all(!is.finite(y))) return(invisible(FALSE))
  xlab <- xlab %||% x_col; ylab <- ylab %||% y_col
  plot(x,y,type="b",pch=16,main=main,xlab=xlab,ylab=ylab); abline(h=max(y[is.finite(y)]), lty=3, col="gray50")
  invisible(TRUE)
}

#' Pick GC metric name from tuning table
#' @keywords internal
#' @export
er_pick_gc_metric <- function(gc_curve, fallback_metric="adj_rand"){
  if (is.null(gc_curve)) return(NULL)
  if ("silhouette" %in% names(gc_curve)) return("silhouette")
  if (fallback_metric %in% names(gc_curve)) return(fallback_metric)
  for (m in c("adj_rand","rand","mutual_info","F_measure","jaccard")) if (m %in% names(gc_curve)) return(m)
  NULL
}

#' Autoplot tuning panels
#' @export
er_autoplot_tuning <- function(res, metric=NULL){
  metric <- metric %||% (res$details$params$tune_metric %||% "adj_rand")
  gc_metric <- er_pick_gc_metric(res$tuning$gc_threshold_curve, fallback_metric=metric)
  panels <- sum(!sapply(list(res$tuning$kmeans_sil_curve,res$tuning$hclust_sil_curve,res$tuning$pam_sil_curve,res$tuning$gc_threshold_curve), is.null))
  if (!panels) return(invisible(FALSE))
  nrow <- if (panels <= 2) 1 else 2; ncol <- ceiling(panels / nrow)
  old <- par(mfrow=c(nrow,ncol), mar=c(4,4,3,1)); on.exit(par(old), add=TRUE)
  if (!is.null(res$tuning$kmeans_sil_curve)) er_plot_curve(res$tuning$kmeans_sil_curve,"k","silhouette","KMeans: silhouette vs k")
  if (!is.null(res$tuning$hclust_sil_curve)) er_plot_curve(res$tuning$hclust_sil_curve,"k","silhouette","HC (Ward.D2): silhouette vs k")
  if (!is.null(res$tuning$pam_sil_curve))    er_plot_curve(res$tuning$pam_sil_curve,"k","silhouette","PAM: silhouette vs k")
  if (!is.null(res$tuning$gc_threshold_curve) && !is.null(gc_metric))
    er_plot_curve(res$tuning$gc_threshold_curve,"threshold",gc_metric,sprintf("GC: %s vs threshold",gc_metric),"threshold",gc_metric)
  invisible(TRUE)
}

#' Draw a small table on a graphics device
#' @export
er_draw_table <- function(df, title = NULL, base_size = 8, max_rows_text = 60) {
  dfc <- as.data.frame(lapply(df, function(col) {
    if (is.list(col)) {
      vapply(col, function(x) paste0(as.character(unlist(x)), collapse = ", "), character(1))
    } else if (inherits(col, "POSIXt")) {
      format(col)
    } else {
      as.character(col)
    }
  }), stringsAsFactors = FALSE, check.names = FALSE)

  if (requireNamespace("gridExtra", quietly = TRUE)) {
    grid::grid.newpage()
    if (!is.null(title)) grid::grid.text(title, y = 0.98, gp = grid::gpar(fontsize = base_size + 2, fontface = "bold"))
    tg <- gridExtra::tableGrob(dfc, rows = NULL, theme = gridExtra::ttheme_minimal(base_size = base_size))
    grid::pushViewport(grid::viewport(y = 0.94, height = 0.90, just = "top"))
    grid::grid.draw(tg)
    grid::popViewport()
  } else {
    plot.new()
    if (!is.null(title)) title(main = title, cex.main = 1)
    txt <- capture.output(print(utils::head(dfc, max_rows_text)))
    text(0, 1, paste(txt, collapse = "\n"), adj = c(0, 1), cex = 0.72, family = "mono")
  }
}

#' Parameters table for a pipeline result
#' @export
er_params_df <- function(res) {
  tun <- res$tuning
  par <- res$details$params
  vals <- list(
    tun$kmeans_k, tun$hclust_k, tun$pam_k, tun$gc_best_threshold,
    par$knn_k, par$svd_dim, par$louvain_min_sim, par$cos_thresh,
    par$sn_window, par$sn_method, par$sn_thresh,
    par$gc_method, par$gc_dist_method, par$auto_tune, par$tune_metric
  )
  fmt <- function(x) {
    if (length(x) > 1) paste(x, collapse = ", ")
    else if (is.logical(x)) ifelse(isTRUE(x), "TRUE", "FALSE")
    else if (is.numeric(x)) as.character(signif(x, 6))
    else as.character(x)
  }
  data.frame(
    Parameter = c(
      "kmeans_k","hclust_k","pam_k","gc_best_threshold",
      "knn_k","svd_dim","louvain_min_sim","cos_thresh",
      "sn_window","sn_method","sn_thresh",
      "gc_method","gc_dist_method","auto_tune","tune_metric"
    ),
    Value = vapply(vals, fmt, character(1)),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

#' Top GC thresholds table
#' @export
er_gc_top_table <- function(res, top_n=5, metric=NULL){
  gc_curve <- res$tuning$gc_threshold_curve; if (is.null(gc_curve)) return(NULL)
  metric <- er_pick_gc_metric(gc_curve, fallback_metric = (metric %||% res$details$params$tune_metric %||% "adj_rand"))
  ord <- order(gc_curve[[metric]], decreasing=TRUE)
  utils::head(gc_curve[ord, c("threshold", metric), drop=FALSE], top_n)
}


#' Save an ER PDF report (rounded metrics + runtime)
#'
#' Create a PDF report with overview info, runtime, and performance tables.
#' This version does not use R Markdown, only base graphics + grid.
#'
#' @param res List-like ER result object from \code{er_main()}.
#' @param file Character. Output PDF path.
#' @param dataset_name Character. Dataset label.
#' @param top_n Integer. Number of top items to show if available.
#' @param digits Integer. Decimal places for numeric columns (default 5).
#' @param runtime_sec Numeric or NULL. If provided, runtime in seconds will be printed.
#'
#' @return Invisibly returns the output file path.
#'
#' @examples
#' \dontrun{
#' er_save_report_pdf(res, "cora_report.pdf", dataset_name="CORA", top_n=5, digits=5)
#' }
#' @export
er_save_report_pdf <- function(res,
                               file,
                               dataset_name = "DATASET",
                               top_n        = 5,
                               digits       = 5,
                               runtime_sec  = NULL) {
  if (!requireNamespace("gridExtra", quietly = TRUE)) {
    stop("Package 'gridExtra' is required. Install with install.packages('gridExtra').")
  }
  if (!requireNamespace("grid", quietly = TRUE)) {
    stop("Package 'grid' is required. Install with install.packages('grid').")
  }

  # helper to round numeric cols
  round_num <- function(df, d) {
    if (!is.data.frame(df)) return(df)
    num_cols <- vapply(df, is.numeric, TRUE)
    if (any(num_cols)) df[num_cols] <- lapply(df[num_cols], round, d)
    df
  }

  # rounded copy for display
  res_disp <- res
  if (!is.null(res_disp$performance)) res_disp$performance <- round_num(res_disp$performance, digits)
  if (!is.null(res_disp$agreement))   res_disp$agreement   <- round_num(res_disp$agreement, digits)
  if (!is.null(res_disp$curves)) {
    res_disp$curves <- lapply(res_disp$curves, round_num, d = digits)
  }

  # prepare header text
  header_lines <- c(
    paste("Dataset:", dataset_name),
    paste("Generated at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  )
  if (!is.null(runtime_sec)) {
    header_lines <- c(header_lines, sprintf("Total runtime: %.2f sec", runtime_sec))
  }

  # create PDF
  grDevices::pdf(file, width = 8.5, height = 11)
  grid::grid.newpage()
  grid::grid.text(paste(header_lines, collapse = "\n"),
                  x = 0.05, y = 0.95, just = c("left","top"),
                  gp = grid::gpar(fontsize = 12, fontface = "bold"))

  y_pos <- 0.85

  # add performance table
  if (!is.null(res_disp$performance)) {
    grid::grid.text("Clustering Agreement — Performance", x=0.05, y=y_pos, just=c("left","top"),
                    gp=grid::gpar(fontsize=11, fontface="bold"))
    y_pos <- y_pos - 0.05
    gridExtra::grid.table(res_disp$performance, rows=NULL, theme=gridExtra::ttheme_default(), vp=grid::viewport(y=y_pos, height=0.2))
    y_pos <- y_pos - 0.25
  }

  # add agreement table
  if (!is.null(res_disp$agreement)) {
    grid::grid.text("Agreement Details", x=0.05, y=y_pos, just=c("left","top"),
                    gp=grid::gpar(fontsize=11, fontface="bold"))
    y_pos <- y_pos - 0.05
    gridExtra::grid.table(res_disp$agreement, rows=NULL, theme=gridExtra::ttheme_default(), vp=grid::viewport(y=y_pos, height=0.2))
    y_pos <- y_pos - 0.25
  }

  # add curves (first few rows of each)
  if (!is.null(res_disp$curves)) {
    for (nm in names(res_disp$curves)) {
      tb <- res_disp$curves[[nm]]
      if (is.data.frame(tb)) {
        grid::grid.text(paste("Method Curve:", nm), x=0.05, y=y_pos, just=c("left","top"),
                        gp=grid::gpar(fontsize=11, fontface="bold"))
        y_pos <- y_pos - 0.05
        gridExtra::grid.table(utils::head(tb, 10), rows=NULL, theme=gridExtra::ttheme_default(),
                              vp=grid::viewport(y=y_pos, height=0.2))
        y_pos <- y_pos - 0.25
      }
    }
  }

  # add top items if available
  if (!is.null(res_disp$top_items)) {
    grid::grid.text(sprintf("Top %d items", top_n), x=0.05, y=y_pos, just=c("left","top"),
                    gp=grid::gpar(fontsize=11, fontface="bold"))
    y_pos <- y_pos - 0.05
    gridExtra::grid.table(utils::head(res_disp$top_items, top_n), rows=NULL,
                          theme=gridExtra::ttheme_default(),
                          vp=grid::viewport(y=y_pos, height=0.2))
  }

  grDevices::dev.off()

  invisible(file)
}



#' Generate a timestamped filename
#'
#' Creates a filename with the current date-time as `YYYYMMDDHHMMSS`.
#' Optionally appends three random digits to reduce collision.
#'
#' @param prefix Character. Prefix for the file name (default: "cora_report").
#' @param ext Character. File extension (default: "pdf").
#' @param random Logical. Whether to append three random digits (default: FALSE).
#'
#' @return A character string with the generated filename.
#' @examples
#' make_timestamp_filename()
#' make_timestamp_filename("results", "csv")
#' make_timestamp_filename("report", "pdf", random = TRUE)
#'
#' @export
make_timestamp_filename <- function(prefix = "cora_report", ext = "pdf", random = FALSE) {
  ts <- format(Sys.time(), "%Y%m%d%H%M%S")
  if (random) {
    r3 <- sprintf("%03d", sample(0:999, 1))
    fname <- paste0(prefix, "_", ts, "_", r3, ".", ext)
  } else {
    fname <- paste0(prefix, "_", ts, ".", ext)
  }
  return(fname)
}

#' Write performance/agreement table to CSV/TXT (rounded)
#'
#' Saves a performance (or clustering agreement) data frame to `.csv` or `.txt`
#' with numeric columns rounded to a fixed number of decimal places.
#'
#' @param x A data.frame/tibble with performance or agreement metrics.
#' @param file Character path ending in .csv or .txt.
#' @param digits Integer number of decimal places to round numeric columns (default 5).
#' @param ... Passed to \code{utils::write.csv} or \code{utils::write.table}.
#'
#' @return (Invisibly) the rounded data.frame.
#' @examples
#' \dontrun{
#'   perf <- res$performance %||% res$agreement
#'   er_write_performance(perf, "results/perf_agreement.txt")
#'   er_write_performance(perf, "results/perf_agreement.csv", digits = 4)
#' }
#' @importFrom tools file_ext
#' @export
er_write_performance <- function(x, file, digits = 5, ...) {
  stopifnot(is.data.frame(x), is.character(file), length(file) == 1)
  round_num <- function(df, d) {
    num_cols <- vapply(df, is.numeric, TRUE)
    if (any(num_cols)) df[num_cols] <- lapply(df[num_cols], round, d)
    df
  }
  x_round <- round_num(x, digits)

  ext <- tolower(tools::file_ext(file))
  if (ext == "csv") {
    utils::write.csv(x_round, file = file, row.names = FALSE, ...)
  } else if (ext == "txt") {
    utils::write.table(x_round, file = file, sep = "\t", row.names = FALSE, quote = FALSE, ...)
  } else {
    stop("Unsupported file extension: ", ext, " (use .csv or .txt)")
  }
  invisible(x_round)
}
