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


#' Save an organized ER PDF report (no Rmd)
#'
#' Creates a multi-page PDF report with a cover, clean metric tables (rounded),
#' optional curves, and basic run metadata. No R Markdown is used.
#'
#' @param res List-like ER result object from \code{er_main()}.
#'   Tries to use: \code{res$performance}, \code{res$agreement}, \code{res$curves},
#'   \code{res$top_items}, and common metadata like \code{res$fields} or
#'   \code{res$params$fields}, \code{res$best}, etc. All are optional.
#' @param file Output PDF filepath.
#' @param dataset_name Label for the dataset.
#' @param top_n How many top items to show if available.
#' @param digits Decimal places for numeric printing.
#' @param runtime_sec Optional elapsed time in seconds to display on the cover.
#'
#' @return (Invisibly) the output path.
#' @export
er_save_report_pdf <- function(res,
                               file,
                               dataset_name = "DATASET",
                               top_n        = 5,
                               digits       = 5,
                               runtime_sec  = NULL) {
  if (!requireNamespace("gridExtra", quietly = TRUE))
    stop("Please install 'gridExtra'.")
  if (!requireNamespace("grid", quietly = TRUE))
    stop("Please install 'grid'.")
  has_gg <- requireNamespace("ggplot2", quietly = TRUE)

  # ---------- small utils ----------
  round_num <- function(df, d) {
    if (!is.data.frame(df)) return(df)
    num <- vapply(df, is.numeric, TRUE)
    if (any(num)) df[num] <- lapply(df[num], round, d)
    df
  }
  # Coerce any shape to a printable data.frame
  coerce_table <- function(x) {
    if (is.null(x)) return(NULL)
    if (is.data.frame(x)) return(round_num(x, digits))
    if (is.matrix(x))     return(round_num(as.data.frame(x, stringsAsFactors = FALSE), digits))
    # named vector/list -> Metric / Value
    v <- tryCatch(unlist(x), error = function(e) NULL)
    if (is.null(v)) return(NULL)
    out <- data.frame(Metric = names(v), Value = as.numeric(v), row.names = NULL, check.names = FALSE)
    out$Value <- round(out$Value, digits)
    out
  }
  paste_fields <- function(x) {
    if (is.null(x)) return(NA_character_)
    if (is.character(x)) return(paste(unique(x), collapse = ", "))
    if (is.list(x) && is.character(unlist(x))) return(paste(unique(unlist(x)), collapse = ", "))
    NA_character_
  }
  get_or <- function(x, default = NA) if (is.null(x)) default else x

  # Discover some metadata if available
  fields_used <- paste_fields(get_or(res$fields, get_or(res$params$fields, NULL)))
  n_records   <- get_or(res$n_records, get_or(NROW(get_or(res$data, NULL)), NA))
  # Try to infer #clusters from a factor/vector named 'cluster' or from best partition
  n_clusters  <- NA
  if (!is.null(res$clusters)) n_clusters <- length(unique(res$clusters))
  if (!is.null(res$best) && !is.null(res$best$clusters)) n_clusters <- length(unique(res$best$clusters))
  if (!is.null(res$labels)) n_clusters <- length(unique(res$labels))

  # Build clean tables
  perf_tbl <- coerce_table(res$performance)
  agr_tbl  <- coerce_table(res$agreement)

  # Curves: expect a list of data.frames under res$curves
  curves <- res$curves
  curve_plots <- list()
  if (!is.null(curves) && is.list(curves) && has_gg) {
    for (nm in names(curves)) {
      tb <- curves[[nm]]
      if (!is.data.frame(tb)) next
      tb <- round_num(tb, digits)
      # Try common x/y keys; fall back to first numeric pair
      xcol <- intersect(c("k","knn","min_sim","embed_k","gc_threshold","n_clusters","step"), names(tb))[1]
      ycands <- c("silhouette","modularity","ch","db","gap","ari","f1","precision","recall")
      ycol <- intersect(ycands, names(tb))[1]
      if (is.na(xcol) || is.na(ycol)) {
        # fallback: first two numeric columns
        num_cols <- names(tb)[vapply(tb, is.numeric, TRUE)]
        if (length(num_cols) >= 2) {
          xcol <- num_cols[1]; ycol <- num_cols[2]
        } else {
          next
        }
      }
      p <- ggplot2::ggplot(tb, ggplot2::aes_string(x = xcol, y = ycol)) +
        ggplot2::geom_line() + ggplot2::geom_point() +
        ggplot2::labs(title = nm, x = xcol, y = ycol)
      curve_plots[[nm]] <- p
    }
  }

  # Pagination helpers ---------------------------------------------------
  new_page <- function() {
    grid::grid.newpage()
  }
  title_grob <- function(text, y = 0.95, size = 14) {
    grid::grid.text(text, x = 0.05, y = y, just = c("left","top"),
                    gp = grid::gpar(fontsize = size, fontface = "bold"))
  }
  para_grob <- function(text, y, size = 11) {
    grid::grid.text(text, x = 0.05, y = y, just = c("left","top"),
                    gp = grid::gpar(fontsize = size))
  }
  draw_table <- function(df, y, height = 0.75) {
    tg <- gridExtra::tableGrob(df, rows = NULL)
    grid::pushViewport(grid::viewport(x = 0.5, y = y, width = 0.9, height = height))
    grid::grid.draw(tg)
    grid::popViewport()
  }
  # Split a data.frame into pages with max rows per page
  split_pages <- function(df, rows_per_page = 30) {
    n <- nrow(df)
    if (is.null(n) || n == 0) return(list(df))
    idx <- split(seq_len(n), ceiling(seq_len(n) / rows_per_page))
    lapply(idx, function(ii) df[ii, , drop = FALSE])
  }

  # Start PDF ------------------------------------------------------------
  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  grDevices::pdf(file, width = 8.5, height = 11)

  # Cover page
  new_page()
  title_grob(sprintf("%s — Entity Resolution Report", dataset_name))
  lines <- c(
    sprintf("Generated at: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    if (!is.na(fields_used)) sprintf("Fields used: %s", fields_used) else NULL,
    if (!is.na(n_records))   sprintf("Records: %s", format(n_records, big.mark = ",")) else NULL,
    if (!is.na(n_clusters))  sprintf("Clusters (inferred): %s", format(n_clusters, big.mark = ",")) else NULL,
    if (!is.null(runtime_sec)) sprintf("Total runtime: %.2f sec", runtime_sec) else NULL
  )
  para_grob(paste(lines, collapse = "\n"), y = 0.88)

  # Performance table (paginated)
  if (!is.null(perf_tbl)) {
    for (i in seq_along(split_pages(perf_tbl))) {
      pg <- split_pages(perf_tbl)[[i]]
      new_page()
      title_grob("Clustering Agreement — Performance", y = 0.96)
      if (length(split_pages(perf_tbl)) > 1)
        para_grob(sprintf("Page %d of %d", i, length(split_pages(perf_tbl))), y = 0.91, size = 9)
      draw_table(pg, y = 0.55, height = 0.8)
    }
  }

  # Agreement table (paginated)
  if (!is.null(agr_tbl)) {
    for (i in seq_along(split_pages(agr_tbl))) {
      pg <- split_pages(agr_tbl)[[i]]
      new_page()
      title_grob("Agreement Details", y = 0.96)
      if (length(split_pages(agr_tbl)) > 1)
        para_grob(sprintf("Page %d of %d", i, length(split_pages(agr_tbl))), y = 0.91, size = 9)
      draw_table(pg, y = 0.55, height = 0.8)
    }
  }

  # Curves (one plot per page)
  if (length(curve_plots)) {
    for (nm in names(curve_plots)) {
      new_page()
      title_grob("Method Curves", y = 0.96)
      grid::pushViewport(grid::viewport(x = 0.5, y = 0.5, width = 0.9, height = 0.8))
      print(curve_plots[[nm]])
      grid::popViewport()
    }
  }

  # Top items (if present)
  if (!is.null(res$top_items) && is.data.frame(res$top_items) && nrow(res$top_items) > 0) {
    new_page()
    title_grob(sprintf("Top %d items", top_n), y = 0.96)
    draw_table(utils::head(round_num(res$top_items, digits), top_n), y = 0.55, height = 0.8)
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
