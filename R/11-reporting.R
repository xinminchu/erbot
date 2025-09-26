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

#' Save a PDF report for a pipeline result
#' @param res result list returned by [er_unified_pipeline]
#' @param file output pdf path
#' @param dataset_name character
#' @export
er_save_report_pdf <- function(res, file="er_report.pdf", dataset_name=NULL, top_n=5, metric=NULL, width=11, height=8.5){
  grDevices::pdf(file=file, width=width, height=height, onefile=TRUE)
  plot.new()
  ttl <- if (is.null(dataset_name)) "Entity Resolution Report" else paste("Entity Resolution Report —", dataset_name)
  title(main=ttl, cex.main=1.4, font.main=2)
  mtext(paste("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M:%S")), side=3, line=0.5, cex=0.8, adj=1)
  text(0,0.85,paste0("Records: ", res$details$n), adj=c(0,0.5), cex=1)
  text(0,0.80,paste0("Embeddings detected: ", ifelse(res$details$has_embeddings,"yes","no")), adj=c(0,0.5), cex=1)
  text(0,0.74,"Methods included:", adj=c(0,0.5), cex=1)
  meths <- grep("^pred_", names(res$predictions), value=TRUE)
  text(0.02,0.68,paste("•", sub("^pred_","",meths), collapse="\n• "), adj=c(0,1), cex=0.9)

  er_draw_table(er_params_df(res), title="Selected Parameters")
  er_autoplot_tuning(res)

  gc_curve <- res$tuning$gc_threshold_curve
  if (!is.null(gc_curve)) {
    plot.new(); par(new=TRUE)
    metric_use <- er_pick_gc_metric(gc_curve, fallback_metric = (metric %||% res$details$params$tune_metric %||% "adj_rand"))
    er_plot_curve(gc_curve,"threshold",metric_use, sprintf("GC tuning — %s vs threshold", metric_use), "threshold", metric_use)
    top_gc <- er_gc_top_table(res, top_n=top_n, metric=metric)
    if (!is.null(top_gc)) er_draw_table(top_gc, title=sprintf("GC tuning (top %d by %s)", top_n, names(top_gc)[2]))
  }

  if (!is.null(res$performance) && nrow(res$performance)) {
    perf <- res$performance
    cols_order <- unique(c("Method","adj_rand","rand","jaccard","F_measure","fowlkes_mallow",
                           "tpr","fpr","mutual_info.MI","mutual_info.G","mutual_info.FJ","mutual_info.VI",
                           setdiff(names(perf), c("Method","adj_rand","rand","jaccard","F_measure","fowlkes_mallow","tpr","fpr",
                                                  "mutual_info.MI","mutual_info.SG","mutual_info.FJ","mutual_info.VI"))))
    perf <- perf[, intersect(cols_order, names(perf)), drop=FALSE]
    chunk_size <- 8
    col_blocks <- split(seq_along(perf), ceiling(seq_along(perf)/chunk_size))
    for (blk in col_blocks) er_draw_table(perf[, blk, drop=FALSE], title="Clustering Agreement — Performance")
  }
  grDevices::dev.off(); invisible(file)
}
