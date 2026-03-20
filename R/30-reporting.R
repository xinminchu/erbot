########################################
# File: D:/erbot/R/30-reporting.R
########################################

#' Quick base-graphics plot for a tuning curve
#'
#' @param df A `data.frame` or `tibble` containing the curve data.
#' @param x_col Character. Name of the column to use as the x-axis.
#' @param y_col Character. Name of the column to use as the y-axis.
#' @param main Character. Plot title. Default `""`.
#' @param xlab Character. x-axis label. Defaults to `x_col`.
#' @param ylab Character. y-axis label. Defaults to `y_col`.
#' @return `TRUE` invisibly on success; `FALSE` invisibly if the data are
#'   unusable (missing columns or all non-finite y values).
#' @export
er_plot_curve <- function(df, x_col, y_col, main="", xlab=NULL, ylab=NULL){
  if (is.null(df) || !all(c(x_col,y_col) %in% names(df))) return(invisible(FALSE))
  x <- df[[x_col]]; y <- df[[y_col]]; if (all(!is.finite(y))) return(invisible(FALSE))
  xlab <- xlab %||% x_col; ylab <- ylab %||% y_col
  plot(x,y,type="b",pch=16,main=main,xlab=xlab,ylab=ylab); abline(h=max(y[is.finite(y)]), lty=3, col="gray50")
  invisible(TRUE)
}

#' Pick GC metric name from tuning table
#'
#' Returns the name of the best metric column to use from a graph-coloring
#' threshold-sweep table. Prefers `"silhouette"` when present (unsupervised),
#' otherwise tries `fallback_metric` and then a fixed priority list.
#'
#' @param gc_curve A `data.frame`/`tibble` of tuning results with one column
#'   per metric, as returned by [er_gc_from_text()].
#' @param fallback_metric Character. Metric name to try if `"silhouette"` is
#'   absent. Default `"adj_rand"`.
#' @return A single character metric name, or `NULL` if none found.
#' @export
er_pick_gc_metric <- function(gc_curve, fallback_metric="adj_rand"){
  if (is.null(gc_curve)) return(NULL)
  if ("silhouette" %in% names(gc_curve)) return("silhouette")
  if (fallback_metric %in% names(gc_curve)) return(fallback_metric)
  for (m in c("adj_rand","rand","mutual_info","F_measure","jaccard")) if (m %in% names(gc_curve)) return(m)
  NULL
}

#' Autoplot tuning panels
#'
#' Draws up to four base-graphics panels (k-means silhouette, HC silhouette,
#' PAM silhouette, GC threshold curve) from an `er_unified_pipeline()` result.
#'
#' @param res List returned by [er_unified_pipeline()] or [er_main()],
#'   containing a `$tuning` sub-list.
#' @param metric Character. Metric to display for the GC panel. If `NULL`
#'   (default), auto-selected via [er_pick_gc_metric()].
#' @return `TRUE` invisibly if at least one panel was drawn; `FALSE` invisibly
#'   if no tuning data are available.
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
#'
#' Renders a `data.frame` as a grid table using **gridExtra** when available,
#' or falls back to plain text drawn with [graphics::text()].
#'
#' @param df A `data.frame` to render.
#' @param title Optional character title drawn above the table.
#' @param base_size Numeric. Base font size in points. Default `8`.
#' @param max_rows_text Integer. Maximum rows printed in the plain-text
#'   fallback. Default `60`.
#' @return Called for its side effect (drawing on the current device).
#'   Returns `NULL` invisibly.
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
#'
#' Extracts tuning-selected and user-supplied parameters from an
#' [er_unified_pipeline()] result and formats them as a two-column
#' `data.frame` suitable for printing or inclusion in a PDF report.
#'
#' @param res List returned by [er_unified_pipeline()] or [er_main()].
#' @return A `data.frame` with columns `Parameter` and `Value` (character).
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
#'
#' Returns the top-`n` rows from the graph-coloring threshold-sweep tuning
#' table, ordered by the best available metric.
#'
#' @param res List returned by [er_unified_pipeline()] or [er_main()],
#'   containing `$tuning$gc_threshold_curve`.
#' @param top_n Integer. Number of rows to return. Default `5`.
#' @param metric Character. Metric column to sort by. If `NULL` (default),
#'   auto-selected via [er_pick_gc_metric()].
#' @return A `data.frame` with columns `threshold` and the selected metric,
#'   or `NULL` if no GC tuning data are present.
#' @export
er_gc_top_table <- function(res, top_n=5, metric=NULL){
  gc_curve <- res$tuning$gc_threshold_curve; if (is.null(gc_curve)) return(NULL)
  metric <- er_pick_gc_metric(gc_curve, fallback_metric = (metric %||% res$details$params$tune_metric %||% "adj_rand"))
  ord <- order(gc_curve[[metric]], decreasing=TRUE)
  utils::head(gc_curve[ord, c("threshold", metric), drop=FALSE], top_n)
}


#' Save an organized ER PDF report (backward-compatible, no Rmd)
#'
#' Combines legacy `er_main()` report pieces (params, performance, agreement, top items)
#' with new `er_tune()` benchmarking pages (best-per-method, Top-N, tuning curves,
#' runtime & peak memory). Automatically adapts to the structure it finds in `res`.
#'
#' @param res ER result from `er_main()` (legacy) or `er_tune()` (new). If `res` is
#'   a list with `$curves`, it will render tuning/benchmark pages; otherwise it renders
#'   legacy tables and any provided curve data.frames in `res$curves` (list).
#' @param file Output PDF path.
#' @param dataset_name Title label.
#' @param objective Scoring column to highlight best-per-method when `res$curves`
#'   is present. One of: "silhouette","silhouette_penalized","ch","db_min",
#'   "modularity","stability","ari","pairwise_f1". Default "silhouette_penalized".
#' @param top_n Top rows for the Top-N page (if tuning) and for legacy `res$top_items`.
#' @param digits Rounding for numbers.
#' @param runtime_sec Optional total runtime in seconds (overrides values inside `res`).
#' @return (invisible) the output file path.
#' @examples
#' \dontrun{
#' tuned <- er_tune(data=cora, fields=c("title","authors","address"),
#'                  methods=c("kmeans","louvain","leiden"))
#' er_save_report_pdf(tuned, "results/cora_report.pdf", "CORA",
#'                    objective="silhouette_penalized", top_n=5)
#' }
#' @import ggplot2
#' @import gridExtra
#' @import dplyr
#' @export
er_save_report_pdf <- function(res,
                               file,
                               dataset_name = "DATASET",
                               objective    = "silhouette_penalized",
                               top_n        = 5,
                               digits       = 5,
                               runtime_sec  = NULL) {

  # ---- deps (fail clearly) ----
  if (!requireNamespace("gridExtra", quietly = TRUE)) stop("Please install 'gridExtra'.")
  if (!requireNamespace("grid", quietly = TRUE))      stop("Please install 'grid'.")
  has_gg <- requireNamespace("ggplot2", quietly = TRUE)
  has_dplyr <- requireNamespace("dplyr", quietly = TRUE)

  # ---- tiny helpers ----
  get_or <- function(x, default = NULL) if (is.null(x)) default else x
  round_num <- function(df, d) {
    if (!is.data.frame(df)) return(df)
    num <- vapply(df, is.numeric, TRUE)
    if (any(num)) df[num] <- lapply(df[num], round, d)
    df
  }
  paste_fields <- function(x) {
    if (is.null(x)) return(NA_character_)
    if (is.character(x)) return(paste(unique(x), collapse = ", "))
    if (is.list(x) && is.character(unlist(x))) return(paste(unique(unlist(x)), collapse = ", "))
    NA_character_
  }
  split_pages <- function(df, rows_per_page = 30) {
    n <- nrow(df); if (is.null(n) || n == 0) return(list(df))
    idx <- split(seq_len(n), ceiling(seq_len(n) / rows_per_page))
    lapply(idx, function(ii) df[ii, , drop = FALSE])
  }
  new_page   <- function() grid::grid.newpage()
  title_grob <- function(text, y = 0.95, size = 14) {
    grid::grid.text(text, x = 0.05, y = y, just = c("left","top"),
                    gp = grid::gpar(fontsize = size, fontface = "bold"))
  }
  para_grob  <- function(text, y, size = 11) {
    grid::grid.text(text, x = 0.05, y = y, just = c("left","top"),
                    gp = grid::gpar(fontsize = size))
  }
  draw_table <- function(df, y, height = 0.75) {
    tg <- gridExtra::tableGrob(df, rows = NULL)
    grid::pushViewport(grid::viewport(x = 0.5, y = y, width = 0.94, height = height))
    on.exit(grid::popViewport(), add = TRUE)
    grid::grid.draw(tg)
  }

  # ---- legacy-friendly field harvest ----
  fields_used <- paste_fields(
    get_or(res$fields,
           get_or(get_or(res$params$fields, NULL),
                  get_or(get_or(res$details$fields, NULL),
                         get_or(get_or(res$details$params$fields, NULL), NULL)))))
  n_records <- get_or(res$n_records,
                      get_or(NROW(get_or(res$data, NULL)),
                             get_or(get_or(res$details$n, NULL),
                                    get_or(length(get_or(res$ids, NULL)), NA))))
  n_clusters <- NA
  if (!is.null(res$clusters))      n_clusters <- length(unique(res$clusters))
  if (!is.null(res$labels))        n_clusters <- length(unique(res$labels))
  if (!is.null(res$best$clusters)) n_clusters <- length(unique(res$best$clusters))
  if (!is.null(res$best$labels))   n_clusters <- length(unique(res$best$labels))
  has_embeddings <- get_or(
    get_or(res$details$has_embeddings, NULL),
    !is.null(get_or(get_or(res$features$embeddings, NULL), get_or(res$embeddings, NULL)))
  )
  runtime_sec <- get_or(runtime_sec, get_or(res$runtime_sec, get_or(res$details$total_runtime, NULL)))

  # ---- params table (legacy) ----
  param_sources <- list(
    get_or(res$selected_params, NULL),
    get_or(res$params, NULL),
    get_or(res$details$params, NULL)
  )
  params_list <- Filter(Negate(is.null), param_sources)
  params_tbl  <- NULL
  if (length(params_list)) {
    flat <- list()
    for (ps in params_list) {
      scalars <- ps[vapply(ps, function(v) is.atomic(v) && length(v) == 1, TRUE)]
      flat <- c(flat, scalars)
    }
    if (length(flat)) {
      params_tbl <- data.frame(
        Parameter = names(flat),
        Value     = vapply(flat, as.character, character(1)),
        row.names = NULL, check.names = FALSE
      )
      key_first <- c("kmeans_k","hclust_k","pam_k","gc_best_threshold","knn_k",
                     "svd_dim","louvain_min_sim","cos_thresh","sn_window",
                     "sn_method","sn_thresh","gc_method","gc_dist_method",
                     "auto_tune","tune_metric")
      ord <- unique(c(intersect(key_first, params_tbl$Parameter), params_tbl$Parameter))
      params_tbl <- params_tbl[match(ord, params_tbl$Parameter), , drop = FALSE]
    }
  }

  # ---- legacy metrics tables ----
  as_metric_table <- function(x, digits = 5, prefer_method_col = TRUE) {
    if (is.null(x)) return(NULL)
    if (is.data.frame(x)) {
      out <- round_num(x, digits)
      if (prefer_method_col && !"Method" %in% names(out) && !is.null(rownames(out))) {
        out <- cbind(Method = rownames(out), out, row.names = NULL)
      }
      return(out)
    }
    if (is.matrix(x)) {
      out <- as.data.frame(x, stringsAsFactors = FALSE)
      if (!"Method" %in% names(out) && !is.null(rownames(out))) {
        out <- cbind(Method = rownames(out), out, row.names = NULL)
      }
      return(round_num(out, digits))
    }
    if (is.numeric(x) && !is.null(names(x))) {
      out <- as.data.frame(as.list(x), stringsAsFactors = FALSE)
      return(round_num(out, digits))
    }
    if (is.list(x)) {
      ok <- vapply(x, function(el) is.numeric(el) && !is.null(names(el)), TRUE)
      if (length(ok) && all(ok)) {
        methods <- names(x)
        metrics <- unique(unlist(lapply(x, names)))
        df <- data.frame(Method = methods, check.names = FALSE)
        for (m in metrics) df[[m]] <- vapply(x, function(el) get_or(el[[m]], NA_real_), numeric(1))
        return(round_num(df, digits))
      }
    }
    tryCatch({ round_num(as.data.frame(x, stringsAsFactors = FALSE), digits) },
             error = function(e) NULL)
  }
  perf_tbl <- as_metric_table(get_or(res$performance, NULL), digits = digits)
  agr_tbl  <- as_metric_table(get_or(res$agreement,  NULL), digits = digits)

  # ---- new tuning curves (tidy) or legacy curve list ----
  tuning_df <- get_or(res$curves, NULL)
  is_tuning_like <- is.data.frame(tuning_df) &&
    all(c("method","time_sec","peak_mem_MB") %in% names(tuning_df))

  # For legacy: res$curves may be a list of data.frames -> draw as grobs
  make_curve_grob <- function(tb, title) {
    if (!has_gg || !is.data.frame(tb)) return(NULL)
    xcol <- intersect(c("k","k_knn","min_sim","embed_k","gc_threshold","n_clusters","step","threshold","eps","resolution_parameter"),
                      names(tb))
    xcol <- get_or(xcol[1], NULL)
    ycands <- c("silhouette","silhouette_pen","modularity","ch","db","gap","ari","f1","precision","recall",
                "adj_rand","rand","jaccard","F_measure","fowlkes_mallow",".score")
    ycol <- intersect(ycands, names(tb)); ycol <- get_or(ycol[1], NULL)
    if (is.null(xcol) || is.null(ycol)) {
      num_cols <- names(tb)[vapply(tb, is.numeric, TRUE)]
      if (length(num_cols) >= 2) { xcol <- num_cols[1]; ycol <- num_cols[2] } else { return(NULL) }
    }
    tb <- round_num(tb, digits)
    p <- ggplot2::ggplot(tb, ggplot2::aes_string(x = xcol, y = ycol)) +
      ggplot2::geom_line() + ggplot2::geom_point() +
      ggplot2::labs(title = title, x = xcol, y = ycol) +
      ggplot2::theme_minimal(base_size = 11)
    ggplot2::ggplotGrob(p)
  }
  legacy_curve_grobs <- list()
  if (!is_tuning_like && is.list(res$curves) && length(res$curves)) {
    for (nm in names(res$curves)) {
      g <- make_curve_grob(res$curves[[nm]], nm)
      if (!is.null(g)) legacy_curve_grobs[[nm]] <- g
    }
  }

  # ---- create output ----
  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  grDevices::pdf(file, width = 8.5, height = 11, onefile = TRUE)
  on.exit(try(grDevices::dev.off(), silent = TRUE), add = TRUE)

  # Cover page
  new_page()
  title_grob(sprintf("%s — Entity Resolution Report", dataset_name))
  cover_lines <- c(
    sprintf("Generated at: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    if (!is.na(fields_used)) sprintf("Fields used: %s", fields_used) else NULL,
    if (!is.na(n_records))   sprintf("Records: %s", format(n_records, big.mark = ",")) else NULL,
    if (!is.na(n_clusters))  sprintf("Clusters (inferred): %s", format(n_clusters, big.mark = ",")) else NULL,
    if (!is.null(has_embeddings)) sprintf("Embeddings detected: %s", if (isTRUE(has_embeddings)) "yes" else "no") else NULL,
    if (!is.null(runtime_sec)) sprintf("Total runtime: %.2f sec", runtime_sec) else NULL
  )
  para_grob(paste(cover_lines, collapse = "\n"), y = 0.88)

  # ---- Selected Parameters (legacy) ----
  if (!is.null(params_tbl) && nrow(params_tbl)) {
    new_page(); title_grob("Selected Parameters", y = 0.96)
    draw_table(params_tbl, y = 0.55, height = 0.8)
  }

  # ---- Performance (legacy) ----
  if (!is.null(perf_tbl) && nrow(perf_tbl)) {
    pages <- split_pages(perf_tbl, rows_per_page = 28)
    for (i in seq_along(pages)) {
      new_page(); title_grob("Clustering Agreement — Performance", y = 0.96)
      if (length(pages) > 1) para_grob(sprintf("Page %d of %d", i, length(pages)), y = 0.91, size = 9)
      draw_table(pages[[i]], y = 0.55, height = 0.8)
    }
  }

  # ---- Agreement (legacy) ----
  if (!is.null(agr_tbl) && nrow(agr_tbl)) {
    pages <- split_pages(agr_tbl, rows_per_page = 28)
    for (i in seq_along(pages)) {
      new_page(); title_grob("Agreement Details", y = 0.96)
      if (length(pages) > 1) para_grob(sprintf("Page %d of %d", i, length(pages)), y = 0.91, size = 9)
      draw_table(pages[[i]], y = 0.55, height = 0.8)
    }
  }

  # ---- NEW: Tuning/benchmark pages when res$curves is tidy ----
  if (is_tuning_like) {
    curves <- tuning_df

    # normalize expected columns
    needed <- c("method","silhouette","silhouette_pen","ch","db","modularity",
                "stability","ari","pairwise_f1","time_sec","peak_mem_MB",
                "k","k_knn","min_sim","eps","minPts","resolution_parameter","max_dist")
    for (nm in needed) if (is.null(curves[[nm]])) curves[[nm]] <- NA
    # score col
    score_col <- switch(objective,
                        silhouette = "silhouette",
                        silhouette_penalized = "silhouette_pen",
                        ch = "ch",
                        db_min = "db",
                        modularity = "modularity",
                        stability = "stability",
                        ari = "ari",
                        pairwise_f1 = "pairwise_f1",
                        stop("Unknown objective for report: ", objective)
    )
    curves$.score <- curves[[score_col]]
    if (objective == "db_min") curves$.score <- -curves$.score

    # Best per method + Top-N
    if (has_dplyr) {
      best_tbl <- curves |>
        dplyr::group_by(.data$method) |>
        dplyr::arrange(dplyr::desc(.data$.score)) |>
        dplyr::slice_head(n = 1) |>
        dplyr::ungroup()

      top_tbl <- curves |>
        dplyr::arrange(dplyr::desc(.data$.score)) |>
        dplyr::slice_head(n = min(top_n, nrow(curves)))
    } else {
      # fallbacks without dplyr
      best_tbl <- do.call(rbind, lapply(split(curves, curves$method), function(df) df[which.max(df$.score), , drop = FALSE]))
      top_tbl  <- curves[order(-curves$.score), , drop = FALSE]
      top_tbl  <- utils::head(top_tbl, min(top_n, nrow(top_tbl)))
    }

    # key x-axis chooser per method (for score plot)
    select_x <- function(df) {
      x <- if (!all(is.na(df$k))) "k" else
        if (!all(is.na(df$k_knn))) "k_knn" else
          if (!all(is.na(df$resolution_parameter))) "resolution_parameter" else
            if (!all(is.na(df$eps))) "eps" else
              if (!all(is.na(df$min_sim))) "min_sim" else
                if (!all(is.na(df$max_dist))) "max_dist" else NULL
      x
    }
    curves$.xvar <- NA_real_
    for (m in unique(curves$method)) {
      idx <- which(curves$method == m)
      xnm <- select_x(curves[idx, , drop = FALSE])
      if (!is.null(xnm)) curves$.xvar[idx] <- curves[idx, xnm, drop = TRUE]
    }

    # Best-per-method table (compact)
    fmt_num <- function(x) ifelse(is.na(x), "", sprintf(paste0("%.", digits, "f"), x))
    best_view <- best_tbl |>
      dplyr::mutate(dplyr::across(c(.data$silhouette, .data$silhouette_pen, .data$ch, .data$db,
                                    .data$modularity, .data$stability, .data$ari, .data$pairwise_f1,
                                    .data$time_sec, .data$peak_mem_MB), \(z) as.numeric(z))) |>
      dplyr::mutate(dplyr::across(c(.data$silhouette, .data$silhouette_pen, .data$ch, .data$db,
                                    .data$modularity, .data$stability, .data$ari, .data$pairwise_f1,
                                    .data$time_sec, .data$peak_mem_MB), fmt_num)) |>
      dplyr::select(.data$method, .data$k, .data$k_knn, .data$min_sim, .data$eps, .data$minPts,
                    .data$resolution_parameter, .data$max_dist,
                    .data$silhouette_pen, .data$ch, .data$db, .data$modularity,
                    .data$time_sec, .data$peak_mem_MB)

    # Top-N overall table
    top_view <- top_tbl |>
      dplyr::mutate(dplyr::across(c(.data$.score, .data$silhouette_pen, .data$ch, .data$db,
                                    .data$modularity, .data$time_sec, .data$peak_mem_MB), \(z) as.numeric(z))) |>
      dplyr::mutate(dplyr::across(c(.data$.score, .data$silhouette_pen, .data$ch, .data$db,
                                    .data$modularity, .data$time_sec, .data$peak_mem_MB), fmt_num)) |>
      dplyr::select(.data$method, .data$k, .data$k_knn, .data$min_sim, .data$eps, .data$minPts,
                    .data$resolution_parameter, .data$max_dist,
                    .data$.score, .data$silhouette_pen, .data$ch, .data$db,
                    .data$modularity, .data$time_sec, .data$peak_mem_MB)

    # Page: Best per method
    new_page()
    title_grob(sprintf("%s — Tuning summary (%s)", dataset_name, objective), y = 0.96)
    draw_table(best_view, y = 0.55, height = 0.8)

    # Page: Top-N overall
    new_page()
    title_grob(sprintf("Top %d configurations overall", nrow(top_view)), y = 0.96)
    draw_table(top_view, y = 0.55, height = 0.8)

    # Page: Score curves by method
    if (has_gg) {
      # derive highlighted points (best per method)
      best_points <- best_tbl
      # get x for each best row:
      best_points$.xvar <- NA_real_
      for (i in seq_len(nrow(best_points))) {
        m <- best_points$method[i]
        xnm <- select_x(best_points[i, , drop = FALSE])
        if (!is.null(xnm)) best_points$.xvar[i] <- best_points[[xnm]][i]
      }
      p_scores <- ggplot2::ggplot(curves, ggplot2::aes(x = .data$.xvar, y = .data$.score, group = .data$method)) +
        ggplot2::geom_point(alpha = 0.7) +
        ggplot2::geom_line(alpha = 0.5) +
        ggplot2::geom_point(data = best_points,
                            ggplot2::aes(x = .data$.xvar, y = .data$.score),
                            size = 3) +
        ggplot2::facet_wrap(~ method, scales = "free_x") +
        ggplot2::labs(title = sprintf("%s — Tuning curves (%s)", dataset_name, objective),
                      x = "Key parameter (method-specific)", y = "Score") +
        ggplot2::theme_minimal(base_size = 12)
      print(p_scores)
    }

    # Page: Runtime per method
    if (has_gg) {
      p_time <- ggplot2::ggplot(curves,
                                ggplot2::aes(x = stats::reorder(.data$method, .data$time_sec, median, na.rm = TRUE),
                                             y = .data$time_sec)) +
        ggplot2::geom_boxplot(outlier.shape = NA) +
        ggplot2::geom_point(position = ggplot2::position_jitter(width = 0.1), alpha = 0.5) +
        ggplot2::coord_flip() +
        ggplot2::labs(title = "Runtime per method", x = "", y = "Seconds") +
        ggplot2::theme_minimal(base_size = 12)
      print(p_time)
    }

    # Page: Peak memory per method
    if (has_gg) {
      p_mem <- ggplot2::ggplot(curves,
                               ggplot2::aes(x = stats::reorder(.data$method, .data$peak_mem_MB, median, na.rm = TRUE),
                                            y = .data$peak_mem_MB)) +
        ggplot2::geom_boxplot(outlier.shape = NA) +
        ggplot2::geom_point(position = ggplot2::position_jitter(width = 0.1), alpha = 0.5) +
        ggplot2::coord_flip() +
        ggplot2::labs(title = "Peak memory per method", x = "", y = "MiB") +
        ggplot2::theme_minimal(base_size = 12)
      print(p_mem)
    }
  }

  # ---- Legacy curve pages (if any) ----
  if (length(legacy_curve_grobs)) {
    for (nm in names(legacy_curve_grobs)) {
      new_page(); title_grob("Method Curves", y = 0.96)
      grid::pushViewport(grid::viewport(x = 0.5, y = 0.47, width = 0.94, height = 0.82))
      on.exit(grid::popViewport(), add = TRUE)
      grid::grid.draw(legacy_curve_grobs[[nm]])
    }
  }

  # ---- Top items (legacy) ----
  if (!is.null(res$top_items) && is.data.frame(res$top_items) && nrow(res$top_items) > 0) {
    new_page()
    title_grob(sprintf("Top %d items", min(top_n, nrow(res$top_items))), y = 0.96)
    draw_table(utils::head(round_num(res$top_items, digits), top_n), y = 0.55, height = 0.8)
  }

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


# ── er_data_profile helpers ──────────────────────────────────────────────────

# Cover page: title block + overview stats + field-type bar chart
.dp_cover <- function(df, diag, flds, title) {
  old <- graphics::par(mar = c(0, 0, 0, 0))
  on.exit(graphics::par(old), add = TRUE)

  graphics::layout(matrix(c(1, 2, 3), nrow = 3), heights = c(0.18, 0.35, 0.47))

  # ---- panel 1: title ----
  graphics::plot.new()
  graphics::text(0.5, 0.65, title,
                 cex = 1.9, font = 2, adj = c(0.5, 0.5))
  graphics::text(0.5, 0.25,
                 paste("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M")),
                 cex = 0.85, adj = c(0.5, 0.5), col = "gray40")

  # ---- panel 2: overview stats text ----
  graphics::plot.new()
  n_rec   <- if (!is.null(diag)) diag$n        else nrow(df)
  n_col   <- ncol(df)
  mode_   <- if (!is.null(diag)) diag$mode     else "unknown"
  e_pairs <- if (!is.null(diag)) diag$estimated_pairs else NA
  blk_key <- if (!is.null(diag)) diag$recommended_block_key    else NA
  blk_met <- if (!is.null(diag)) diag$recommended_block_method else NA
  src_col <- if (!is.null(diag)) diag$source_col else NA
  id_col  <- if (!is.null(diag)) diag$id_col     else NA

  lines <- c(
    sprintf("Records       : %s", format(n_rec,  big.mark = ",")),
    sprintf("Columns       : %d", n_col),
    sprintf("Mode          : %s", mode_),
    if (!is.na(e_pairs)) sprintf("Estimated pairs: %s", format(round(e_pairs), big.mark = ",")) else NULL,
    if (!is.na(id_col))  sprintf("ID column      : %s", id_col)  else NULL,
    if (!is.na(src_col)) sprintf("Source column  : %s", src_col) else NULL,
    if (!is.na(blk_key)) sprintf("Block key      : %s", blk_key) else NULL,
    if (!is.na(blk_met)) sprintf("Block method   : %s", blk_met) else NULL
  )
  graphics::text(0.05, 0.92, paste(lines, collapse = "\n"),
                 adj = c(0, 1), cex = 0.95, family = "mono")

  # ---- panel 3: field-type bar chart ----
  if (!is.null(flds) && nrow(flds) > 0 && "type" %in% names(flds)) {
    type_counts <- sort(table(flds$type), decreasing = TRUE)
    cols <- grDevices::hcl.colors(length(type_counts), palette = "Set2")
    old2 <- graphics::par(mar = c(4, 6, 3, 1))
    on.exit(graphics::par(old2), add = TRUE)
    graphics::barplot(type_counts,
                      horiz    = TRUE,
                      las      = 1,
                      col      = cols,
                      xlab     = "Number of fields",
                      main     = "Field types",
                      cex.names = 0.85,
                      cex.axis  = 0.8)
  } else {
    graphics::plot.new()
    graphics::text(0.5, 0.5, "(no field diagnosis available)",
                   cex = 0.85, col = "gray50", adj = c(0.5, 0.5))
  }

  graphics::layout(1)   # reset layout
}

# Per-field page: info text + distribution chart + missingness bar
.dp_field_page <- function(col, frow, top_k_cats = 10L, hist_bins = 20L) {
  old <- graphics::par(mar = c(0, 0, 0, 0))
  on.exit(graphics::par(old), add = TRUE)

  graphics::layout(matrix(c(1, 2, 3), nrow = 3), heights = c(0.28, 0.57, 0.15))

  ftype    <- if (!is.null(frow)) frow$type            else "unknown"
  miss_pct <- if (!is.null(frow)) frow$missingness      else mean(is.na(col))
  card     <- if (!is.null(frow)) frow$cardinality_ratio else NA
  avg_tok  <- if (!is.null(frow)) frow$avg_tokens        else NA
  sim_meth <- if (!is.null(frow)) frow$sim_method         else NA
  blk_cand <- if (!is.null(frow)) frow$blocking_candidate else FALSE
  fname    <- if (!is.null(frow) && "name" %in% names(frow)) frow$name else "field"

  # ---- panel 1: field info ----
  graphics::plot.new()
  graphics::text(0.5, 0.97,
                 sprintf("Field: %s", fname),
                 adj = c(0.5, 1), cex = 1.3, font = 2)
  info_lines <- c(
    sprintf("Type            : %s", ftype),
    sprintf("Missingness     : %.1f%%", miss_pct * 100),
    if (!is.na(card))    sprintf("Cardinality ratio: %.3f", card)    else NULL,
    if (!is.na(avg_tok)) sprintf("Avg tokens       : %.1f", avg_tok) else NULL,
    if (!is.na(sim_meth)) sprintf("Similarity method: %s", sim_meth) else NULL,
    sprintf("Blocking candidate: %s", if (isTRUE(blk_cand)) "yes" else "no")
  )
  graphics::text(0.05, 0.72, paste(info_lines, collapse = "\n"),
                 adj = c(0, 1), cex = 0.88, family = "mono")

  # ---- panel 2: distribution chart ----
  non_miss <- col[!is.na(col)]
  old2 <- graphics::par(mar = c(4, 4, 2, 1))
  on.exit(graphics::par(old2), add = TRUE)

  if (length(non_miss) == 0L) {
    graphics::plot.new()
    graphics::text(0.5, 0.5, "(all values missing)", cex = 0.9, col = "gray50", adj = c(0.5, 0.5))
  } else if (ftype %in% c("numeric", "year", "integer")) {
    vals <- suppressWarnings(as.numeric(non_miss))
    vals <- vals[!is.na(vals)]
    if (length(vals) > 0L) {
      graphics::hist(vals, breaks = hist_bins, col = "#7EB0D5", border = "white",
                     main = "Value distribution", xlab = fname, ylab = "Count")
    } else {
      graphics::plot.new()
      graphics::text(0.5, 0.5, "(no numeric values)", cex = 0.9, col = "gray50")
    }
  } else if (ftype %in% c("text", "long_text", "abstract", "title")) {
    # word-count histogram
    word_counts <- nchar(gsub("[^ ]+", "", trimws(as.character(non_miss)))) + 1L
    graphics::hist(word_counts, breaks = hist_bins, col = "#B2D8B2", border = "white",
                   main = "Word-count distribution", xlab = "Words per value", ylab = "Count")
  } else {
    # categorical: top-k bar chart
    tbl <- sort(table(as.character(non_miss)), decreasing = TRUE)
    tbl <- utils::head(tbl, top_k_cats)
    cols <- grDevices::hcl.colors(length(tbl), palette = "Pastel1")
    graphics::barplot(tbl, horiz = TRUE, las = 1, col = cols,
                      main = sprintf("Top %d values", length(tbl)),
                      xlab = "Count", cex.names = 0.75, cex.axis = 0.8)
  }

  # ---- panel 3: missingness bar ----
  old3 <- graphics::par(mar = c(1, 4, 1, 1))
  on.exit(graphics::par(old3), add = TRUE)
  graphics::barplot(
    rbind(miss_pct, 1 - miss_pct),
    horiz   = TRUE,
    col     = c("#E88B8B", "#B2D8B2"),
    border  = NA,
    axes    = FALSE,
    names.arg = "",
    main    = ""
  )
  graphics::legend("bottom",
                   legend = c(sprintf("Missing %.1f%%", miss_pct * 100),
                              sprintf("Present %.1f%%", (1 - miss_pct) * 100)),
                   fill   = c("#E88B8B", "#B2D8B2"),
                   border = NA, horiz = TRUE, cex = 0.8, bty = "n")

  graphics::layout(1)
}

# Recommendations page: blocking + per-field similarity table
.dp_recommendations <- function(diag, flds) {
  old <- graphics::par(mar = c(0, 0, 0, 0))
  on.exit(graphics::par(old), add = TRUE)

  graphics::plot.new()
  graphics::text(0.5, 0.97, "Recommendations", adj = c(0.5, 1), cex = 1.5, font = 2)

  lines <- character(0)

  if (!is.null(diag)) {
    lines <- c(lines,
               "BLOCKING:",
               sprintf("  Needed  : %s", if (isTRUE(diag$blocking_needed)) "yes" else "no"),
               if (!is.null(diag$recommended_block_key))
                 sprintf("  Key     : %s", diag$recommended_block_key) else NULL,
               if (!is.null(diag$recommended_block_method))
                 sprintf("  Method  : %s", diag$recommended_block_method) else NULL,
               "")
  }

  if (!is.null(flds) && nrow(flds) > 0L) {
    lines <- c(lines, "SIMILARITY METHODS BY FIELD:", "")
    col_names <- c("Field", "Type", "Sim method", "Block cand.")
    hdr <- sprintf("%-22s %-14s %-16s %s", col_names[1], col_names[2], col_names[3], col_names[4])
    sep <- paste(rep("-", nchar(hdr)), collapse = "")
    lines <- c(lines, hdr, sep)
    for (i in seq_len(nrow(flds))) {
      r <- flds[i, ]
      sm  <- if (!is.null(r$sim_method) && !is.na(r$sim_method)) r$sim_method else "-"
      blk <- if (isTRUE(r$blocking_candidate)) "yes" else "no"
      lines <- c(lines, sprintf("%-22s %-14s %-16s %s",
                                substr(r$name, 1, 22),
                                substr(r$type, 1, 14),
                                substr(sm, 1, 16),
                                blk))
    }
  }

  graphics::text(0.03, 0.88, paste(lines, collapse = "\n"),
                 adj = c(0, 1), cex = 0.82, family = "mono")
}


#' Dataset profile PDF report
#'
#' Reads loaded data (from [er_load_input()] or a plain `data.frame`) and an
#' optional diagnosis (from [er_diagnose()]) and writes a multi-page PDF
#' summarising the dataset's structure and field characteristics.
#'
#' Pages produced:
#' \enumerate{
#'   \item Cover — title, overview statistics, and a field-type bar chart.
#'   \item One page per field — info panel, value-distribution chart, and a
#'         missingness bar.  Limited to \code{max_fields} fields.
#'   \item Recommendations — blocking strategy and per-field similarity methods.
#' }
#'
#' @param data A `data.frame` or the raw result from [er_load_input()].
#' @param diag Optional. The list returned by [er_diagnose()].  When `NULL`
#'   the function still runs but skips diagnosis-derived panels.
#' @param output_file Character.  Path for the output PDF.  Defaults to
#'   `"data_profile_<timestamp>.pdf"` in the current working directory.
#' @param title Character.  Report title shown on the cover page.  Defaults to
#'   the output file base name.
#' @param top_k_cats Integer.  Maximum number of categories shown in
#'   categorical bar charts (default `10`).
#' @param hist_bins Integer.  Number of histogram bins for numeric / word-count
#'   charts (default `20`).
#' @param max_fields Integer.  Maximum number of field pages to include
#'   (default `30`).  Fields are taken in column order.
#' @return (Invisible) the output file path.
#' @examples
#' \dontrun{
#' ## Basic usage — no ground truth needed
#' df   <- er_load_input("my_data.csv")
#' diag <- er_diagnose(df)
#' er_data_profile(df, diag, output_file = "reports/my_data_profile.pdf",
#'                 title = "My Dataset — Profile")
#'
#' ## Skip diagnosis (quick overview only)
#' er_data_profile(df, output_file = "quick_profile.pdf")
#' }
#' @export
er_data_profile <- function(data,
                             diag        = NULL,
                             output_file = NULL,
                             title       = NULL,
                             top_k_cats  = 10L,
                             hist_bins   = 20L,
                             max_fields  = 30L) {

  # ---- normalise data ----
  df <- if (is.data.frame(data)) data else as.data.frame(data)

  # ---- output path ----
  if (is.null(output_file)) {
    ts          <- format(Sys.time(), "%Y%m%d%H%M%S")
    output_file <- paste0("data_profile_", ts, ".pdf")
  }
  if (is.null(title)) {
    title <- tools::file_path_sans_ext(basename(output_file))
  }
  dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

  # ---- field metadata ----
  flds <- if (!is.null(diag) && !is.null(diag$fields)) diag$fields else NULL

  # fields to profile (cap at max_fields)
  field_names <- if (!is.null(flds)) flds$name else names(df)
  field_names <- intersect(field_names, names(df))
  field_names <- utils::head(field_names, max_fields)

  # ---- open PDF device ----
  grDevices::pdf(output_file, width = 8.5, height = 11, onefile = TRUE)
  on.exit(try(grDevices::dev.off(), silent = TRUE), add = TRUE)

  # ---- page 1: cover ----
  .dp_cover(df, diag, flds, title)

  # ---- pages 2…n: one per field ----
  for (fn in field_names) {
    col  <- df[[fn]]
    frow <- if (!is.null(flds)) flds[flds$name == fn, , drop = FALSE] else NULL
    if (!is.null(frow) && nrow(frow) == 0L) frow <- NULL
    if (!is.null(frow) && nrow(frow) > 0L)  frow <- frow[1L, ]
    .dp_field_page(col, frow, top_k_cats = top_k_cats, hist_bins = hist_bins)
  }

  # ---- last page: recommendations ----
  .dp_recommendations(diag, flds)

  invisible(output_file)
}
