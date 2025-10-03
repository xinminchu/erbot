########################################

#' Generate a timestamped filename
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

#' Save an organized ER PDF report (grid-only; no base graphics)
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

  # metadata
  fields_used <- paste_fields(
    get_or(res$fields,
      get_or(get_or(res$params$fields, NULL),
        get_or(get_or(res$details$fields, NULL),
          get_or(get_or(res$details$params$fields, NULL), NULL)))))
  n_records <- get_or(res$n_records,
                get_or(NROW(get_or(res$data, NULL)),
                  get_or(get_or(res$details$n, NULL),
                    get_or(length(get_or(res$ids, NULL)),
                      NA))))
  n_clusters <- NA
  if (!is.null(res$clusters))            n_clusters <- length(unique(res$clusters))
  if (!is.null(res$labels))              n_clusters <- length(unique(res$labels))
  if (!is.null(res$best$clusters))       n_clusters <- length(unique(res$best$clusters))
  if (!is.null(res$best$labels))         n_clusters <- length(unique(res$best$labels))
  has_embeddings <- get_or(
    get_or(res$details$has_embeddings, NULL),
    !is.null(get_or(get_or(res$features$embeddings, NULL), get_or(res$embeddings, NULL)))
  )
  runtime_sec <- get_or(runtime_sec, get_or(res$runtime_sec, get_or(res$details$total_runtime, NULL)))

  # coerce tables
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
      out <- round_num(out, digits)
      return(out)
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
    tryCatch({
      out <- as.data.frame(x, stringsAsFactors = FALSE)
      round_num(out, digits)
    }, error = function(e) NULL)
  }

  perf_tbl <- as_metric_table(get_or(res$performance, NULL), digits = digits)
  agr_tbl  <- as_metric_table(get_or(res$agreement,  NULL), digits = digits)

  # curves -> grobs
  curve_list <- list()
  if (is.list(res$curves)) {
    curve_list <- res$curves
  } else {
    gc_curve <- get_or(res$tuning$gc_threshold_curve, NULL)
    if (is.data.frame(gc_curve)) curve_list <- list(GC_threshold = gc_curve)
  }

  make_curve_grob <- function(tb, title) {
    if (!has_gg || !is.data.frame(tb)) return(NULL)
    xcol <- intersect(c("k","knn","min_sim","embed_k","gc_threshold","n_clusters","step","threshold"), names(tb))
    xcol <- get_or(xcol[1], NULL)
    ycands <- c("silhouette","modularity","ch","db","gap","ari","f1","precision","recall",
                "adj_rand","rand","jaccard","F_measure","fowlkes_mallow")
    ycol <- intersect(ycands, names(tb)); ycol <- get_or(ycol[1], NULL)
    if (is.null(xcol) || is.null(ycol)) {
      num_cols <- names(tb)[vapply(tb, is.numeric, TRUE)]
      if (length(num_cols) >= 2) { xcol <- num_cols[1]; ycol <- num_cols[2] } else { return(NULL) }
    }
    tb <- round_num(tb, digits)
    p <- ggplot2::ggplot(tb, ggplot2::aes_string(x = xcol, y = ycol)) +
      ggplot2::geom_line() + ggplot2::geom_point() +
      ggplot2::labs(title = title, x = xcol, y = ycol)
    ggplot2::ggplotGrob(p)
  }

  curve_grobs <- list()
  if (length(curve_list)) {
    for (nm in names(curve_list)) {
      g <- make_curve_grob(curve_list[[nm]], nm)
      if (!is.null(g)) curve_grobs[[nm]] <- g
    }
  }

  # draw helpers
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
    grid::pushViewport(grid::viewport(x = 0.5, y = y, width = 0.9, height = height))
    on.exit(grid::popViewport(), add = TRUE)
    grid::grid.draw(tg)
  }
  split_pages <- function(df, rows_per_page = 28) {
    n <- nrow(df)
    if (is.null(n) || n == 0) return(list(df))
    idx <- split(seq_len(n), ceiling(seq_len(n) / rows_per_page))
    lapply(idx, function(ii) df[ii, , drop = FALSE])
  }

  # render (safe device)
  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  grDevices::pdf(file, width = 8.5, height = 11, onefile = TRUE)

  # Cover
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

  # Parameters (if available)
  params_tbl <- tryCatch(er_params_df(res), error=function(e) NULL)
  if (!is.null(params_tbl) && nrow(params_tbl)) {
    new_page(); title_grob("Selected Parameters", y = 0.96)
    draw_table(params_tbl, y = 0.55, height = 0.8)
  }

  # Performance
  if (!is.null(perf_tbl) && nrow(perf_tbl)) {
    pages <- split_pages(perf_tbl, rows_per_page = 28)
    for (i in seq_along(pages)) {
      new_page(); title_grob("Clustering Agreement — Performance", y = 0.96)
      if (length(pages) > 1) para_grob(sprintf("Page %d of %d", i, length(pages)), y = 0.91, size = 9)
      draw_table(pages[[i]], y = 0.55, height = 0.8)
    }
  }

  # Agreement
  if (!is.null(agr_tbl) && nrow(agr_tbl)) {
    pages <- split_pages(agr_tbl, rows_per_page = 28)
    for (i in seq_along(pages)) {
      new_page(); title_grob("Agreement Details", y = 0.96)
      if (length(pages) > 1) para_grob(sprintf("Page %d of %d", i, length(pages)), y = 0.91, size = 9)
      draw_table(pages[[i]], y = 0.55, height = 0.8)
    }
  }

  # Curves
  if (length(curve_grobs)) {
    for (nm in names(curve_grobs)) {
      new_page(); title_grob("Method Curves", y = 0.96)
      grid::pushViewport(grid::viewport(x = 0.5, y = 0.47, width = 0.9, height = 0.82))
      on.exit(grid::popViewport(), add = TRUE)
      grid::grid.draw(curve_grobs[[nm]])
    }
  }

  # Top items
  if (!is.null(res$top_items) && is.data.frame(res$top_items) && nrow(res$top_items) > 0) {
    new_page()
    title_grob(sprintf("Top %d items", min(top_n, nrow(res$top_items))), y = 0.96)
    draw_table(utils::head(round_num(res$top_items, digits), top_n), y = 0.55, height = 0.8)
  }

  grDevices::dev.off()
  invisible(file)
}


########################################
