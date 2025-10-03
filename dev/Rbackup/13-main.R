#' High-level facade (full pipeline + plotting/tables)
#' @export
er_main <- function(
    data, truth=NULL, fields=NULL, extra_fields=NULL, id_col=NULL, embed_col=NULL,
    k_clusters=10, knn_k=15, mst_cut_ratio=5, mst_k=NULL, sn_window=40, sn_method="jw", sn_thresh=0.12,
    svd_dim=100, louvain_min_sim=0.0, cos_thresh=0.88,
    gc_thresholds=seq(0.06,0.20,0.02), gc_method="rlf", gc_dist_method="jw",
    auto_tune=TRUE, tune_metric="adj_rand", k_grid=c(5,10,15,20),
    eval_mode="labeled_only", write_csv=NULL, sheet=NULL,
    run_comm_methods=c("walktrap","infomap","fast_greedy","label_prop"),
    auto_plot=TRUE, show_tables=TRUE, show_progress=TRUE
){
  res <- er_unified_pipeline(
    data=data, truth=truth, sheet=sheet, id_col=id_col, fields=fields, extra_fields=extra_fields,
    embed_col=embed_col, k_clusters=k_clusters, knn_k=knn_k,
    mst_cut_ratio=mst_cut_ratio, mst_k=mst_k, sn_window=sn_window, sn_method=sn_method, sn_thresh=sn_thresh,
    svd_dim=svd_dim, louvain_min_sim=louvain_min_sim, cos_thresh=cos_thresh,
    gc_thresholds=gc_thresholds, gc_method=gc_method, gc_dist_method=gc_dist_method,
    auto_tune=auto_tune, tune_metric=tune_metric, k_grid=k_grid,
    eval_mode=eval_mode, write_csv=write_csv, run_comm_methods=run_comm_methods,
    show_progress=show_progress
  )
  if (isTRUE(show_tables)) {
    cat("\n===== Selected Parameters =====\n")
    print(list(
      kmeans_k = res$tuning$kmeans_k,
      hclust_k = res$tuning$hclust_k,
      pam_k    = res$tuning$pam_k,
      gc_best_threshold = res$tuning$gc_best_threshold
    ))
  }
  if (isTRUE(auto_plot))   er_autoplot_tuning(res)
  if (isTRUE(show_tables)) {
    cat("\n===== Tuning tables (top 5) =====\n")
    if (!is.null(res$tuning$gc_threshold_curve)) {
      gc_curve <- res$tuning$gc_threshold_curve
      metric <- er_pick_gc_metric(gc_curve, fallback_metric = (res$details$params$tune_metric %||% "adj_rand"))
      ord <- order(gc_curve[[metric]], decreasing=TRUE)
      print(utils::head(gc_curve[ord, c("threshold", metric), drop=FALSE], 5))
    }
    if (!is.null(res$performance)) print(res$performance)
  }
  invisible(res)
}

#' Minimal facade (quick comparisons)
#' @export
er_main_simple <- function(data,
                           truth = NULL,
                           fields = c("title","authors","address"),
                           k_grid = seq(10, 300, by = 10),
                           write_csv = NULL,
                           svd_k = 100,
                           knn_k = 15,
                           louvain_min_sim = 0.0,
                           use_methods = c("kmeans_tfidf_svd","louvain_knn","louvain_multifield")) {

  use_methods <- match.arg(use_methods,
                           c("kmeans_tfidf_svd","louvain_knn","louvain_multifield"),
                           several.ok = TRUE)

  df <- if (is.character(data) && file.exists(data)) {
    read.csv(data, stringsAsFactors = FALSE)
  } else if (is.data.frame(data)) {
    data
  } else {
    stop("Unsupported 'data' input. Provide a data.frame or a readable file path.")
  }
  n <- nrow(df)

  df_text <- er_select_fields(df, id_col = NULL, fields = fields, extra_fields = NULL,
                              normalize = TRUE, auto_fields = is.null(fields))
  Xsvd <- er_features_tfidf_svd(df_text$text_for_matching, svd_dim = svd_k)

  km_best <- NULL
  .er_kmeans_sweep <- function(X_dense, k_grid, seed=123, nstart=10) {
    k_grid <- sort(unique(k_grid[k_grid >= 2 & k_grid < nrow(X_dense)]))
    if (length(k_grid) == 0) stop("k_grid has no valid k.")
    rn <- sqrt(rowSums(X_dense^2)); rn[rn == 0] <- 1
    Z <- X_dense / rn
    best <- list(k = NULL, cl = NULL, score = -Inf)
    for (k in k_grid) {
      set.seed(seed); cl <- kmeans(Z, centers = k, nstart = nstart)$cluster
      centers <- rowsum(Z, cl) / as.vector(table(cl))
      cnorm <- sqrt(rowSums(centers^2)); cnorm[cnorm == 0] <- 1
      centers <- centers / cnorm
      cos_self <- rowSums(Z * centers[cl, , drop = FALSE])
      cos_to_all <- Z %*% t(centers)
      cos_next <- vapply(seq_len(nrow(Z)), function(i) max(cos_to_all[i, setdiff(seq_len(k), cl[i])]), numeric(1))
      s <- (cos_self - cos_next) / pmax(cos_self, cos_next, 1e-8)
      s_mean <- mean(s, na.rm = TRUE)
      if (is.finite(s_mean) && s_mean > best$score) best <- list(k = k, cl = cl, score = s_mean)
    }
    best
  }
  if ("kmeans_tfidf_svd" %in% use_methods) km_best <- .er_kmeans_sweep(Xsvd, k_grid)

  lv_knn <- NULL
  if ("louvain_knn" %in% use_methods) {
    lv_res <- er_louvain_knn(Xsvd, knn = knn_k, min_sim = louvain_min_sim)
    lv_knn <- as.integer(lv_res$labels)
  }

  lv_multi <- NULL
  if ("louvain_multifield" %in% use_methods) {
    spec <- list()
    if ("title" %in% fields && "title" %in% names(df))   spec <- append(spec, list(list(name="title",   type="jw", w=1)))
    if ("authors" %in% fields && "authors" %in% names(df)) spec <- append(spec, list(list(name="authors", type="jw", w=1)))
    if ("address" %in% fields && "address" %in% names(df)) spec <- append(spec, list(list(name="address", type="jw", w=1)))
    if ("year" %in% names(df)) spec <- append(spec, list(list(name="year", type="year", w=1, tau=0.5)))
    blk <- if ("title" %in% names(df)) { tolower(substr(df$title %||% "", 1, 1)) } else { rep("ALL", n) }
    S <- er_similarity_multifield(df, spec = spec, block_key = blk, top_k = knn_k)
    lv_multi <- er_louvain_from_S(S, min_sim = louvain_min_sim)
  }

  .eval_ext <- function(pred, truth) {
    out <- list(ARI = NA_real_, Precision = NA_real_, Recall = NA_real_, F1 = NA_real_)
    if (is.null(truth) || !is.vector(truth) || length(truth) != length(pred)) return(out)
    if (requireNamespace("aricode", quietly = TRUE))
      out$ARI <- tryCatch(aricode::ARI(pred, truth), error=function(e) NA_real_)
    pair_count <- function(labels) { sum(choose(as.numeric(table(labels)), 2)) }
    cont <- table(pred, truth)
    TP <- sum(choose(as.numeric(cont), 2))
    Pp <- pair_count(pred); Pt <- pair_count(truth)
    prec <- if (Pp == 0) 0 else TP / Pp
    rec  <- if (Pt == 0) 0 else TP / Pt
    out$Precision <- prec; out$Recall <- rec; out$F1 <- if ((prec+rec)==0) 0 else 2*prec*rec/(prec+rec)
    out
  }

  cand <- list()
  if (!is.null(km_best))    cand$kmeans_tfidf_svd <- list(name="kmeans_tfidf_svd", cl=km_best$cl, k=km_best$k, score_ext = .eval_ext(km_best$cl, truth), score_int = km_best$score)
  if (!is.null(lv_knn))     cand$louvain_knn      <- list(name="louvain_knn",      cl=lv_knn,       k=NA_integer_,     score_ext = .eval_ext(lv_knn, truth),       score_int = NA_real_)
  if (!is.null(lv_multi))   cand$louvain_multifield <- list(name="louvain_multifield", cl=lv_multi, k=NA_integer_,    score_ext = .eval_ext(lv_multi, truth),     score_int = NA_real_)
  if (!length(cand)) stop("No methods ran. Check inputs or 'use_methods'.")

  pick <- NULL
  if (!is.null(truth)) {
    scores <- sapply(cand, function(x) c(ARI = x$score_ext$ARI %||% -Inf, F1  = x$score_ext$F1  %||% -Inf))
    ord <- order(scores["ARI",], scores["F1",], decreasing = TRUE, na.last = TRUE)
    pick <- cand[[ord[1]]]
  } else {
    pick <- cand[[1L]]
  }

  pred <- pick$cl
  outdf <- data.frame(row_id = seq_len(n), cluster = as.integer(pred))
  if (!is.null(write_csv)) {
    idcol <- intersect(c("id","ID","Id","record_id"), names(df))
    if (length(idcol)) outdf[[idcol[1]]] <- df[[idcol[1]]]
    keep <- intersect(fields, names(df))
    outdf <- cbind(outdf, df[keep])
    utils::write.csv(outdf, write_csv, row.names = FALSE)
  }

  list(
    clusters = as.integer(pred),
    chosen = pick$name,
    k = pick$k,
    external = pick$score_ext,
    internal = pick$score_int,
    summary = list(n = n, fields = fields, used_methods = names(cand))
  )
}
