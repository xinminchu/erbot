########################################

#' Unified ER pipeline (full)
#' @inheritParams er_select_fields
#' @param truth optional truth (see [er_truth_from_any])
#' @param embed_col name of embedding column if present
#' @param k_clusters default k for centroidal methods
#' @param knn_k k for kNN graphs
#' @param gc_thresholds thresholds to sweep for GC
#' @param auto_tune logical: tune k by silhouette (or CA when truth)
#' @return list with performance, predictions, tuning, details
#' @export
er_unified_pipeline <- function(
    data, truth=NULL, sheet=NULL, id_col=NULL, fields=NULL, extra_fields=NULL,
    id_candidates=c("id","affiliation_id","record_id","rec_id","docid","rowid","paper_id"),
    text_candidates=c("title","name","raw_title","string","text","affiliation","aggregate value","clean ag.value"),
    embed_col=NULL, embed_candidates=c("embedded clean ag.value","embedded ag.value","emb","embedding","vector","embedding_clean"),
    k_clusters=10, knn_k=15,
    mst_cut_ratio=5, mst_k=NULL, sn_window=40, sn_method="jw", sn_thresh=0.12,
    svd_dim=100, louvain_min_sim=0.0, cos_thresh=0.88,
    gc_thresholds=seq(0.06,0.20,0.02), gc_method=c("lf","sl","rlf"), gc_dist_method="jw",
    auto_tune=TRUE, tune_metric="adj_rand", k_grid=c(5,10,15,20),
    eval_mode=c("labeled_only","singleton_fill"), write_csv=NULL,
    run_comm_methods=c("walktrap","infomap","fast_greedy","label_prop"),
    show_progress=TRUE
){
  eval_mode <- match.arg(eval_mode); gc_method <- match.arg(gc_method)
  flags <- c(load=1, select=1, tfidf=1, embed=1, kmeans=1, mstsn=1, louvain=1, embknn=1, hc=1, pam=1,
             gc = as.integer(!is.null(gc_thresholds) && length(gc_thresholds) > 0),
             comm = as.integer(length(run_comm_methods) > 0), eval=1, write=1)
  p <- if (isTRUE(show_progress)) er_progress_start(sum(flags), "ER unified pipeline") else NULL

  df_raw  <- er_load_input(data, sheet = sheet);                                       er_progress_tick(p, "Loaded input")
  df_text <- er_select_fields(df_raw, id_col=id_col, fields=fields, extra_fields=extra_fields,
                              id_candidates=id_candidates, text_candidates=text_candidates,
                              normalize=TRUE, auto_fields=TRUE);                       er_progress_tick(p, "Selected fields")

  Xsvd <- er_features_tfidf_svd(df_text$text_for_matching, svd_dim=svd_dim);          er_progress_tick(p, "Built TF-IDF + SVD")
  ec <- if (is.null(embed_col)) intersect(tolower(embed_candidates), names(df_raw))[1] else tolower(embed_col)
  E <- if (!is.na(ec) && !is.null(ec) && ec %in% names(df_raw)) er_safe_parse_embedding_col(df_raw[[ec]]) else NULL
  er_progress_tick(p, sprintf("Parsed embeddings: %s", ifelse(is.null(E), "no", "yes")))

  # KMeans (tuned)
  pick_k <- k_clusters
  if (auto_tune) {
    if (!is.null(truth)) {
      tt <- er_truth_from_any(truth); gold_map <- setNames(tt$cluster_id, tt$id)
      idx <- which(df_text$id %in% names(gold_map)); gold <- gold_map[df_text$id[idx]]
      km_rows <- lapply(k_grid, function(k){ labs <- er_kmeans_from_X(Xsvd[idx,,drop=FALSE], k=k); met <- er_eval_ca_one(labs, gold); dplyr::bind_cols(tibble::tibble(k=k), met) })
      km_tune <- dplyr::bind_rows(km_rows); pick_k <- km_tune$k[order(km_tune[[tune_metric]], decreasing=TRUE)][1]
    } else {
      Dcos <- er_cosine_dist(Xsvd)
      km_rows <- lapply(k_grid, function(k){ labs <- er_kmeans_from_X(Xsvd, k=k); tibble::tibble(k=k, silhouette=er_silhouette_avg(labs, Dcos)) })
      km_sil_curve <- dplyr::bind_rows(km_rows); pick_k <- km_sil_curve$k[which.max(km_sil_curve$silhouette %||% NA_real_)]; if (!length(pick_k)) pick_k <- k_clusters
    }
  }
  pred_kmeans <- er_kmeans_from_X(Xsvd, k=pick_k);                                     er_progress_tick(p, sprintf("KMeans (k=%d)", pick_k))

  # MST/SN+edit
  pred_mst_sn <- er_mst_or_sn_edit(df_text$text_for_matching, mst_cut_ratio=mst_cut_ratio, mst_k=mst_k,
                                   sn_window=sn_window, sn_method=sn_method, sn_thresh=sn_thresh)
  er_progress_tick(p, "MST/SN+edit")

  # Louvain kNN
  lv <- er_louvain_knn(Xsvd, knn=knn_k, min_sim=louvain_min_sim)
  pred_louvain <- lv$labels; g_knn <- lv$graph;                                        er_progress_tick(p, sprintf("Louvain kNN (k=%d)", knn_k))

  # Embed kNN
  pred_embed <- if (!is.null(E)) er_embed_knn(E, k=knn_k, cos_thresh=cos_thresh) else NULL
  er_progress_tick(p, sprintf("Embed-kNN: %s", ifelse(is.null(pred_embed), "skipped", "done")))

  # HC (tuned)
  pick_k_hc <- k_clusters
  if (auto_tune) {
    if (!is.null(truth)) {
      tt <- er_truth_from_any(truth); gold_map <- setNames(tt$cluster_id, tt$id)
      idx <- which(df_text$id %in% names(gold_map)); gold <- gold_map[df_text$id[idx]]
      rows <- lapply(k_grid, function(k){ labs <- er_hclust_from_X(Xsvd[idx,,drop=FALSE], k=k); met <- er_eval_ca_one(labs, gold); dplyr::bind_cols(tibble::tibble(k=k), met) })
      hc_tune <- dplyr::bind_rows(rows); pick_k_hc <- hc_tune$k[order(hc_tune[[tune_metric]], decreasing=TRUE)][1]
    } else {
      Dcos <- er_cosine_dist(Xsvd)
      rows <- lapply(k_grid, function(k){ labs <- er_hclust_from_X(Xsvd, k=k); tibble::tibble(k=k, silhouette=er_silhouette_avg(labs, Dcos)) })
      hclust_sil_curve <- dplyr::bind_rows(rows); pick_k_hc <- hclust_sil_curve$k[which.max(hclust_sil_curve$silhouette %||% NA_real_)]; if (!length(pick_k_hc)) pick_k_hc <- k_clusters
    }
  }
  pred_hclust <- er_hclust_from_X(Xsvd, k=pick_k_hc);                                   er_progress_tick(p, sprintf("HC (k=%d)", pick_k_hc))

  # PAM (tuned)
  pick_k_pam <- k_clusters
  if (auto_tune) {
    if (!is.null(truth)) {
      tt <- er_truth_from_any(truth); gold_map <- setNames(tt$cluster_id, tt$id)
      idx <- which(df_text$id %in% names(gold_map)); gold <- gold_map[df_text$id[idx]]
      rows <- lapply(k_grid, function(k){ labs <- er_pam_from_X(Xsvd[idx,,drop=FALSE], k=k); met <- er_eval_ca_one(labs, gold); dplyr::bind_cols(tibble::tibble(k=k), met) })
      pam_tune <- dplyr::bind_rows(rows); pick_k_pam <- pam_tune$k[order(pam_tune[[tune_metric]], decreasing=TRUE)][1]
    } else {
      Dcos <- er_cosine_dist(Xsvd)
      rows <- lapply(k_grid, function(k){ labs <- er_pam_from_X(Xsvd, k=k); tibble::tibble(k=k, silhouette=er_silhouette_avg(labs, Dcos)) })
      pam_sil_curve <- dplyr::bind_rows(rows); pick_k_pam <- pam_sil_curve$k[which.max(pam_sil_curve$silhouette %||% NA_real_)]; if (!length(pick_k_pam)) pick_k_pam <- k_clusters
    }
  }
  pred_pam <- er_pam_from_X(Xsvd, k=pick_k_pam);                                        er_progress_tick(p, sprintf("PAM (k=%d)", pick_k_pam))

  # Graph Coloring (threshold sweep)
  pred_gc <- NULL; gc_best_thr <- NA; gc_tune_tbl <- NULL
  if (!is.null(gc_thresholds) && length(gc_thresholds) && nrow(df_text) >= 2) {
    truth_vec <- if (!is.null(truth)) { tt <- er_truth_from_any(truth); setNames(tt$cluster_id, tt$id)[df_text$id] } else NULL
    gc_res <- er_gc_from_text(df_text$text_for_matching, thresholds=gc_thresholds, gc_method=gc_method,
                              dist_method=gc_dist_method, tune_metric=tune_metric, truth=truth_vec,
                              progress=p, dist_block=4000)
    pred_gc <- gc_res$labels; gc_best_thr <- gc_res$best_threshold; gc_tune_tbl <- gc_res$tuning
  }
  er_progress_tick(p, "Graph Coloring")

  # Extra communities
  extra_preds <- list()
  if (length(run_comm_methods) && !is.null(lv$graph)) {
    for (m in run_comm_methods) {
      labs <- tryCatch({
        if (igraph::ecount(lv$graph) == 0) rep(1L, nrow(df_text)) else switch(m,
                                                                              "walktrap"    = igraph::membership(igraph::cluster_walktrap(lv$graph)),
                                                                              "infomap"     = igraph::membership(igraph::cluster_infomap(lv$graph)),
                                                                              "fast_greedy" = igraph::membership(igraph::cluster_fast_greedy(lv$graph)),
                                                                              "label_prop"  = igraph::membership(igraph::cluster_label_prop(lv$graph)),
                                                                              rep(1L, nrow(df_text)))
      }, error=function(e) rep(1L, nrow(df_text)))
      extra_preds[[paste0("pred_comm_", m)]] <- as.integer(labs)
    }
  }
  er_progress_tick(p, "Extra communities")

  out <- tibble::tibble(
    id = df_text$id,
    text_for_matching = df_text$text_for_matching,
    pred_kmeans   = as.integer(pred_kmeans),
    pred_mst_or_sn = as.integer(pred_mst_sn),
    pred_louvain  = as.integer(pred_louvain),
    pred_hclust   = as.integer(pred_hclust),
    pred_pam      = as.integer(pred_pam)
  )
  if (!is.null(pred_embed)) out$pred_embedKNN <- as.integer(pred_embed)
  if (!is.null(pred_gc))    out$pred_gc      <- as.integer(pred_gc)
  if (length(extra_preds))  out <- dplyr::bind_cols(out, tibble::as_tibble(extra_preds))

  perf_tbl <- NULL
  if (!is.null(truth)) {
    truth_clusters <- er_truth_from_any(truth)
    if (nrow(truth_clusters)) {
      out <- dplyr::left_join(out, truth_clusters, by="id")
      idx <- if (eval_mode == "singleton_fill") {
        if (any(is.na(out$cluster_id))) {
          start <- suppressWarnings(max(as.integer(out$cluster_id), na.rm=TRUE)); start <- ifelse(is.finite(start), start, 0L)
          miss <- which(is.na(out$cluster_id)); out$cluster_id[miss] <- start + seq_along(miss)
        }
        seq_len(nrow(out))
      } else which(!is.na(out$cluster_id))
      if (length(idx) >= 2) {
        gold <- out$cluster_id[idx]; meth_cols <- grep("^pred_", names(out), value=TRUE)
        perf_tbl <- dplyr::bind_rows(lapply(meth_cols, function(mc){
          met <- er_eval_ca_one(out[[mc]][idx], gold)
          dplyr::bind_cols(tibble::tibble(Method = sub("^pred_","",mc)), met)
        }))
        perf_tbl$Method <- dplyr::recode(perf_tbl$Method,
                                         "mst_or_sn"="MST_or_SN_Edit","embedKNN"="Embed_kNN",
                                         "comm_walktrap"="Comm_Walktrap","comm_infomap"="Comm_Infomap",
                                         "comm_fast_greedy"="Comm_FastGreedy","comm_label_prop"="Comm_LabelProp")
      }
    }
  }
  er_progress_tick(p, "Evaluation")

  if (!is.null(write_csv)) readr::write_csv(out, write_csv)
  er_progress_tick(p, ifelse(is.null(write_csv), "No CSV written", paste("Wrote CSV:", write_csv)))
  er_progress_done(p)

  list(
    performance = perf_tbl,
    predictions = out,
    tuning = list(
      kmeans_k = pick_k, hclust_k = pick_k_hc, pam_k = pick_k_pam,
      gc_best_threshold = gc_best_thr, gc_threshold_curve = gc_tune_tbl,
      kmeans_sil_curve = if (exists("km_sil_curve")) km_sil_curve else NULL,
      hclust_sil_curve = if (exists("hclust_sil_curve")) hclust_sil_curve else NULL,
      pam_sil_curve    = if (exists("pam_sil_curve")) pam_sil_curve else NULL
    ),
    details = list(
      n = nrow(df_text), has_embeddings = !is.null(E),
      params = list(
        k_clusters=k_clusters, knn_k=knn_k,
        mst_cut_ratio=mst_cut_ratio, mst_k=mst_k,
        sn_window=sn_window, sn_method=sn_method, sn_thresh=sn_thresh,
        svd_dim=svd_dim, louvain_min_sim=louvain_min_sim, cos_thresh=cos_thresh,
        gc_thresholds=gc_thresholds, gc_method=gc_method, gc_dist_method=gc_dist_method,
        auto_tune=auto_tune, tune_metric=tune_metric, k_grid=k_grid
      )
    )
  )
}


########################################
