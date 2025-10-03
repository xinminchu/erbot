########################################

#' Locate resolve_entities() (GCMER or sourced)
#' @keywords internal
#' @return function
#' @export
er_get_resolve_entities_fun <- function(){
  if (exists("resolve_entities", mode="function")) return(get("resolve_entities"))
  if (requireNamespace("GCMER", quietly=TRUE) &&
      "resolve_entities" %in% ls(asNamespace("GCMER"))) return(GCMER::resolve_entities)
  stop("Need 'resolve_entities()' for Graph Coloring. library(GCMER) or source('rcode.R').")
}

#' Graph Coloring resolution from text via threshold sweep
#' @param text_vec character
#' @param thresholds numeric vector
#' @param gc_method one of 'lf','sl','rlf'
#' @param dist_method stringdist method
#' @param tune_metric metric to maximize if truth present
#' @param truth optional vector aligned to text (or named by id)
#' @param progress optional er_progress
#' @return list(labels, best_threshold, tuning, thresholds)
#' @export
er_gc_from_text <- function(text_vec, thresholds, gc_method=c("lf","sl","rlf"),
                            dist_method="jw", tune_metric="adj_rand", truth=NULL,
                            ca_metrics=c("chi2","rand","adj_rand","fowlkes_mallow","mirkin","jaccard","tpr","fpr","F_measure","meila_heckerman","max_match","van_dongen","mutual_info"),
                            progress=NULL, dist_block=4000){
  gc_method <- match.arg(gc_method)
  re_fun <- er_get_resolve_entities_fun()
  ca_fun <- er_get_clustering_agreement_fun()
  D <- er_pairwise_stringdist(text_vec, method=dist_method, block=dist_block, progress=progress)
  res <- re_fun(D, thresholds=thresholds, method=gc_method)
  ents <- res$ents; if (!is.matrix(ents)) ents <- as.matrix(ents); colnames(ents) <- paste0("thr_", thresholds)
  tuning <- NULL; best_idx <- 1L
  if (!is.null(truth)) {
    rows <- lapply(seq_along(thresholds), function(j){
      vec <- ca_fun(as.integer(ents[,j]), as.integer(truth), method=ca_metrics)
      tibble::as_tibble_row(as.list(vec)) |> dplyr::mutate(threshold = thresholds[j])
    })
    tuning <- dplyr::bind_rows(rows); stopifnot(tune_metric %in% names(tuning))
    best_idx <- order(tuning[[tune_metric]], decreasing=TRUE)[1]
  } else {
    sil <- vapply(seq_len(ncol(ents)), function(j) er_silhouette_avg(ents[,j], D), numeric(1))
    tuning <- tibble::tibble(threshold = thresholds, silhouette = sil)
    best_idx <- which.max(sil); if (!length(best_idx) || !is.finite(sil[best_idx])) best_idx <- 1L
  }
  list(labels=as.integer(ents[,best_idx]), best_threshold=thresholds[best_idx], tuning=tuning, thresholds=thresholds)
}


########################################
