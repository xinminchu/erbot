########################################

#' Locate clustering_agreement() (GCMER or sourced)
#' @keywords internal
#' @export
er_get_clustering_agreement_fun <- function(){
  if (exists("clustering_agreement", mode="function")) return(get("clustering_agreement"))
  if (requireNamespace("GCMER", quietly=TRUE) &&
      "clustering_agreement" %in% ls(asNamespace("GCMER"))) return(GCMER::clustering_agreement)
  stop("Need 'clustering_agreement()'. library(GCMER) or source('rcode.R').")
}

#' Locate resolve_entities() (GCMER or sourced)
#' @keywords internal
#' @export
er_get_resolve_entities_fun <- function(){
  if (exists("resolve_entities", mode="function")) return(get("resolve_entities"))
  if (requireNamespace("GCMER", quietly=TRUE) &&
      "resolve_entities" %in% ls(asNamespace("GCMER"))) return(GCMER::resolve_entities)
  stop("Need 'resolve_entities()' for Graph Coloring. library(GCMER) or source('rcode.R').")
}

#' Average silhouette from distance
#' @export
er_silhouette_avg <- function(labels, D){
  labs <- as.integer(factor(labels))
  if (length(unique(labs)) < 2) return(NA_real_)
  sil <- tryCatch(cluster::silhouette(as.integer(factor(labels)), dist = stats::as.dist(D)), error=function(e) NULL)
  if (is.null(sil)) return(NA_real_)
  mean(sil[,3], na.rm=TRUE)
}

#' Pairwise string distances in blocks (with optional progress)
#' @export
er_pairwise_stringdist <- function(text_vec, method="jw", block=4000, progress=NULL){
  n <- length(text_vec)
  if (n <= 1) return(matrix(0, n, n))
  if (inherits(progress, "er_progress")) {
    total_pairs <- n * (n - 1) / 2
    cat(sprintf("\nComputing pairwise distances [%s] for n=%d (~%.1fM pairs)\n",
                method, n, total_pairs / 1e6))
  }
  D <- matrix(0, n, n)
  total_pairs <- n * (n - 1) / 2
  done_pairs <- 0
  for (i in seq(1, n, by=block)) {
    i2 <- min(i+block-1, n); xi <- text_vec[i:i2]
    for (j in seq(i, n, by=block)) {
      j2 <- min(j+block-1, n); xj <- text_vec[j:j2]
      sub <- as.matrix(stringdist::stringdistmatrix(xi, xj, method=method))
      D[i:i2, j:j2] <- sub
      if (j > i) D[j:j2, i:i2] <- t(sub)
      new_pairs <- if (i==j) (i2 - i + 1) * (i2 - i) / 2 else (i2 - i + 1) * (j2 - j + 1)
      done_pairs <- done_pairs + new_pairs
      if (inherits(progress, "er_progress")) {
        pct <- 100 * done_pairs / max(1, total_pairs)
        cat(sprintf("\r Distance progress: %5.1f%%", pct)); flush.console()
      }
    }
  }
  if (inherits(progress, "er_progress")) cat("\n")
  D
}

#' Graph Coloring resolution from text via threshold sweep
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

#' Convert pairwise truth to clusters
#' @export
er_pairs_to_clusters <- function(truth_pairs, id1="id1", id2="id2"){
  stopifnot(all(c(id1,id2) %in% names(truth_pairs)))
  pairs <- truth_pairs %>% dplyr::transmute(id1=as.character(.data[[id1]]), id2=as.character(.data[[id2]])) %>%
    dplyr::filter(!is.na(id1), !is.na(id2), id1!="", id2!="", id1!=id2) %>%
    dplyr::mutate(a=pmin(id1,id2), b=pmax(id1,id2)) %>% dplyr::distinct(a,b,.keep_all=FALSE)
  if (!nrow(pairs)) return(tibble::tibble(id=character(), cluster_id=integer()))
  g <- igraph::graph_from_data_frame(pairs, directed=FALSE)
  memb <- igraph::components(g)$membership
  tibble::tibble(id=names(memb), cluster_id=as.integer(memb))
}

#' Truth ingestion from various formats
#' @export
er_truth_from_any <- function(truth, sep_pair="\\|",
                              id_candidates=c("id","record_id","rec_id","docid","rowid","paper_id")){
  if (is.null(truth)) return(NULL)
  if (is.vector(truth) && !is.null(names(truth))) return(tibble::tibble(id=as.character(names(truth)), cluster_id=as.integer(truth)))
  if (is.character(truth) && length(truth)==1L) {
    p <- truth; ext <- tolower(tools::file_ext(p))
    if (ext %in% c("csv","tsv","txt","psv")) truth <- readr::read_delim(p, delim = ifelse(ext=="tsv","\t", ","), show_col_types=FALSE, guess_max=1e6)
    else if (ext %in% c("xlsx","xls")) truth <- readxl::read_excel(p) else truth <- data.table::fread(p, showProgress = TRUE)
  }
  if (is.data.frame(truth)) {
    names(truth) <- tolower(names(truth))
    if (ncol(truth)==1L) {
      col <- names(truth)[1]
      pairs <- truth %>% dplyr::transmute(tmp=.data[[col]]) %>% dplyr::filter(!is.na(tmp), tmp!="") %>%
        tidyr::separate(tmp, c("id1","id2"), sep=sep_pair, remove=TRUE, fill="right", extra="drop")
      return(er_pairs_to_clusters(pairs,"id1","id2"))
    }
    if (all(c("id1","id2") %in% names(truth))) return(er_pairs_to_clusters(truth,"id1","id2"))
    id_truth <- intersect(names(truth), id_candidates); id_truth <- if (length(id_truth)) id_truth[1] else names(truth)[1]
    lab_col <- setdiff(names(truth), id_truth)[1]; stopifnot(!is.na(lab_col))
    return(truth %>% dplyr::transmute(id = as.character(.data[[id_truth]]), cluster_id = .data[[lab_col]]) %>% dplyr::distinct(id,.keep_all=TRUE))
  }
  stop("Unsupported truth type. Provide pair list, id+cluster table, named vector, or a path.")
}


########################################
