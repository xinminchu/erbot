########################################

#' Locate clustering_agreement() (GCMER or sourced)
#' @keywords internal
#' @return function
#' @export
er_get_clustering_agreement_fun <- function(){
  if (exists("clustering_agreement", mode="function")) return(get("clustering_agreement"))
  if (requireNamespace("GCMER", quietly=TRUE) &&
      "clustering_agreement" %in% ls(asNamespace("GCMER"))) return(GCMER::clustering_agreement)
  stop("Need 'clustering_agreement()'. library(GCMER) or source('rcode.R').")
}

#' Evaluate one set of labels against truth using GCMER metrics
#' @param pred integer vector
#' @param truth integer vector
#' @param metrics character vector of metric names
#' @return tibble row with metrics
#' @export
er_eval_ca_one <- function(pred, truth, metrics=c(
  "chi2","rand","adj_rand","fowlkes_mallow","mirkin","jaccard",
  "tpr","fpr","F_measure","meila_heckerman","max_match","van_dongen","mutual_info"
)){
  fun <- er_get_clustering_agreement_fun()
  vec <- fun(as.integer(pred), as.integer(truth), method = metrics)
  tibble::as_tibble_row(as.list(vec))
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
