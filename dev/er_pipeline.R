# ==============================================================================
# Unified ER Pipeline (cora / affiliation / D10K / NC voters / etc.)
# Methods: KMeans, MST/SN+Edit, Louvain+kNN, Embed-kNN,
#          HC, PAM, GC(resolve_entities)
# Eval:    clustering_agreement() only (from GCMER)
# Tuning:  supervised by Clustering Agreement (CA, when truth),
#          else unsupervised by silhouette
# Plots:   base-R tuning curves + quick tables
# ==============================================================================


.er_require <- function(pkgs) {
  to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
  if (length(to_install)) install.packages(to_install, quiet = TRUE)
  suppressPackageStartupMessages(invisible(lapply(pkgs, require, character.only = TRUE)))
}
.er_require(c(
  "data.table","dplyr","tibble","stringr","stringi","tidyr","purrr",
  "readr","readxl","text2vec","Matrix","irlba","stringdist","igraph",
  "FNN","mclust","cluster","scales"
))
`%||%` <- function(a,b) if(!is.null(a)) a else b

# ------------------------- Hookups to GCMER / rcode
er_get_clustering_agreement_fun <- function(){
  if (exists("clustering_agreement", mode="function")) return(get("clustering_agreement"))
  if (requireNamespace("GCMER", quietly=TRUE) &&
      "clustering_agreement" %in% ls(asNamespace("GCMER"))) return(GCMER::clustering_agreement)
  stop("Need 'clustering_agreement()'. library(GCMER) or source('rcode.R').")
}
er_get_resolve_entities_fun <- function(){
  if (exists("resolve_entities", mode="function")) return(get("resolve_entities"))
  if (requireNamespace("GCMER", quietly=TRUE) &&
      "resolve_entities" %in% ls(asNamespace("GCMER"))) return(GCMER::resolve_entities)
  stop("Need 'resolve_entities()' for Graph Coloring. library(GCMER) or source('rcode.R').")
}
er_ca_metrics_default <- c(
  "chi2","rand","adj_rand","fowlkes_mallow","mirkin","jaccard",
  "tpr","fpr","F_measure","meila_heckerman","max_match","van_dongen","mutual_info"
)
er_eval_ca_one <- function(pred, truth, metrics=er_ca_metrics_default){
  fun <- er_get_clustering_agreement_fun()
  vec <- fun(as.integer(pred), as.integer(truth), method = metrics)
  tibble::as_tibble_row(as.list(vec))
}

# ------------------------- Progress utilities
er_progress_start <- function(total_steps, title="ER pipeline"){
  pb <- utils::txtProgressBar(min=0, max=total_steps, style=3)
  env <- list(pb=pb, total=total_steps, step=0L, title=title, t0=Sys.time(), last=Sys.time())
  class(env) <- "er_progress"
  cat(sprintf("\n[%s] %s — starting (%d steps)\n", format(env$t0, "%H:%M:%S"), title, total_steps))
  env
}
er_progress_tick <- function(p, label=NULL){
  if (!inherits(p, "er_progress")) return(invisible(NULL))
  p$step <- p$step + 1L
  utils::setTxtProgressBar(p$pb, p$step)
  now <- Sys.time()
  if (!is.null(label)) {
    cat(sprintf("\n[%s] Step %d/%d: %s (%.1fs)\n",
                format(now, "%H:%M:%S"), p$step, p$total, label,
                as.numeric(difftime(now, p$last, units="secs"))))
  }
  p$last <- now
  invisible(p)
}
er_progress_done <- function(p){
  if (!inherits(p, "er_progress")) return(invisible(NULL))
  close(p$pb)
  cat(sprintf("\n[%s] %s — done. Total elapsed: %.1fs\n\n",
              format(Sys.time(), "%H:%M:%S"), p$title,
              as.numeric(difftime(Sys.time(), p$t0, units="secs"))))
  invisible(NULL)
}

# ------------------------- Robust readers (incl. pipe fix)
er_read_pipe_or_fix <- function(path){
  DT <- tryCatch(data.table::fread(path, sep="|", quote="\"", header=TRUE, fill=TRUE, showProgress=FALSE), error=function(e) NULL)
  if (!is.null(DT) && ncol(DT) > 1) { DF <- as.data.frame(DT); names(DF) <- tolower(names(DF)); return(DF) }
  DF <- tryCatch(readr::read_delim(path, delim="|", quote='"', escape_double=TRUE, trim_ws=TRUE, show_col_types=FALSE), error=function(e) NULL)
  if (!is.null(DF) && ncol(DF) > 1) { DF <- as.data.frame(DF); names(DF) <- tolower(names(DF)); return(DF) }
  raw <- readr::read_lines(path); if (!length(raw)) stop("File empty: ", path)
  first <- raw[1]; sep <- if (grepl("\\|", first)) "\\|" else if (grepl("\t", first)) "\t" else if (grepl(";", first)) ";" else ","
  hdr <- strsplit(first, sep)[[1]]
  rows <- raw[-1]
  mat <- t(vapply(rows, function(x){
    parts <- strsplit(x, sep)[[1]]
    length(parts) <- length(hdr); parts[is.na(parts)] <- ""
    parts
  }, character(length(hdr))))
  DF <- as.data.frame(mat, stringsAsFactors=FALSE); names(DF) <- tolower(trimws(hdr)); DF
}
er_load_input <- function(data, sheet=NULL){
  if (is.data.frame(data)) { df <- tibble::as_tibble(data); names(df) <- tolower(names(df)); return(df) }
  if (is.character(data) && length(data) == 1L) {
    key <- tolower(data)
    if (key == "cora") {
      if (!requireNamespace("cora", quietly=TRUE)) stop("Package 'cora' not installed.")
      df <- tibble::as_tibble(get("cora", envir=asNamespace("cora"))); names(df) <- tolower(names(df)); return(df)
    }
    p <- data; ext <- tolower(tools::file_ext(p))
    if (ext %in% c("xlsx","xls")) df <- readxl::read_excel(p, sheet = sheet %||% 1L) %>% tibble::as_tibble()
    else {
      df <- tryCatch(data.table::fread(p, showProgress=TRUE) %>% tibble::as_tibble(), error=function(e) NULL)
      if (is.null(df)) df <- tibble::as_tibble(er_read_pipe_or_fix(p))
      if (ncol(df) == 1) df <- tibble::as_tibble(er_read_pipe_or_fix(p))
    }
    names(df) <- tolower(names(df)); return(df)
  }
  stop("Unsupported 'data'. Provide data.frame, path/URL, or 'cora'.")
}

# ------------------------- NCVR helpers
ncvr_read <- function(root, which=c("5","10","all")){
  which <- match.arg(which)
  all_csv <- list.files(root, pattern="\\.csv$", full.names=TRUE, recursive=TRUE)
  if (!length(all_csv)) stop("No CSVs found under: ", root)
  sel <- switch(which,
                "5" = all_csv[grepl("_nump_5\\.csv$", basename(all_csv))],
                "10"= all_csv[grepl("_nump_10\\.csv$", basename(all_csv))],
                "all" = all_csv
  )
  if (!length(sel)) stop("No files matched split '", which, "'.")
  DT <- data.table::rbindlist(lapply(sel, function(p) data.table::fread(p, showProgress=FALSE)), use.names=TRUE, fill=TRUE)
  df <- tibble::as_tibble(DT); names(df) <- tolower(names(df)); df
}
ncvr_guess_fields <- function(df){
  nms <- names(df)
  pick1 <- function(pats){
    hits <- unique(unlist(lapply(pats, function(p) grep(p, nms, perl=TRUE, value=TRUE))))
    if (length(hits)) hits[1] else NA_character_
  }
  first  <- pick1(c("^first(_|)name$", "^voter_?first_?name$", "^first$"))
  middle <- pick1(c("^middle(_|)name$", "^voter_?middle_?name$", "^middle$", "^mi(ddle)?_?name?$"))
  last   <- pick1(c("^last(_|)name$", "^voter_?last_?name$", "^surname$", "^last$"))
  street <- pick1(c("^res(idence)?_?street(_|)address$", "^res_?addr.*$", "^res_?street.*$", "^address(_1)?$"))
  city   <- pick1(c("^res(idence)?_?city(_|)(desc)?$", "^res_?city$", "^city(_desc)?$"))
  state  <- pick1(c("^res(idence)?_?state(_|)(cd|code)?$", "^res_?state$", "^state(_cd|_code)?$"))
  zip    <- pick1(c("^res(idence)?_?zip(_|)(code)?$", "^res_?zip$", "^zip(_code)?$"))
  fields <- c(first, middle, last, street, city, state, zip)
  fields <- fields[!is.na(fields)]
  if (!length(fields)) return(NULL)
  fields
}

# ------------------------- Field selection (+auto guess)
er_guess_text_fields <- function(df,
                                 id_candidates = c("id","affiliation_id","record_id","rec_id","docid","rowid","paper_id"),
                                 embed_candidates = c("embedded clean ag.value","embedded ag.value","emb","embedding","vector","embedding_clean"),
                                 max_fields = 5){
  nms <- tolower(names(df))
  is_char <- vapply(df, function(x) is.character(x) || is.factor(x), logical(1))
  cand <- setdiff(nms[is_char], c(id_candidates, embed_candidates, "cluster_id","label","class","y"))
  if (!length(cand)) return(character(0))
  score <- setNames(rep(0, length(cand)), cand)
  bump <- function(pat,w){score[grepl(pat,cand,perl=TRUE)]<<-score[grepl(pat,cand,perl=TRUE)]+w}
  bump("(title|name|string|text)$",3); bump("(affil|org|company|institution)",2)
  bump("(author|venue|journal|booktitle)",1.5); bump("(address|street|city|state|zip|country)",1.2)
  bump("(email|phone)",0.5)
  med_chars <- vapply(cand,function(col){x<-as.character(df[[col]]);x[is.na(x)]<-"";stats::median(nchar(x),na.rm=TRUE)},numeric(1))
  total <- score + scales::rescale(med_chars, to=c(0,1), from=range(med_chars, finite=TRUE))
  cand[order(total, decreasing=TRUE)][seq_len(min(max_fields, length(cand)))]
}
er_select_fields <- function(df,
                             id_col=NULL, fields=NULL, extra_fields=NULL,
                             id_candidates=c("id","affiliation_id","record_id","rec_id","docid","rowid","paper_id"),
                             text_candidates=c("title","name","raw_title","string","text","affiliation","aggregate value","clean ag.value"),
                             embed_candidates=c("embedded clean ag.value","embedded ag.value","emb","embedding","vector","embedding_clean"),
                             normalize=TRUE, auto_fields=TRUE, max_auto_fields=5){
  names(df) <- tolower(names(df))
  if (is.null(id_col)) {
    id_col <- intersect(id_candidates, names(df))[1]
    if (is.na(id_col)) { df$id <- as.character(seq_len(nrow(df))); id_col <- "id" }
  } else stopifnot(id_col %in% names(df))
  if (is.null(fields) || !length(fields)) {
    main <- intersect(text_candidates, names(df))[1]
    if (!is.na(main)) fields <- main else if (auto_fields) {
      fields <- er_guess_text_fields(df, id_candidates, embed_candidates, max_auto_fields)
      if (!length(fields)) stop("No usable text fields; pass fields= or rename columns. Available: ", paste(names(df), collapse=", "))
      message("Auto-selected fields: ", paste(fields, collapse=" | "))
    } else stop("No text fields. Supply fields= or ensure one of: ", paste(text_candidates, collapse=", "))
  } else {
    fields <- tolower(fields); fields <- fields[fields %in% names(df)]
    if (!length(fields)) stop("Requested fields not found. Available: ", paste(names(df), collapse=", "))
  }
  out <- df %>% dplyr::transmute(
    id = as.character(.data[[id_col]]),
    text_for_matching = stringr::str_squish(do.call(paste, c(.[fields], sep=" ")))
  )
  out$text_for_matching[is.na(out$text_for_matching)|out$text_for_matching==""] <- " "
  if (normalize) out$text_for_matching <- out$text_for_matching %>%
    stringi::stri_trans_nfkc() %>% tolower() %>% stringr::str_replace_all("\\s+"," ") %>% stringr::str_trim()
  out
}

# ------------------------- Embeddings + Features
er_safe_parse_embedding_col <- function(x){
  x <- as.character(x); x[is.na(x)] <- ""
  num_pat <- "[-+]?(?:\\d*\\.\\d+|\\d+)(?:[eE][-+]?\\d+)?"
  lst <- regmatches(x, gregexpr(num_pat, x, perl=TRUE))
  lens <- lengths(lst); if (all(lens==0L)) stop("No numeric tokens found in embedding col.")
  tab <- sort(table(lens[lens>0L]), decreasing=TRUE); d <- if (length(tab)) as.integer(names(tab)[1]) else max(lens)
  m <- matrix(NA_real_, nrow=length(lst), ncol=d)
  for (i in seq_along(lst)) { v <- suppressWarnings(as.numeric(lst[[i]])); if (length(v)) m[i, seq_len(min(length(v), d))] <- v[seq_len(min(length(v), d))] }
  storage.mode(m) <- "double"; m
}
er_features_tfidf_svd <- function(text_vec, svd_dim=100){
  it <- text2vec::itoken(text_vec, tokenizer=text2vec::word_tokenizer, progressbar=FALSE)
  vocab <- text2vec::create_vocabulary(it)
  vec <- text2vec::vocab_vectorizer(vocab)
  dtm <- text2vec::create_dtm(it, vec)
  tfidf <- text2vec::TfIdf$new(); Xtf <- tfidf$fit_transform(dtm)
  k_dim <- max(2L, min(svd_dim, min(dim(Xtf))-1L))
  set.seed(42); svd_res <- irlba::irlba(Xtf, nv=k_dim)
  svd_res$u %*% diag(svd_res$d)
}

# ------------------------- Core methods
er_kmeans_from_X <- function(X, k=10, seed=123){ set.seed(seed); stats::kmeans(X, centers=k, nstart=20)$cluster }
er_cosine_dist   <- function(X){ X<-as.matrix(X); nr<-sqrt(rowSums(X^2)); nr[nr==0]<-1; X<-X/nr; D<-1-(X%*%t(X)); pmax(D,0) }

er_mst_or_sn_edit <- function(text_vec, mst_cut_ratio=5, mst_k=NULL, sn_window=40, sn_method="jw", sn_thresh=0.12){
  n <- length(text_vec); if (n < 2) return(rep(1L, n))
  if (n <= 3000) {
    D <- stringdist::stringdistmatrix(text_vec, text_vec, method="lv"); D <- as.matrix(D); diag(D) <- 0
    g <- igraph::graph_from_adjacency_matrix(D, mode="undirected", weighted=TRUE, diag=FALSE)
    mst <- igraph::mst(g, weights = igraph::E(g)$weight)
    if (is.null(mst_k)) mst_k <- max(2L, floor(n / mst_cut_ratio))
    ord <- order(igraph::E(mst)$weight, decreasing=TRUE)
    cut_e <- igraph::E(mst)[ord][seq_len(min(mst_k-1L, length(ord)))]
    g2 <- if (length(cut_e)) igraph::delete_edges(mst, cut_e) else mst
    return(igraph::components(g2)$membership)
  }
  key <- text_vec %>% stringi::stri_trans_nfkc() %>% tolower() %>% stringr::str_replace_all("\\s+"," ") %>% stringr::str_trim()
  ord <- order(key); edges_from <- integer(0); edges_to <- integer(0)
  for (i in seq_len(n)) {
    idx_i <- ord[i]; j_end <- min(n, i + sn_window); if (j_end <= i) next
    idx_j <- ord[(i+1):j_end]; d <- stringdist::stringdist(text_vec[idx_i], text_vec[idx_j], method = sn_method)
    keep <- is.finite(d) & d <= sn_thresh
    if (any(keep)) { edges_from <- c(edges_from, rep.int(idx_i, sum(keep))); edges_to <- c(edges_to, idx_j[keep]) }
  }
  vdf <- data.frame(name = as.character(seq_len(n)))
  if (length(edges_from)) {
    edf <- data.frame(from = as.character(edges_from), to = as.character(edges_to))
    g <- igraph::graph_from_data_frame(edf, directed=FALSE, vertices=vdf) |> igraph::simplify()
  } else { g <- igraph::make_empty_graph(n = n); igraph::V(g)$name <- as.character(seq_len(n)) }
  as.integer(igraph::components(g)$membership)
}

er_louvain_knn <- function(X, knn=10, min_sim=0.0){
  rs <- sqrt(rowSums(X^2)); rs[rs==0] <- 1; Xn <- X/rs
  knn_use <- max(1L, min(knn, nrow(Xn)-1L))
  nn <- FNN::get.knn(Xn, k=knn_use)
  sims <- vapply(seq_len(nrow(Xn)), function(i) as.numeric(Xn[i,,drop=FALSE] %*% t(Xn[nn$nn.index[i,],,drop=FALSE])), numeric(knn_use))
  keep <- sims >= min_sim
  edf <- cbind(from = rep(seq_len(nrow(Xn)), each=knn_use)[keep], to = as.vector(nn$nn.index)[keep])
  if (!length(edf)) return(list(labels=rep(1L, nrow(Xn)), graph=igraph::make_empty_graph(nrow(Xn))))
  g <- igraph::graph_from_edgelist(matrix(edf, ncol=2), directed=FALSE) |> igraph::simplify()
  list(labels = igraph::membership(igraph::cluster_louvain(g)), graph = g)
}

er_embed_knn <- function(emb_mat, k=15, cos_thresh=0.88){
  if (!is.matrix(emb_mat)) emb_mat <- as.matrix(emb_mat); storage.mode(emb_mat) <- "double"
  n <- nrow(emb_mat); if (n < 2) return(rep(1L, n))
  nr <- sqrt(rowSums(emb_mat^2, na.rm=TRUE)); valid <- is.finite(nr) & nr > 0
  out <- seq_len(n); if (sum(valid) < 2) return(out)
  X <- emb_mat[valid,,drop=FALSE]; X <- X / nr[valid]; k_eff <- max(1L, min(k, nrow(X)-1L))
  knn <- FNN::get.knn(X, k=k_eff)
  edges <- vector("list", nrow(X))
  for (i in seq_len(nrow(X))) {
    idx <- knn$nn.index[i,]; sims_i <- as.numeric(X[i,,drop=FALSE] %*% t(X[idx,,drop=FALSE]))
    keep <- is.finite(sims_i) & sims_i >= cos_thresh
    if (any(keep)) edges[[i]] <- cbind(i, idx[keep])
  }
  edges <- do.call(rbind, edges); verts <- data.frame(name = as.character(seq_len(nrow(X))))
  if (!is.null(edges) && nrow(edges) > 0) {
    edf <- data.frame(from = as.character(edges[,1]), to = as.character(edges[,2]))
    g <- igraph::graph_from_data_frame(edf, directed=FALSE, vertices=verts) |> igraph::simplify()
  } else { g <- igraph::make_empty_graph(n = nrow(X)); igraph::V(g)$name <- as.character(seq_len(nrow(X))) }
  memb_valid <- igraph::components(g)$membership; out[valid] <- as.integer(memb_valid); out
}

er_hclust_from_X <- function(X, k=10, method="ward.D2"){
  D <- er_cosine_dist(X); hc <- stats::hclust(stats::as.dist(D), method=method); stats::cutree(hc, k=k)
}
er_pam_from_X <- function(X, k=10){
  D <- er_cosine_dist(X); cluster::pam(stats::as.dist(D), k=k)$clustering
}

# ------------------------- Silhouette helpers
er_silhouette_avg <- function(labels, D){
  labs <- as.integer(factor(labels))
  if (length(unique(labs)) < 2) return(NA_real_)
  sil <- tryCatch(cluster::silhouette(as.integer(factor(labels)), dist = stats::as.dist(D)), error=function(e) NULL)
  if (is.null(sil)) return(NA_real_)
  mean(sil[,3], na.rm=TRUE)
}

# ------------------------- Pairwise distance with progress (block-wise)
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

# ------------------------- Graph Coloring via resolve_entities (with tuning)
er_gc_from_text <- function(text_vec, thresholds, gc_method=c("lf","sl","rlf"),
                            dist_method="jw", tune_metric="adj_rand", truth=NULL,
                            ca_metrics=er_ca_metrics_default, progress=NULL, dist_block=4000){
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

# ------------------------- Truth ingestion
er_pairs_to_clusters <- function(truth_pairs, id1="id1", id2="id2"){
  stopifnot(all(c(id1,id2) %in% names(truth_pairs)))
  pairs <- truth_pairs %>% transmute(id1=as.character(.data[[id1]]), id2=as.character(.data[[id2]])) %>%
    filter(!is.na(id1), !is.na(id2), id1!="", id2!="", id1!=id2) %>%
    mutate(a=pmin(id1,id2), b=pmax(id1,id2)) %>% distinct(a,b,.keep_all=FALSE)
  if (!nrow(pairs)) return(tibble(id=character(), cluster_id=integer()))
  g <- igraph::graph_from_data_frame(pairs, directed=FALSE)
  memb <- igraph::components(g)$membership
  tibble(id=names(memb), cluster_id=as.integer(memb))
}
er_truth_from_any <- function(truth, sep_pair="\\|",
                              id_candidates=c("id","record_id","rec_id","docid","rowid","paper_id")){
  if (is.null(truth)) return(NULL)
  if (is.vector(truth) && !is.null(names(truth))) return(tibble(id=as.character(names(truth)), cluster_id=as.integer(truth)))
  if (is.character(truth) && length(truth)==1L) {
    p <- truth; ext <- tolower(tools::file_ext(p))
    if (ext %in% c("csv","tsv","txt","psv")) truth <- readr::read_delim(p, delim = ifelse(ext=="tsv","\t", ","), show_col_types=FALSE, guess_max=1e6)
    else if (ext %in% c("xlsx","xls")) truth <- readxl::read_excel(p) else truth <- data.table::fread(p, showProgress = TRUE)
  }
  if (is.data.frame(truth)) {
    names(truth) <- tolower(names(truth))
    if (ncol(truth)==1L) {
      col <- names(truth)[1]
      pairs <- truth %>% transmute(tmp=.data[[col]]) %>% filter(!is.na(tmp), tmp!="") %>%
        tidyr::separate(tmp, c("id1","id2"), sep=sep_pair, remove=TRUE, fill="right", extra="drop")
      return(er_pairs_to_clusters(pairs,"id1","id2"))
    }
    if (all(c("id1","id2") %in% names(truth))) return(er_pairs_to_clusters(truth,"id1","id2"))
    id_truth <- intersect(names(truth), id_candidates); id_truth <- if (length(id_truth)) id_truth[1] else names(truth)[1]
    lab_col <- setdiff(names(truth), id_truth)[1]; stopifnot(!is.na(lab_col))
    return(truth %>% transmute(id = as.character(.data[[id_truth]]), cluster_id = .data[[lab_col]]) %>% distinct(id,.keep_all=TRUE))
  }
  stop("Unsupported truth type. Provide pair list, id+cluster table, named vector, or a path.")
}

# ------------------------- Plot & Report helpers
er_plot_curve <- function(df, x_col, y_col, main="", xlab=NULL, ylab=NULL){
  if (is.null(df) || !all(c(x_col,y_col) %in% names(df))) return(invisible(FALSE))
  x <- df[[x_col]]; y <- df[[y_col]]; if (all(!is.finite(y))) return(invisible(FALSE))
  xlab <- xlab %||% x_col; ylab <- ylab %||% y_col
  plot(x,y,type="b",pch=16,main=main,xlab=xlab,ylab=ylab); abline(h=max(y[is.finite(y)]), lty=3, col="gray50")
  invisible(TRUE)
}
er_pick_gc_metric <- function(gc_curve, fallback_metric="adj_rand"){
  if (is.null(gc_curve)) return(NULL)
  if ("silhouette" %in% names(gc_curve)) return("silhouette")
  if (fallback_metric %in% names(gc_curve)) return(fallback_metric)
  for (m in c("adj_rand","rand","mutual_info","F_measure","jaccard")) if (m %in% names(gc_curve)) return(m)
  NULL
}
er_autoplot_tuning <- function(res, metric=NULL){
  tun <- res$tuning; if (is.null(tun)) return(invisible(FALSE))
  metric <- metric %||% (res$details$params$tune_metric %||% "adj_rand")
  gc_metric <- er_pick_gc_metric(tun$gc_threshold_curve, fallback_metric=metric)
  panels <- sum(!sapply(list(tun$kmeans_sil_curve,tun$hclust_sil_curve,tun$pam_sil_curve,tun$gc_threshold_curve), is.null))
  if (!panels) return(invisible(FALSE))
  nrow <- if (panels <= 2) 1 else 2; ncol <- ceiling(panels / nrow)
  old <- par(mfrow=c(nrow,ncol), mar=c(4,4,3,1)); on.exit(par(old), add=TRUE)
  if (!is.null(tun$kmeans_sil_curve)) er_plot_curve(tun$kmeans_sil_curve,"k","silhouette","KMeans: silhouette vs k")
  if (!is.null(tun$hclust_sil_curve)) er_plot_curve(tun$hclust_sil_curve,"k","silhouette","HC (Ward.D2): silhouette vs k")
  if (!is.null(tun$pam_sil_curve))    er_plot_curve(tun$pam_sil_curve,"k","silhouette","PAM: silhouette vs k")
  if (!is.null(tun$gc_threshold_curve) && !is.null(gc_metric))
    er_plot_curve(tun$gc_threshold_curve,"threshold",gc_metric,sprintf("GC: %s vs threshold",gc_metric),"threshold",gc_metric)
  invisible(TRUE)
}

# Replace your er_draw_table() with this safer version
er_draw_table <- function(df, title = NULL, base_size = 8, max_rows_text = 60) {
  # Coerce all columns to plain character (no lists, no fancy types)
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


# Replace your er_params_df() with this version
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


er_gc_top_table <- function(res, top_n=5, metric=NULL){
  gc_curve <- res$tuning$gc_threshold_curve; if (is.null(gc_curve)) return(NULL)
  metric <- er_pick_gc_metric(gc_curve, fallback_metric = (metric %||% res$details$params$tune_metric %||% "adj_rand"))
  ord <- order(gc_curve[[metric]], decreasing=TRUE)
  gc_curve[ord, c("threshold", metric), drop=FALSE] |> utils::head(top_n)
}
er_save_report_pdf <- function(res, file="er_report.pdf", dataset_name=NULL, top_n=5, metric=NULL, width=11, height=8.5){
  pdf(file=file, width=width, height=height, onefile=TRUE)
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
  dev.off(); invisible(file)
}

# --- ADD/REPLACE: unified pipeline with er_methods filter ---------------------
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
    show_progress=TRUE,
    # NEW: choose methods to run. "all" runs everything.
    er_methods=c("all")  # e.g., "kmeans" or c("kmeans","louvain")
){
  eval_mode <- match.arg(eval_mode)
  gc_method <- match.arg(gc_method)

  # Normalize er_methods
  all_methods <- c("kmeans","mstsn","louvain","embedknn","hc","pam","gc","comm")
  if (length(er_methods)==1 && er_methods[1]=="all") er_methods <- all_methods
  er_methods <- intersect(tolower(er_methods), all_methods)

  run_kmeans   <- "kmeans"   %in% er_methods
  run_mstsn    <- "mstsn"    %in% er_methods
  run_louvain  <- "louvain"  %in% er_methods
  run_embedknn <- "embedknn" %in% er_methods
  run_hc       <- "hc"       %in% er_methods
  run_pam      <- "pam"      %in% er_methods
  run_gc       <- "gc"       %in% er_methods
  run_comm     <- "comm"     %in% er_methods

  # progress setup (only count what will run)
  flags <- c(load=1, select=1, tfidf=1, embed=1,
             kmeans=as.integer(run_kmeans),
             mstsn=as.integer(run_mstsn),
             louvain=as.integer(run_louvain),
             embknn=as.integer(run_embedknn),
             hc=as.integer(run_hc),
             pam=as.integer(run_pam),
             gc=as.integer(run_gc),
             comm=as.integer(run_comm),
             eval=1, write=1)
  p <- if (isTRUE(show_progress)) er_progress_start(sum(flags), "ER unified pipeline") else NULL

  # 1) Load + text
  df_raw  <- er_load_input(data, sheet = sheet);                                       er_progress_tick(p, "Loaded input")
  df_text <- er_select_fields(df_raw, id_col=id_col, fields=fields, extra_fields=extra_fields,
                              id_candidates=id_candidates, text_candidates=text_candidates,
                              normalize=TRUE, auto_fields=TRUE);                       er_progress_tick(p, "Selected fields")

  # 2) Features + optional embeddings
  Xsvd <- er_features_tfidf_svd(df_text$text_for_matching, svd_dim=svd_dim);          er_progress_tick(p, "Built TF-IDF + SVD")
  if (is.null(embed_col)) ec <- intersect(tolower(embed_candidates), names(df_raw))[1] else ec <- tolower(embed_col)
  E <- if (!is.na(ec) && !is.null(ec) && ec %in% names(df_raw)) er_safe_parse_embedding_col(df_raw[[ec]]) else NULL
  er_progress_tick(p, sprintf("Parsed embeddings: %s", ifelse(is.null(E), "no", "yes")))

  # Prepare holders
  pred_kmeans <- pred_mst_sn <- pred_louvain <- pred_hclust <- pred_pam <- pred_embed <- pred_gc <- NULL
  g_knn <- NULL
  km_sil_curve <- hclust_sil_curve <- pam_sil_curve <- NULL
  pick_k <- k_clusters; pick_k_hc <- k_clusters; pick_k_pam <- k_clusters
  gc_best_thr <- NA; gc_tune_tbl <- NULL

  # ---- KMeans (tuned)
  if (run_kmeans) {
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
        km_sil_curve <- dplyr::bind_rows(km_rows)
        pick_k <- km_sil_curve$k[which.max(km_sil_curve$silhouette %||% NA_real_)]
        if (!length(pick_k)) pick_k <- k_clusters
      }
    }
    pred_kmeans <- er_kmeans_from_X(Xsvd, k=pick_k)
    er_progress_tick(p, sprintf("KMeans (k=%d)", pick_k))
  }

  # ---- MST/SN+edit
  if (run_mstsn) {
    pred_mst_sn <- er_mst_or_sn_edit(df_text$text_for_matching, mst_cut_ratio=mst_cut_ratio, mst_k=mst_k,
                                     sn_window=sn_window, sn_method=sn_method, sn_thresh=sn_thresh)
    er_progress_tick(p, "MST/SN+edit")
  }

  # ---- Louvain on kNN
  if (run_louvain) {
    lv <- er_louvain_knn(Xsvd, knn=knn_k, min_sim=louvain_min_sim)
    pred_louvain <- lv$labels; g_knn <- lv$graph
    er_progress_tick(p, sprintf("Louvain kNN (k=%d)", knn_k))
  }

  # ---- Embed-kNN
  if (run_embedknn) {
    pred_embed <- if (!is.null(E)) er_embed_knn(E, k=knn_k, cos_thresh=cos_thresh) else NULL
    er_progress_tick(p, sprintf("Embed-kNN: %s", ifelse(is.null(pred_embed), "skipped", "done")))
  }

  # ---- HC (tuned)
  if (run_hc) {
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
        hclust_sil_curve <- dplyr::bind_rows(rows)
        pick_k_hc <- hclust_sil_curve$k[which.max(hclust_sil_curve$silhouette %||% NA_real_)]
        if (!length(pick_k_hc)) pick_k_hc <- k_clusters
      }
    }
    pred_hclust <- er_hclust_from_X(Xsvd, k=pick_k_hc)
    er_progress_tick(p, sprintf("HC (k=%d)", pick_k_hc))
  }

  # ---- PAM (tuned)
  if (run_pam) {
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
        pam_sil_curve <- dplyr::bind_rows(rows)
        pick_k_pam <- pam_sil_curve$k[which.max(pam_sil_curve$silhouette %||% NA_real_)]
        if (!length(pick_k_pam)) pick_k_pam <- k_clusters
      }
    }
    pred_pam <- er_pam_from_X(Xsvd, k=pick_k_pam)
    er_progress_tick(p, sprintf("PAM (k=%d)", pick_k_pam))
  }

  # ---- Graph Coloring (threshold sweep)
  if (run_gc && nrow(df_text) >= 2) {
    truth_vec <- if (!is.null(truth)) { tt <- er_truth_from_any(truth); setNames(tt$cluster_id, tt$id)[df_text$id] } else NULL
    gc_res <- er_gc_from_text(df_text$text_for_matching, thresholds=gc_thresholds, gc_method=gc_method,
                              dist_method=gc_dist_method, tune_metric=tune_metric, truth=truth_vec,
                              ca_metrics=er_ca_metrics_default, progress=p, dist_block=4000)
    pred_gc <- gc_res$labels; gc_best_thr <- gc_res$best_threshold; gc_tune_tbl <- gc_res$tuning
  }
  er_progress_tick(p, "Graph Coloring")

  # ---- Extra communities on kNN graph
  extra_preds <- list()
  if (run_comm && !is.null(g_knn)) {
    for (m in run_comm_methods) {
      labs <- tryCatch({
        if (igraph::ecount(g_knn) == 0) rep(1L, nrow(df_text)) else switch(m,
                                                                           "walktrap"    = igraph::membership(igraph::cluster_walktrap(g_knn)),
                                                                           "infomap"     = igraph::membership(igraph::cluster_infomap(g_knn)),
                                                                           "fast_greedy" = igraph::membership(igraph::cluster_fast_greedy(g_knn)),
                                                                           "label_prop"  = igraph::membership(igraph::cluster_label_prop(g_knn)),
                                                                           rep(1L, nrow(df_text)))
      }, error=function(e) rep(1L, nrow(df_text)))
      extra_preds[[paste0("pred_comm_", m)]] <- as.integer(labs)
    }
  }
  er_progress_tick(p, "Extra communities")

  # 5) Predictions table (only include what ran)
  out <- tibble::tibble(id = df_text$id, text_for_matching = df_text$text_for_matching)
  if (!is.null(pred_kmeans))   out$pred_kmeans    <- as.integer(pred_kmeans)
  if (!is.null(pred_mst_sn))   out$pred_mst_or_sn <- as.integer(pred_mst_sn)
  if (!is.null(pred_louvain))  out$pred_louvain   <- as.integer(pred_louvain)
  if (!is.null(pred_hclust))   out$pred_hclust    <- as.integer(pred_hclust)
  if (!is.null(pred_pam))      out$pred_pam       <- as.integer(pred_pam)
  if (!is.null(pred_embed))    out$pred_embedKNN  <- as.integer(pred_embed)
  if (!is.null(pred_gc))       out$pred_gc        <- as.integer(pred_gc)
  if (length(extra_preds))     out <- dplyr::bind_cols(out, tibble::as_tibble(extra_preds))

  # 6) Evaluation (unchanged)
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
        auto_tune=auto_tune, tune_metric=tune_metric, k_grid=k_grid,
        er_methods=er_methods
      )
    )
  )
}

# --- ADD/REPLACE: facade that forwards er_methods --------------------------------
er_main <- function(
    data, truth=NULL, fields=NULL, extra_fields=NULL, id_col=NULL, embed_col=NULL,
    k_clusters=10, knn_k=15, mst_cut_ratio=5, mst_k=NULL, sn_window=40, sn_method="jw", sn_thresh=0.12,
    svd_dim=100, louvain_min_sim=0.0, cos_thresh=0.88,
    gc_thresholds=seq(0.06,0.20,0.02), gc_method="rlf", gc_dist_method="jw",
    auto_tune=TRUE, tune_metric="adj_rand", k_grid=c(5,10,15,20),
    eval_mode="labeled_only", write_csv=NULL, sheet=NULL,
    run_comm_methods=c("walktrap","infomap","fast_greedy","label_prop"),
    auto_plot=TRUE, show_tables=TRUE, show_progress=TRUE,
    er_methods=c("all")
){
  res <- er_unified_pipeline(
    data=data, truth=truth, sheet=sheet, id_col=id_col, fields=fields, extra_fields=extra_fields,
    embed_col=embed_col, k_clusters=k_clusters, knn_k=knn_k,
    mst_cut_ratio=mst_cut_ratio, mst_k=mst_k, sn_window=sn_window, sn_method=sn_method, sn_thresh=sn_thresh,
    svd_dim=svd_dim, louvain_min_sim=louvain_min_sim, cos_thresh=cos_thresh,
    gc_thresholds=gc_thresholds, gc_method=gc_method, gc_dist_method=gc_dist_method,
    auto_tune=auto_tune, tune_metric=tune_metric, k_grid=k_grid,
    eval_mode=eval_mode, write_csv=write_csv, run_comm_methods=run_comm_methods,
    show_progress=show_progress,
    er_methods=er_methods
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


# ============================
#  A) Internal-quality toolkit
# ============================

# Calinski–Harabasz and Davies–Bouldin (on Euclidean geometry of X)
er_ch_db <- function(X, labels) {
  labs <- as.integer(factor(labels))
  k <- length(unique(labs))
  n <- nrow(X)
  if (k < 2 || k >= n) return(list(ch = NA_real_, db = NA_real_))
  # centroids
  centers <- rowsum(X, labs) / as.vector(table(labs))
  # WSS
  W <- 0
  for (g in unique(labs)) {
    Xi <- X[labs == g, , drop = FALSE]
    cg <- centers[g, , drop = FALSE]
    W <- W + sum(rowSums((Xi - matrix(cg, nrow = nrow(Xi), ncol = ncol(Xi), byrow = TRUE))^2))
  }
  # TSS & BSS
  mu <- matrix(colMeans(X), nrow = n, ncol = ncol(X), byrow = TRUE)
  T <- sum(rowSums((X - mu)^2))
  B <- T - W
  ch <- (B / (k - 1)) / (W / (n - k))

  # Davies–Bouldin
  # cluster scatter s_i = avg distance of members to centroid (euclidean)
  s <- numeric(k)
  for (g in seq_len(k)) {
    Xi <- X[labs == g, , drop = FALSE]
    cg <- centers[g, , drop = FALSE]
    di <- sqrt(rowSums((Xi - matrix(cg, nrow = nrow(Xi), ncol = ncol(Xi), byrow = TRUE))^2))
    s[g] <- mean(di)
  }
  # centroid distances
  Cdist <- as.matrix(dist(centers))
  diag(Cdist) <- Inf
  R <- outer(seq_len(k), seq_len(k), Vectorize(function(i, j) (s[i] + s[j]) / Cdist[i, j]))
  db <- mean(apply(R, 1, max), na.rm = TRUE)
  list(ch = as.numeric(ch), db = as.numeric(db))
}

# Average silhouette ( ↑ ), given a precomputed distance matrix (dist object or full matrix)
er_silhouette_avg_fromD <- function(labels, D) {
  labs <- as.integer(factor(labels))
  if (length(unique(labs)) < 2) return(NA_real_)
  if (!inherits(D, "dist")) D <- stats::as.dist(D)
  sil <- tryCatch(cluster::silhouette(labs, dist = D), error = function(e) NULL)
  if (is.null(sil)) return(NA_real_)
  mean(sil[, 3], na.rm = TRUE)
}

# Modularity on a graph ( ↑ )
er_modularity_safe <- function(g, labels) {
  if (inherits(g, "igraph") && igraph::vcount(g) == length(labels) && igraph::ecount(g) > 0) {
    return(as.numeric(igraph::modularity(g, membership = as.integer(factor(labels)))))
  }
  NA_real_
}

# Rank-mix aggregator (lower is better). You can change weights.
# Larger-better metrics: silhouette, ch, modularity.
# Smaller-better metrics: db.
er_rank_mix <- function(df, weights = c(silhouette = 1, ch = 0.5, db = 0.5, modularity = 1)) {
  # Keep only available metrics
  mets <- intersect(names(weights), names(df))
  if (!length(mets)) return(rep(NA_real_, nrow(df)))
  Z <- matrix(0, nrow = nrow(df), ncol = length(mets))
  colnames(Z) <- mets
  for (m in mets) {
    v <- df[[m]]
    if (all(is.na(v))) { Z[, m] <- NA_real_; next }
    # rank: larger better except DB
    if (m == "db") {
      r <- rank(v, na.last = "keep", ties.method = "average")           # smaller better
    } else {
      r <- rank(-v, na.last = "keep", ties.method = "average")          # larger better
    }
    # z-normalize ranks to [0,1]
    r_min <- min(r, na.rm = TRUE); r_max <- max(r, na.rm = TRUE)
    z <- if (r_max > r_min) (r - r_min) / (r_max - r_min) else rep(0.5, length(r))
    Z[, m] <- z * weights[m]
  }
  rowSums(Z, na.rm = TRUE)
}

# Small helper to choose a best row by objective
er_choose_best <- function(curve, objective = c("silhouette","ch","db","modularity","ranked_mix"),
                           weights = c(silhouette = 1, ch = 0.5, db = 0.5, modularity = 1)) {
  objective <- match.arg(objective)
  if (!nrow(curve)) return(list(best_row = NULL, curve = curve))
  if (objective == "ranked_mix") {
    curve$rank_mix <- er_rank_mix(curve, weights = weights)
    best_idx <- which.min(curve$rank_mix)
  } else if (objective == "db") {
    best_idx <- which.min(curve$db)
  } else { # silhouette / ch / modularity
    best_idx <- which.max(curve[[objective]])
  }
  list(best_row = curve[best_idx, , drop = FALSE], curve = curve)
}

# ============================
#  B) Per-method grid tuners
# ============================

# KMeans (TF-IDF+SVD space)
er_tune_kmeans_internal <- function(Xsvd, k_grid = c(5,10,15,20), objective = "silhouette", Dcos = NULL) {
  if (is.null(Dcos)) Dcos <- er_cosine_dist(Xsvd)
  rows <- lapply(k_grid, function(k) {
    labs <- er_kmeans_from_X(Xsvd, k = k)
    sil  <- er_silhouette_avg_fromD(labs, Dcos)
    chdb <- er_ch_db(Xsvd, labs)
    tibble::tibble(method = "kmeans", k = k, silhouette = sil, ch = chdb$ch, db = chdb$db, k_clusters = length(unique(labs)))
  })
  curve <- dplyr::bind_rows(rows)
  er_choose_best(curve, objective = objective)
}

# Hierarchical (Ward.D2)
er_tune_hclust_internal <- function(Xsvd, k_grid = c(5,10,15,20), objective = "silhouette", Dcos = NULL) {
  if (is.null(Dcos)) Dcos <- er_cosine_dist(Xsvd)
  rows <- lapply(k_grid, function(k) {
    labs <- er_hclust_from_X(Xsvd, k = k)
    sil  <- er_silhouette_avg_fromD(labs, Dcos)
    chdb <- er_ch_db(Xsvd, labs)
    tibble::tibble(method = "hclust", k = k, silhouette = sil, ch = chdb$ch, db = chdb$db, k_clusters = length(unique(labs)))
  })
  curve <- dplyr::bind_rows(rows)
  er_choose_best(curve, objective = objective)
}

# PAM
er_tune_pam_internal <- function(Xsvd, k_grid = c(5,10,15,20), objective = "silhouette", Dcos = NULL) {
  if (is.null(Dcos)) Dcos <- er_cosine_dist(Xsvd)
  rows <- lapply(k_grid, function(k) {
    labs <- er_pam_from_X(Xsvd, k = k)
    sil  <- er_silhouette_avg_fromD(labs, Dcos)
    chdb <- er_ch_db(Xsvd, labs)
    tibble::tibble(method = "pam", k = k, silhouette = sil, ch = chdb$ch, db = chdb$db, k_clusters = length(unique(labs)))
  })
  curve <- dplyr::bind_rows(rows)
  er_choose_best(curve, objective = objective)
}

# Louvain on kNN (graph-based)
er_tune_louvain_internal <- function(Xsvd,
                                     knn_grid = c(10,15,20,25),
                                     min_sim_grid = c(0.0, 0.05, 0.1),
                                     objective = "modularity",
                                     Dcos = NULL) {
  if (is.null(Dcos)) Dcos <- er_cosine_dist(Xsvd)
  rows <- list()
  for (k in knn_grid) for (ms in min_sim_grid) {
    lv <- er_louvain_knn(Xsvd, knn = k, min_sim = ms)
    labs <- lv$labels
    sil  <- er_silhouette_avg_fromD(labs, Dcos)
    mod  <- er_modularity_safe(lv$graph, labs)
    rows[[length(rows) + 1]] <- tibble::tibble(method = "louvain_knn", knn_k = k, min_sim = ms,
                                               silhouette = sil, modularity = mod,
                                               k_clusters = length(unique(labs)))
  }
  curve <- dplyr::bind_rows(rows)
  er_choose_best(curve, objective = objective)
}

# Embed-kNN (if embeddings present)
er_tune_embed_knn_internal <- function(E,
                                       k_grid = c(10,15,20),
                                       cos_grid = c(0.85,0.88,0.90),
                                       objective = "modularity",
                                       Xsvd = NULL, Dcos = NULL) {
  # silhouette evaluated on SVD/cosine if provided (optional but useful)
  if (is.null(Dcos) && !is.null(Xsvd)) Dcos <- er_cosine_dist(Xsvd)
  rows <- list()
  for (k in k_grid) for (ct in cos_grid) {
    labs <- er_embed_knn(E, k = k, cos_thresh = ct)
    sil  <- if (!is.null(Dcos)) er_silhouette_avg_fromD(labs, Dcos) else NA_real_
    # Build small graph from embeddings for modularity (reuse knn edges)
    # We re-run the graph build part of er_embed_knn():
    emb <- as.matrix(E); nr <- sqrt(rowSums(emb^2)); valid <- is.finite(nr) & nr > 0
    mod <- NA_real_
    if (sum(valid) > 2) {
      X <- emb[valid,,drop=FALSE] / nr[valid]
      k_eff <- max(1L, min(k, nrow(X)-1L))
      knn <- FNN::get.knn(X, k = k_eff)
      edges <- vector("list", nrow(X))
      for (i in seq_len(nrow(X))) {
        idx <- knn$nn.index[i,]; sims_i <- as.numeric(X[i,,drop=FALSE] %*% t(X[idx,,drop=FALSE]))
        keep <- is.finite(sims_i) & sims_i >= ct
        if (any(keep)) edges[[i]] <- cbind(i, idx[keep])
      }
      edges <- do.call(rbind, edges)
      if (!is.null(edges) && nrow(edges) > 0) {
        edf <- data.frame(from = as.character(edges[,1]), to = as.character(edges[,2]))
        g <- igraph::graph_from_data_frame(edf, directed = FALSE) |> igraph::simplify()
        mod <- er_modularity_safe(g, labs[valid])
      }
    }
    rows[[length(rows) + 1]] <- tibble::tibble(method = "embed_knn", knn_k = k, cos_thresh = ct,
                                               silhouette = sil, modularity = mod,
                                               k_clusters = length(unique(labs)))
  }
  curve <- dplyr::bind_rows(rows)
  er_choose_best(curve, objective = objective)
}

# Graph Coloring (threshold sweep) — uses string distances and (optionally) modularity on threshold graph
er_tune_gc_internal <- function(text_vec,
                                thresholds = seq(0.06, 0.20, 0.02),
                                dist_method = "jw",
                                objective = "silhouette",
                                n_mod_limit = 8000,
                                progress = NULL) {
  n <- length(text_vec)
  # distance once
  Dstr <- er_pairwise_stringdist(text_vec, method = dist_method, progress = progress)
  rows <- list()
  for (thr in thresholds) {
    # GC via resolve_entities
    gc_res <- er_gc_from_text(text_vec, thresholds = thr, gc_method = "rlf",
                              dist_method = dist_method, tune_metric = "adj_rand",
                              truth = NULL, ca_metrics = er_ca_metrics_default, progress = NULL)
    labs <- gc_res$labels
    sil  <- er_silhouette_avg_fromD(labs, Dstr)
    # Optional modularity on threshold graph (edges where dist <= thr). Cap n for memory.
    mod <- NA_real_
    if (n <= n_mod_limit) {
      # Build simple unweighted graph
      # (Use upper triangle to avoid duplicates)
      idx <- which(Dstr <= thr, arr.ind = TRUE)
      idx <- idx[idx[,1] < idx[,2], , drop = FALSE]
      if (nrow(idx) > 0) {
        edf <- data.frame(from = idx[,1], to = idx[,2])
        g <- igraph::graph_from_data_frame(edf, directed = FALSE, vertices = data.frame(name = seq_len(n)))
        mod <- er_modularity_safe(g, labs)
      }
    }
    rows[[length(rows) + 1]] <- tibble::tibble(method = "gc", threshold = thr,
                                               silhouette = sil, modularity = mod,
                                               k_clusters = length(unique(labs)))
  }
  curve <- dplyr::bind_rows(rows)
  er_choose_best(curve, objective = objective)
}

# ============================
#  C) One-call model selection
# ============================

# Returns: per-method best rows, full curves, and a cross-method leaderboard (by the same objective)
er_compare_methods_unsupervised <- function(
    data,
    fields = NULL, id_col = NULL, sheet = NULL,
    # grids
    k_grid = c(5,10,15,20),
    knn_grid = c(10,15,20,25),
    min_sim_grid = c(0.0, 0.05, 0.10),
    gc_thresholds = seq(0.06, 0.20, 0.02),
    embed_k_grid = c(10,15,20), embed_cos_grid = c(0.85, 0.88, 0.90),
    # objective
    objective = c("silhouette","ch","db","modularity","ranked_mix"),
    mix_weights = c(silhouette = 1, ch = 0.5, db = 0.5, modularity = 1),
    # plumbing
    show_progress = TRUE, n_mod_limit = 8000
){
  objective <- match.arg(objective)

  # 1) Load + select + features
  p <- if (isTRUE(show_progress)) er_progress_start(7, "Unsupervised model selection") else NULL
  df_raw  <- er_load_input(data, sheet = sheet);                                                     er_progress_tick(p, "Loaded input")
  df_text <- er_select_fields(df_raw, id_col = id_col, fields = fields, normalize = TRUE, auto_fields = is.null(fields));  er_progress_tick(p, "Selected fields")
  Xsvd <- er_features_tfidf_svd(df_text$text_for_matching, svd_dim = 100);                           er_progress_tick(p, "Built TF-IDF + SVD")
  Dcos <- er_cosine_dist(Xsvd)

  # Optional embeddings
  emb_col <- intersect(tolower(c("embedded clean ag.value","embedded ag.value","emb","embedding","vector","embedding_clean")), names(df_raw))[1]
  E <- if (!is.na(emb_col)) er_safe_parse_embedding_col(df_raw[[emb_col]]) else NULL
  er_progress_tick(p, sprintf("Embeddings: %s", ifelse(is.null(E), "no", "yes")))

  # 2) Tune per method
  km     <- er_tune_kmeans_internal(Xsvd, k_grid = k_grid, objective = objective, Dcos = Dcos);      er_progress_tick(p, "KMeans tuned")
  hc     <- er_tune_hclust_internal(Xsvd, k_grid = k_grid, objective = objective, Dcos = Dcos);      er_progress_tick(p, "HC tuned")
  pam    <- er_tune_pam_internal(Xsvd, k_grid = k_grid, objective = objective, Dcos = Dcos);         er_progress_tick(p, "PAM tuned")
  lv     <- er_tune_louvain_internal(Xsvd, knn_grid = knn_grid, min_sim_grid = min_sim_grid,
                                     objective = if (objective %in% c("modularity","ranked_mix")) objective else "silhouette",
                                     Dcos = Dcos);                                                    er_progress_tick(p, "Louvain tuned")
  gc     <- er_tune_gc_internal(df_text$text_for_matching, thresholds = gc_thresholds,
                                dist_method = "jw", objective = if (objective == "modularity") "silhouette" else objective,
                                n_mod_limit = n_mod_limit, progress = p);                             er_progress_tick(p, "GC tuned")
  embknn <- if (!is.null(E)) er_tune_embed_knn_internal(E, k_grid = embed_k_grid, cos_grid = embed_cos_grid,
                                                        objective = if (objective %in% c("modularity","ranked_mix")) objective else "silhouette",
                                                        Xsvd = Xsvd, Dcos = Dcos) else NULL

  # 3) Build leaderboard by the same objective
  best_rows <- dplyr::bind_rows(
    km$best_row, hc$best_row, pam$best_row, lv$best_row, gc$best_row,
    if (!is.null(embknn)) embknn$best_row else NULL
  ) %>%
    dplyr::mutate(method = as.character(method))

  # harmonize columns
  add_if_missing <- function(df, col) { if (!col %in% names(df)) df[[col]] <- NA_real_; df }
  for (m in c("silhouette","ch","db","modularity")) best_rows <- add_if_missing(best_rows, m)

  # choose order
  if (objective == "ranked_mix") {
    best_rows$rank_mix <- er_rank_mix(best_rows, mix_weights)
    leaderboard <- best_rows[order(best_rows$rank_mix, na.last = TRUE), ]
  } else if (objective == "db") {
    leaderboard <- best_rows[order(best_rows$db, na.last = TRUE), ]
  } else if (objective == "modularity") {
    leaderboard <- best_rows[order(-best_rows$modularity, na.last = TRUE), ]
  } else if (objective == "ch") {
    leaderboard <- best_rows[order(-best_rows$ch, na.last = TRUE), ]
  } else { # silhouette
    leaderboard <- best_rows[order(-best_rows$silhouette, na.last = TRUE), ]
  }

  er_progress_done(p)

  list(
    leaderboard = leaderboard,
    per_method_best = list(kmeans = km$best_row, hclust = hc$best_row, pam = pam$best_row,
                           louvain_knn = lv$best_row, gc = gc$best_row, embed_knn = if (!is.null(embknn)) embknn$best_row else NULL),
    curves = list(kmeans = km$curve, hclust = hc$curve, pam = pam$curve, louvain_knn = lv$curve,
                  gc = gc$curve, embed_knn = if (!is.null(embknn)) embknn$curve else NULL),
    fields_used = colnames(df_text)[2]
  )
}
