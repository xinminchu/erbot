# ============================================================
# Unified, modular ER pipeline (cora / affiliation / D10K / etc.)
# ============================================================

# ---- Packages ----
.er_require <- function(pkgs) {
  to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
  if (length(to_install)) install.packages(to_install, quiet = TRUE)
  suppressPackageStartupMessages(invisible(lapply(pkgs, require, character.only = TRUE)))
}
.er_require(c("data.table","dplyr","tibble","stringr","stringi",
              "tidyr","purrr", "readr","readxl","text2vec","Matrix",
              "irlba","stringdist","igraph", "FNN","mclust"))

# -------------------------
# 0) Truth handling helpers
# -------------------------
er_pairs_to_clusters <- function(truth_pairs, id1 = "id1", id2 = "id2") {
  stopifnot(all(c(id1, id2) %in% names(truth_pairs)))
  pairs <- truth_pairs %>%
    transmute(id1 = as.character(.data[[id1]]),
              id2 = as.character(.data[[id2]])) %>%
    filter(!is.na(id1), !is.na(id2), id1 != "", id2 != "", id1 != id2) %>%
    mutate(a = pmin(id1, id2), b = pmax(id1, id2)) %>%
    distinct(a, b, .keep_all = FALSE)

  if (!nrow(pairs)) return(tibble(id = character(), cluster_id = integer()))
  g <- igraph::graph_from_data_frame(pairs, directed = FALSE)
  memb <- igraph::components(g)$membership
  tibble(id = names(memb), cluster_id = as.integer(memb))
}

er_truth_from_any <- function(truth, sep_pair = "\\|",
                              id_candidates = c("id","record_id","rec_id","docid","rowid","paper_id")) {
  if (is.null(truth)) return(NULL)

  # Named vector (id->cluster)
  if (is.vector(truth) && !is.null(names(truth))) {
    return(tibble(id = as.character(names(truth)), cluster_id = as.integer(truth)))
  }

  # Path to file?
  if (is.character(truth) && length(truth) == 1L) {
    p <- truth
    ext <- tolower(tools::file_ext(p))
    if (ext %in% c("csv","tsv","txt","psv")) {
      truth <- readr::read_delim(p, delim = ifelse(ext=="tsv","\t", ","), show_col_types = FALSE, guess_max = 1e6)
    } else if (ext %in% c("xlsx","xls")) {
      truth <- readxl::read_excel(p)
    } else {
      truth <- data.table::fread(p, showProgress = TRUE)
    }
  }

  # Data frame?
  if (is.data.frame(truth)) {
    names(truth) <- tolower(names(truth))
    # one-column pair list e.g. "Entity1|Entity2"
    if (ncol(truth) == 1L) {
      col <- names(truth)[1]
      pairs <- truth %>%
        transmute(tmp = .data[[col]]) %>%
        filter(!is.na(tmp), tmp != "") %>%
        tidyr::separate(tmp, c("id1","id2"), sep = sep_pair, remove = TRUE, fill = "right", extra = "drop")
      return(er_pairs_to_clusters(pairs, "id1", "id2"))
    }
    # (id1,id2) pair table?
    if (all(c("id1","id2") %in% names(truth))) {
      return(er_pairs_to_clusters(truth, "id1","id2"))
    }
    # (id, cluster) style?
    id_truth <- intersect(names(truth), id_candidates)
    id_truth <- if (length(id_truth)) id_truth[1] else names(truth)[1]
    lab_col <- setdiff(names(truth), id_truth)[1]
    stopifnot(!is.na(lab_col))
    return(truth %>%
             transmute(id = as.character(.data[[id_truth]]),
                       cluster_id = .data[[lab_col]]) %>%
             distinct(id, .keep_all = TRUE))
  }
  stop("Unsupported truth type. Provide a pair list, id+cluster table, named vector, or a path.")
}

# -----------------------------------
# 1) Data loading / input normalization
# -----------------------------------
er_load_input <- function(data,
                          sheet = NULL,
                          dataset_name = NULL) {
  # In-memory data frame
  if (is.data.frame(data)) {
    df <- tibble::as_tibble(data)
    names(df) <- tolower(names(df))
    return(df)
  }

  # Special dataset names
  if (is.character(data) && length(data) == 1L) {
    key <- tolower(data)
    if (key %in% c("cora")) {
      if (!requireNamespace("cora", quietly = TRUE)) stop("Package 'cora' not installed.")
      df <- tibble::as_tibble(get("cora", envir = asNamespace("cora")))
      names(df) <- tolower(names(df))
      return(df)
    }
  }

  # Paths / URLs
  if (is.character(data) && length(data) == 1L) {
    p <- data
    ext <- tolower(tools::file_ext(p))
    if (ext %in% c("xlsx","xls")) {
      df <- readxl::read_excel(p, sheet = sheet %||% 1L)
      df <- tibble::as_tibble(df)
    } else if (ext %in% c("csv","tsv","txt","psv","pipe")) {
      # fread auto-detects separators; works for | too
      df <- data.table::fread(p, showProgress = TRUE)
      df <- tibble::as_tibble(df)
    } else {
      # try fread as a catch-all (works for many URLs/text files)
      df <- data.table::fread(p, showProgress = TRUE)
      df <- tibble::as_tibble(df)
    }
    names(df) <- tolower(names(df))
    return(df)
  }

  stop("Unsupported 'data' input. Provide a data.frame/tibble, a dataset key (e.g. 'cora'), or a path/URL.")
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

# -----------------------------------
# 2) Column detection & text building
# -----------------------------------
er_select_fields <- function(df,
                             id_col = NULL,
                             fields = NULL,              # character vector of columns to combine (preferred)
                             extra_fields = NULL,        # additional columns to concat (optional)
                             id_candidates = c("id","affiliation_id","record_id","rec_id","docid","rowid","paper_id"),
                             text_candidates = c("title","name","raw_title","string","text","affiliation","aggregate value","clean ag.value"),
                             normalize = TRUE) {

  names(df) <- tolower(names(df))
  # id
  if (is.null(id_col)) {
    id_col <- intersect(id_candidates, names(df))[1]
    if (is.na(id_col)) {
      df$id <- as.character(seq_len(nrow(df)))
      id_col <- "id"
    }
  } else {
    stopifnot(id_col %in% names(df))
  }

  # text fields
  if (is.null(fields) || !length(fields)) {
    main <- intersect(text_candidates, names(df))[1]
    if (is.na(main)) stop("No text field found. Supply 'fields=' or ensure one of: ", paste(text_candidates, collapse=", "))
    fields <- main
  } else {
    # keep only present
    fields <- tolower(fields)
    fields <- fields[fields %in% names(df)]
    if (!length(fields)) stop("None of the requested 'fields' exist in the data.")
  }

  if (!is.null(extra_fields) && length(extra_fields)) {
    extra_fields <- tolower(extra_fields)
    extra_fields <- extra_fields[extra_fields %in% names(df)]
    fields <- unique(c(fields, extra_fields))
  }

  out <- df %>%
    transmute(
      id = as.character(.data[[id_col]]),
      text_for_matching = stringr::str_squish(do.call(paste, c(.[fields], sep = " ")))
    )
  out$text_for_matching[is.na(out$text_for_matching) | out$text_for_matching==""] <- " "

  if (normalize) {
    out$text_for_matching <- out$text_for_matching %>%
      stringi::stri_trans_nfkc() %>%
      tolower() %>%
      stringr::str_replace_all("\\s+", " ") %>%
      stringr::str_trim()
  }
  out
}

# -----------------------------------
# 3) Embedding parsing (for D10K-like)
# -----------------------------------
er_safe_parse_embedding_col <- function(x) {
  x <- as.character(x); x[is.na(x)] <- ""
  num_pat <- "[-+]?(?:\\d*\\.\\d+|\\d+)(?:[eE][-+]?\\d+)?"
  lst <- regmatches(x, gregexpr(num_pat, x, perl = TRUE))
  lens <- lengths(lst)
  if (all(lens == 0L)) stop("No numeric tokens found in the embedding column.")
  tab <- sort(table(lens[lens > 0L]), decreasing = TRUE)
  d <- if (length(tab)) as.integer(names(tab)[1]) else max(lens)
  if (is.na(d) || d <= 0L) stop("Parsed embedding length invalid.")
  m <- matrix(NA_real_, nrow = length(lst), ncol = d)
  for (i in seq_along(lst)) {
    v <- suppressWarnings(as.numeric(lst[[i]]))
    if (length(v)) {
      if (length(v) >= d) m[i, ] <- v[1:d] else m[i, 1:length(v)] <- v
    }
  }
  storage.mode(m) <- "double"
  m
}

# -----------------------------------
# 4) Methods
# -----------------------------------
er_kmeans_tfidf <- function(text_vec, k = 10, svd_dim = 100, seed = 123) {
  it   <- text2vec::itoken(text_vec, tokenizer = text2vec::word_tokenizer, progressbar = FALSE)
  vocab <- text2vec::create_vocabulary(it)
  vec   <- text2vec::vocab_vectorizer(vocab)
  dtm   <- text2vec::create_dtm(it, vec)
  tfidf <- text2vec::TfIdf$new()
  Xtf   <- tfidf$fit_transform(dtm)

  k_dim <- max(2L, min(svd_dim, min(dim(Xtf)) - 1L))
  set.seed(42)
  svd_res <- irlba::irlba(Xtf, nv = k_dim)
  Xsvd <- svd_res$u %*% diag(svd_res$d)

  set.seed(seed)
  stats::kmeans(Xsvd, centers = k, nstart = 20)$cluster
}

er_mst_or_sn_edit <- function(text_vec, mst_cut_ratio = 5, mst_k = NULL,
                              sn_window = 40, sn_method = "jw", sn_thresh = 0.12) {
  n <- length(text_vec)
  if (n < 2) return(rep(1L, n))
  # use MST for smaller n, sorted-neighborhood for larger n
  if (n <= 3000) {
    D <- stringdist::stringdistmatrix(text_vec, text_vec, method = "lv")
    D <- as.matrix(D); diag(D) <- 0
    g <- igraph::graph_from_adjacency_matrix(D, mode="undirected", weighted=TRUE, diag=FALSE)
    mst <- igraph::mst(g, weights = igraph::E(g)$weight)
    if (is.null(mst_k)) mst_k <- max(2L, floor(n / mst_cut_ratio))
    ord <- order(igraph::E(mst)$weight, decreasing = TRUE)
    cut_e <- igraph::E(mst)[ord][seq_len(min(mst_k-1L, length(ord)))]
    g2 <- if (length(cut_e)) igraph::delete_edges(mst, cut_e) else mst
    return(igraph::components(g2)$membership)
  }
  # Sorted-neighborhood (linear-ish)
  key <- text_vec %>% stringi::stri_trans_nfkc() %>% tolower() %>%
    stringr::str_replace_all("\\s+"," ") %>% stringr::str_trim()
  ord <- order(key)
  edges_from <- integer(0); edges_to <- integer(0)
  for (i in seq_len(n)) {
    idx_i <- ord[i]
    j_end <- min(n, i + sn_window)
    if (j_end <= i) next
    idx_j <- ord[(i+1):j_end]
    d <- stringdist::stringdist(text_vec[idx_i], text_vec[idx_j], method = sn_method)
    keep <- is.finite(d) & d <= sn_thresh
    if (any(keep)) {
      edges_from <- c(edges_from, rep.int(idx_i, sum(keep)))
      edges_to   <- c(edges_to,   idx_j[keep])
    }
  }
  vdf <- data.frame(name = as.character(seq_len(n)))
  if (length(edges_from)) {
    edf <- data.frame(from = as.character(edges_from), to = as.character(edges_to))
    g <- igraph::graph_from_data_frame(edf, directed = FALSE, vertices = vdf) |> igraph::simplify()
  } else {
    g <- igraph::make_empty_graph(n = n); igraph::V(g)$name <- as.character(seq_len(n))
  }
  as.integer(igraph::components(g)$membership)
}

er_louvain_knn <- function(text_vec, knn = 10, min_sim = 0.0, svd_dim = 100) {
  it   <- text2vec::itoken(text_vec, tokenizer = text2vec::word_tokenizer, progressbar = FALSE)
  vocab <- text2vec::create_vocabulary(it)
  vec   <- text2vec::vocab_vectorizer(vocab)
  dtm   <- text2vec::create_dtm(it, vec)
  tfidf <- text2vec::TfIdf$new()
  Xtf   <- tfidf$fit_transform(dtm)

  k_dim <- max(2L, min(svd_dim, min(dim(Xtf)) - 1L))
  set.seed(42)
  svd_res <- irlba::irlba(Xtf, nv = k_dim)
  Xsvd <- svd_res$u %*% diag(svd_res$d)

  rs <- sqrt(rowSums(Xsvd^2)); rs[rs==0] <- 1
  Xn <- Xsvd / rs

  knn_use <- max(1L, min(knn, nrow(Xn)-1L))
  nn <- FNN::get.knn(Xn, k = knn_use)
  edges <- cbind(rep(seq_len(nrow(nn$nn.index)), each = knn_use),
                 as.vector(nn$nn.index))

  # cosine ~ dot on normalized rows
  sims <- vapply(seq_len(nrow(Xn)), function(i){
    as.numeric(Xn[i, , drop = FALSE] %*% t(Xn[nn$nn.index[i, ], , drop = FALSE]))
  }, numeric(knn_use))
  keep <- sims >= min_sim
  edf <- cbind(from = rep(seq_len(nrow(Xn)), each = knn_use)[keep],
               to   = as.vector(nn$nn.index)[keep])

  if (!length(edf)) return(rep(1L, nrow(Xn)))
  g <- igraph::graph_from_edgelist(matrix(edf, ncol = 2), directed = FALSE) |> igraph::simplify()
  igraph::membership(igraph::cluster_louvain(g))
}

er_embed_knn <- function(emb_mat, k = 15, cos_thresh = 0.88) {
  if (!is.matrix(emb_mat)) emb_mat <- as.matrix(emb_mat)
  storage.mode(emb_mat) <- "double"
  n <- nrow(emb_mat); if (n < 2) return(rep(1L, n))
  nr <- sqrt(rowSums(emb_mat^2, na.rm = TRUE))
  valid <- is.finite(nr) & nr > 0
  out <- seq_len(n)  # default singletons
  if (sum(valid) < 2) return(out)
  X <- emb_mat[valid, , drop = FALSE]; X <- X / nr[valid]
  k_eff <- max(1L, min(k, nrow(X) - 1L))
  knn <- FNN::get.knn(X, k = k_eff)

  edges <- vector("list", nrow(X))
  for (i in seq_len(nrow(X))) {
    idx <- knn$nn.index[i, ]
    sims_i <- as.numeric(X[i, , drop = FALSE] %*% t(X[idx, , drop = FALSE]))
    keep <- is.finite(sims_i) & sims_i >= cos_thresh
    if (any(keep)) edges[[i]] <- cbind(i, idx[keep])
  }
  edges <- do.call(rbind, edges)

  verts <- data.frame(name = as.character(seq_len(nrow(X))))
  if (!is.null(edges) && nrow(edges) > 0) {
    edf <- data.frame(from = as.character(edges[,1]), to = as.character(edges[,2]))
    g <- igraph::graph_from_data_frame(edf, directed = FALSE, vertices = verts) |> igraph::simplify()
  } else {
    g <- igraph::make_empty_graph(n = nrow(X)); igraph::V(g)$name <- as.character(seq_len(nrow(X)))
  }
  memb_valid <- igraph::components(g)$membership
  out[valid] <- as.integer(memb_valid)
  out
}

# -----------------------------------
# 5) Evaluation
# -----------------------------------
er_choose2 <- function(x) ifelse(x >= 2, x*(x-1)/2, 0)
er_pairwise_prf_fast <- function(pred, truth) {
  tab <- table(pred = as.integer(pred), truth = as.integer(truth))
  TP <- sum(er_choose2(tab))
  FP <- sum(er_choose2(rowSums(tab))) - TP
  FN <- sum(er_choose2(colSums(tab))) - TP
  Precision <- ifelse(TP + FP == 0, 0, TP/(TP+FP))
  Recall    <- ifelse(TP + FN == 0, 0, TP/(TP+FN))
  F1        <- ifelse(Precision + Recall == 0, 0, 2*Precision*Recall/(Precision+Recall))
  list(Precision=Precision, Recall=Recall, F1=F1)
}
er_adj_rand <- function(p, t) mclust::adjustedRandIndex(as.integer(p), as.integer(t))

# -----------------------------------
# 6) Unified pipeline
# -----------------------------------
er_unified_pipeline <- function(
    data,                            # df, path/URL, or key "cora"
    truth = NULL,                    # df/path/named vector/pair list/single-col pair
    sheet = NULL,                    # excel sheet (if applicable)
    # text building
    id_col = NULL,
    fields = NULL,
    extra_fields = NULL,
    id_candidates = c("id","affiliation_id","record_id","rec_id","docid","rowid","paper_id"),
    text_candidates = c("title","name","raw_title","string","text","affiliation","aggregate value","clean ag.value"),
    # embeddings
    embed_col = NULL,
    embed_candidates = c("embedded clean ag.value","embedded ag.value","emb","embedding","vector","embedding_clean"),
    # methods / params
    k_clusters = 10,
    knn_k = 15,
    mst_cut_ratio = 5,
    mst_k = NULL,
    sn_window = 40, sn_method = "jw", sn_thresh = 0.12,
    svd_dim = 100,
    louvain_min_sim = 0.0,
    cos_thresh = 0.88,
    eval_mode = c("labeled_only","singleton_fill"),
    write_csv = NULL                 # output file path
){
  eval_mode <- match.arg(eval_mode)

  # 1) Load
  df_raw <- er_load_input(data, sheet = sheet)
  # 2) Text
  df_text <- er_select_fields(df_raw, id_col = id_col, fields = fields, extra_fields = extra_fields,
                              id_candidates = id_candidates, text_candidates = text_candidates, normalize = TRUE)
  n <- nrow(df_text)

  # 3) Embeddings (optional)
  if (is.null(embed_col)) {
    ec <- intersect(tolower(embed_candidates), names(df_raw))[1]
  } else {
    ec <- tolower(embed_col)
  }
  E <- NULL
  if (!is.na(ec) && !is.null(ec) && ec %in% names(df_raw)) {
    E <- er_safe_parse_embedding_col(df_raw[[ec]])
  }

  # 4) Methods
  pred_kmeans  <- er_kmeans_tfidf(df_text$text_for_matching, k = k_clusters, svd_dim = svd_dim)
  pred_mst_sn  <- er_mst_or_sn_edit(df_text$text_for_matching, mst_cut_ratio = mst_cut_ratio, mst_k = mst_k,
                                    sn_window = sn_window, sn_method = sn_method, sn_thresh = sn_thresh)
  pred_louvain <- er_louvain_knn(df_text$text_for_matching, knn = knn_k, min_sim = louvain_min_sim, svd_dim = svd_dim)
  pred_embed   <- if (!is.null(E)) er_embed_knn(E, k = knn_k, cos_thresh = cos_thresh) else NULL

  # 5) Predictions table
  out <- tibble::tibble(
    id = df_text$id,
    text_for_matching = df_text$text_for_matching,
    pred_kmeans   = as.integer(pred_kmeans),
    pred_mst_or_sn = as.integer(pred_mst_sn),
    pred_louvain  = as.integer(pred_louvain)
  )
  if (!is.null(E)) out$pred_embedKNN <- as.integer(pred_embed)

  # 6) Truth (optional) & evaluation
  perf_tbl <- NULL
  if (!is.null(truth)) {
    truth_clusters <- er_truth_from_any(truth)
    if (nrow(truth_clusters)) {
      out <- dplyr::left_join(out, truth_clusters, by = "id")
      if (eval_mode == "singleton_fill") {
        if (any(is.na(out$cluster_id))) {
          start <- suppressWarnings(max(as.integer(out$cluster_id), na.rm = TRUE)); start <- ifelse(is.finite(start), start, 0L)
          miss  <- which(is.na(out$cluster_id))
          out$cluster_id[miss] <- start + seq_along(miss)
        }
        idx <- seq_len(nrow(out))
      } else {
        idx <- which(!is.na(out$cluster_id))
      }
      if (length(idx) >= 2) {
        gold <- out$cluster_id[idx]
        meth_cols <- grep("^pred_", names(out), value = TRUE)
        res_list <- lapply(meth_cols, function(mc){
          pr <- out[[mc]][idx]
          prf <- er_pairwise_prf_fast(pr, gold)
          list(Method = mc,
               ARI = er_adj_rand(pr, gold),
               Precision = prf$Precision, Recall = prf$Recall, F1 = prf$F1)
        })
        perf_tbl <- dplyr::bind_rows(lapply(res_list, tibble::as_tibble))
        perf_tbl$Method <- sub("^pred_", "", perf_tbl$Method)
        perf_tbl$Method <- dplyr::recode(perf_tbl$Method,
                                         "mst_or_sn" = "MST_or_SN_Edit",
                                         "embedKNN"  = "Embed_kNN",
                                         .default = perf_tbl$Method
        )
      }
    }
  }

  # 7) Write
  if (!is.null(write_csv)) readr::write_csv(out, write_csv)

  list(
    performance = perf_tbl,
    predictions = out,
    details = list(
      n = n,
      used_fields = setdiff(names(df_text), c("id","text_for_matching")),
      has_embeddings = !is.null(E),
      params = list(
        k_clusters = k_clusters, knn_k = knn_k,
        mst_cut_ratio = mst_cut_ratio, mst_k = mst_k,
        sn_window = sn_window, sn_method = sn_method, sn_thresh = sn_thresh,
        svd_dim = svd_dim, louvain_min_sim = louvain_min_sim, cos_thresh = cos_thresh,
        eval_mode = eval_mode
      )
    )
  )
}

# -----------------------------------
# 7) Main runner (nice facade)
# -----------------------------------
er_main <- function(
    data, truth = NULL,
    fields = NULL, extra_fields = NULL,
    id_col = NULL, embed_col = NULL,
    # method params
    k_clusters = 10, knn_k = 15,
    mst_cut_ratio = 5, mst_k = NULL,
    sn_window = 40, sn_method = "jw", sn_thresh = 0.12,
    svd_dim = 100, louvain_min_sim = 0.0, cos_thresh = 0.88,
    eval_mode = "labeled_only",
    write_csv = NULL, sheet = NULL
){
  res <- er_unified_pipeline(
    data = data, truth = truth, sheet = sheet,
    id_col = id_col, fields = fields, extra_fields = extra_fields,
    embed_col = embed_col,
    k_clusters = k_clusters, knn_k = knn_k,
    mst_cut_ratio = mst_cut_ratio, mst_k = mst_k,
    sn_window = sn_window, sn_method = sn_method, sn_thresh = sn_thresh,
    svd_dim = svd_dim, louvain_min_sim = louvain_min_sim, cos_thresh = cos_thresh,
    eval_mode = eval_mode,
    write_csv = write_csv
  )
  if (!is.null(res$performance)) print(res$performance)
  invisible(res)
}
