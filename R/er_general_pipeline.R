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
#`%||%` <- function(a,b) if(!is.null(a)) a else b

# ================================
# er_similarity_multifield helper
# Includes required similarity functions and top-k pruning
# ================================

`%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a

# --- Similarity primitives ---
.sim_text_lev <- function(a,b){
  if (is.na(a) || is.na(b) || a=="" || b=="") return(NA_real_)
  m <- max(nchar(a), nchar(b)); if (m==0) return(NA_real_)
  1 - stringdist::stringdist(a,b,method="lv")/m
}

.sim_text_jw <- function(a,b){
  if (is.na(a) || is.na(b) || a=="" || b=="") return(NA_real_)
  1 - stringdist::stringdist(a,b,method="jw")
}

.tokenize <- function(x){
  x <- tolower(x %||% ""); x <- gsub("[^[:alnum:]]+"," ",x); x <- stringr::str_squish(x)
  if (identical(x,"")) character(0) else unique(strsplit(x,"\\s+")[[1]])
}

.sim_text_jacc <- function(a,b){
  if (is.na(a) || is.na(b) || a=="" || b=="") return(NA_real_)
  A <- .tokenize(a); B <- .tokenize(b)
  if (!length(A) && !length(B)) return(NA_real_)
  inter <- length(intersect(A,B)); uni <- length(union(A,B)); if (uni==0) return(NA_real_)
  inter/uni
}

.sim_categorical <- function(a,b){
  if (is.na(a) || is.na(b)) return(NA_real_)
  as.numeric(identical(as.character(a), as.character(b)))
}

.sim_numeric <- function(a,b,range_max,tau=1){
  if (is.na(a) || is.na(b)) return(NA_real_)
  if (!is.finite(range_max) || range_max <= 0) return(NA_real_)
  d <- abs(as.numeric(a) - as.numeric(b)) / range_max
  exp(-(d / tau))
}

# --- Keep top-k neighbors per row (symmetric) ---
.keep_topk_per_row <- function(S, k){
  n <- nrow(S)
  ki <- integer(0); kj <- integer(0); kx <- numeric(0)
  for (i in seq_len(n)) {
    st <- S@p[i]+1L; en <- S@p[i+1L]; if (en < st) next
    cols <- S@i[st:en] + 1L; vals <- S@x[st:en]
    self <- which(cols==i); others <- setdiff(seq_along(cols), self)
    take <- integer(0)
    if (length(others)) take <- others[head(order(vals[others], decreasing=TRUE), k)]
    if (length(self)) take <- c(self, take)
    ki <- c(ki, rep(i, length(take))); kj <- c(kj, cols[take]); kx <- c(kx, vals[take])
  }
  S2 <- Matrix::sparseMatrix(i=c(ki,kj), j=c(kj,ki), x=c(kx,kx), dims=dim(S))
  diag(S2) <- 1
  S2
}

# --- Main function: missing-aware, field-weighted multi-field similarity ---
# spec: list of lists: list(name=..., type=..., w=..., tau=? for numeric/year)
# types supported: "lev", "jw", "jaccard", "categorical", "numeric", "year"
er_similarity_multifield <- function(df, spec, block_key = NULL, top_k = 30) {
  n <- nrow(df)
  if (is.null(block_key)) block_key <- rep("ALL", n)
  # ignore NA block keys explicitly
  ukeys <- unique(block_key[!is.na(block_key)])

  # precompute numeric ranges safely
  num_ranges <- list()
  for (sp in spec) {
    if (sp$type %in% c("numeric","year")) {
      vv <- suppressWarnings(as.numeric(df[[sp$name]]))
      vv <- vv[is.finite(vv)]
      if (length(vv) >= 2) num_ranges[[sp$name]] <- diff(range(vv)) else num_ranges[[sp$name]] <- NA_real_
    }
  }

  I <- integer(0); J <- integer(0); X <- numeric(0)

  for (bk in ukeys) {
    idx <- which(block_key == bk)  # NA-safe because bk is not NA
    if (length(idx) <= 1) next

    # iterate only over i < j pairs without generating NA
    if (length(idx) >= 2) {
      for (ii in seq_len(length(idx) - 1L)) {
        i <- idx[ii]
        js <- idx[(ii+1L):length(idx)]  # guaranteed non-empty here
        svec <- numeric(length(js))
        for (kk in seq_along(js)) {
          j <- js[kk]
          num <- 0; den <- 0
          for (sp in spec) {
            f <- sp$name; w <- sp$w; a <- df[[f]][i]; b <- df[[f]][j]
            sij <- switch(sp$type,
                          lev        = .sim_text_lev(a,b),
                          jw         = .sim_text_jw(a,b),
                          jaccard    = .sim_text_jacc(a,b),
                          categorical= .sim_categorical(a,b),
                          numeric    = .sim_numeric(a,b, num_ranges[[f]], tau=sp$tau %||% 1),
                          year       = .sim_numeric(a,b, num_ranges[[f]], tau=sp$tau %||% 0.5),
                          stop("Unknown type: ", sp$type)
            )
            if (!is.na(sij)) { num <- num + w*sij; den <- den + w }
          }
          s <- if (den>0) num/den else 0
          svec[kk] <- max(0, min(1, s))
        }
        I <- c(I, rep(i, length(js))); J <- c(J, js); X <- c(X, svec)
      }
    }
  }

  # final NA guard (shouldn't be necessary now, but keeps robust)
  keep <- which(!is.na(I) & !is.na(J) & !is.na(X))
  if (length(keep) == 0) {
    S <- Matrix::Diagonal(n = n, x = 1)
  } else {
    S <- Matrix::sparseMatrix(i = c(I[keep], J[keep]),
                              j = c(J[keep], I[keep]),
                              x = c(X[keep], X[keep]),
                              dims = c(n, n))
    diag(S) <- 1
  }

  if (!is.null(top_k) && top_k > 0) S <- .keep_topk_per_row(S, top_k)
  S
}

# --- Optional: Louvain from similarity ---
# ===============================================
# SAFE replacement for er_louvain_from_S
# Avoids using non-existent slots like S@j (dgCMatrix has i, p, x, not j)
# Builds edges via Matrix::summary(S) which returns (i, j, x) 1-based.
# ===============================================
er_louvain_from_S <- function(S, min_sim = 0.0) {
  stopifnot(inherits(S, "sparseMatrix"))
  E <- Matrix::summary(S)  # data.frame with cols: i, j, x (1-based)
  # keep only off-diagonal, above threshold
  E <- E[E$i != E$j & is.finite(E$x) & !is.na(E$x) & E$x >= min_sim, , drop = FALSE]
  if (!nrow(E)) return(rep(1L, nrow(S)))

  # Undirected simple graph: keep only i < j to avoid duplicate edges
  E <- E[E$i < E$j, , drop = FALSE]

  G <- igraph::graph_from_data_frame(E[, c("i","j")], directed = FALSE, vertices = data.frame(name = seq_len(nrow(S))))
  igraph::E(G)$weight <- E$x

  cl <- igraph::cluster_louvain(G, weights = igraph::E(G)$weight)

  memb <- rep(NA_integer_, nrow(S))
  # igraph vertex names are character; coerce to integer index
  v_ids <- as.integer(igraph::V(G)$name)
  memb[v_ids] <- igraph::membership(cl)

  # Assign isolated vertices (not in any edge) unique cluster IDs
  if (anyNA(memb)) {
    maxlab <- if (all(is.na(memb))) 0L else max(memb, na.rm = TRUE)
    na_idx <- which(is.na(memb))
    memb[na_idx] <- maxlab + seq_along(na_idx)
  }
  memb
}


.er_kmeans_sweep <- function(X_dense, k_grid, seed=123, nstart=10) {
  # Ensure k are valid
  k_grid <- sort(unique(k_grid[k_grid >= 2 & k_grid < nrow(X_dense)]))
  if (length(k_grid) == 0) stop("k_grid has no valid k (>=2 and < n).")

  # L2-normalize rows -> cosine geometry
  rn <- sqrt(rowSums(X_dense^2))
  rn[rn == 0] <- 1
  Z <- X_dense / rn

  best <- list(k = NULL, cl = NULL, score = -Inf)

  for (k in k_grid) {
    set.seed(seed)
    cl <- kmeans(Z, centers = k, nstart = nstart)$cluster

    # Compute cluster centers in Z-space and L2-normalize them
    centers <- rowsum(Z, cl) / as.vector(table(cl))
    cnorm <- sqrt(rowSums(centers^2))
    cnorm[cnorm == 0] <- 1
    centers <- centers / cnorm

    # Cosine to own center
    cos_self <- rowSums(Z * centers[cl, , drop = FALSE])

    # Cosine to all centers, then take max over "other" centers per row
    cos_to_all <- Z %*% t(centers)
    cos_next <- numeric(nrow(Z))
    for (i in seq_len(nrow(Z))) {
      # exclude own cluster column
      cos_next[i] <- max(cos_to_all[i, setdiff(seq_len(k), cl[i])])
    }

    # Silhouette-like score in cosine space
    s <- (cos_self - cos_next) / pmax(cos_self, cos_next, 1e-8)
    s_mean <- mean(s, na.rm = TRUE)

    if (is.finite(s_mean) && s_mean > best$score) {
      best <- list(k = k, cl = cl, score = s_mean)
    }
  }
  best
}

.er_eval_external <- function(pred, truth) {
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


.er_kmeans_sweep <- function(X_dense, k_grid, seed=123, nstart=10) {
  # Ensure k are valid
  k_grid <- sort(unique(k_grid[k_grid >= 2 & k_grid < nrow(X_dense)]))
  if (length(k_grid) == 0) stop("k_grid has no valid k (>=2 and < n).")

  # L2-normalize rows -> cosine geometry
  rn <- sqrt(rowSums(X_dense^2))
  rn[rn == 0] <- 1
  Z <- X_dense / rn

  best <- list(k = NULL, cl = NULL, score = -Inf)

  for (k in k_grid) {
    set.seed(seed)
    cl <- kmeans(Z, centers = k, nstart = nstart)$cluster

    # Compute cluster centers in Z-space and L2-normalize them
    centers <- rowsum(Z, cl) / as.vector(table(cl))
    cnorm <- sqrt(rowSums(centers^2))
    cnorm[cnorm == 0] <- 1
    centers <- centers / cnorm

    # Cosine to own center
    cos_self <- rowSums(Z * centers[cl, , drop = FALSE])

    # Cosine to all centers, then take max over "other" centers per row
    cos_to_all <- Z %*% t(centers)
    cos_next <- numeric(nrow(Z))
    for (i in seq_len(nrow(Z))) {
      # exclude own cluster column
      cos_next[i] <- max(cos_to_all[i, setdiff(seq_len(k), cl[i])])
    }

    # Silhouette-like score in cosine space
    s <- (cos_self - cos_next) / pmax(cos_self, cos_next, 1e-8)
    s_mean <- mean(s, na.rm = TRUE)

    if (is.finite(s_mean) && s_mean > best$score) {
      best <- list(k = k, cl = cl, score = s_mean)
    }
  }
  best
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


# ===============================================
# SAFE drop-in: er_save_report_pdf
# - Avoids vapply length-0 errors by sanitizing all fields to length-1 scalars
# - Generates a clean PDF using base grid (no rmarkdown dependency)
# - Works even when external metrics are NA / missing
# ===============================================

er_save_report_pdf <- function(res, out_path, dataset_name = "ER Report") {
  # Dependencies
  if (!requireNamespace("grid", quietly = TRUE)) stop("Package 'grid' is required.")
  if (!requireNamespace("grDevices", quietly = TRUE)) stop("Package 'grDevices' is required.")

  # ---- helpers ----
  .ensure1_chr <- function(x, default = "NA") {
    if (is.null(x) || length(x) == 0) return(default)
    x <- x[1]
    if (is.na(x)) return(default)
    as.character(x)
  }
  .ensure1_num <- function(x, digits = 3, default = "NA") {
    if (is.null(x) || length(x) == 0) return(default)
    x <- x[1]
    if (is.na(x)) return(default)
    fmt <- tryCatch(formatC(as.numeric(x), format = "f", digits = digits), error = function(e) default)
    if (length(fmt) == 0) default else fmt
  }
  .len <- function(x) if (is.null(x)) 0L else length(x)

  # ---- collect safe values ----
  n <- .len(res$clusters)
  n_clusters <- if (n > 0) length(unique(res$clusters)) else 0L

  lines <- c(
    paste0("Dataset: ", .ensure1_chr(dataset_name)),
    paste0("Chosen method: ", .ensure1_chr(res$chosen)),
    paste0("k (if applicable): ", .ensure1_chr(res$k %||% NA)),
    paste0("Records (n): ", .ensure1_chr(n)),
    paste0("Clusters: ", .ensure1_chr(n_clusters)),
    "",
    "External evaluation (if truth provided):",
    paste0("  ARI: ", .ensure1_num(res$external$ARI)),
    paste0("  Precision: ", .ensure1_num(res$external$Precision)),
    paste0("  Recall: ", .ensure1_num(res$external$Recall)),
    paste0("  F1: ", .ensure1_num(res$external$F1))
  )

  # cluster size summary
  if (n > 0) {
    cs <- sort(table(res$clusters), decreasing = TRUE)
    topk <- head(cs, 20)
    lines <- c(lines, "", "Top 20 cluster sizes:", paste0("  ", names(topk), ": ", as.integer(topk)))
  }

  # field summary
  if (!is.null(res$summary$fields)) {
    lines <- c(lines, "", paste0("Fields used: ", paste(res$summary$fields, collapse = ", ")))
  }

  # ---- draw PDF ----
  try(dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE), silent = TRUE)
  grDevices::pdf(out_path, width = 8.5, height = 11)
  on.exit(try(grDevices::dev.off(), silent = TRUE), add = TRUE)

  grid::grid.newpage()
  y <- 0.95
  line_h <- 0.03

  # Title
  ttl <- paste0("Entity Resolution Report — ", .ensure1_chr(dataset_name))
  grid::grid.text(ttl, x = 0.5, y = y, gp = grid::gpar(fontsize = 18, fontface = "bold"))
  y <- y - 2*line_h

  # Body
  for (ln in lines) {
    if (y < 0.05) {  # new page if needed
      grid::grid.newpage(); y <- 0.95
    }
    grid::grid.text(.ensure1_chr(ln, default=""), x = 0.05, y = y, just = "left",
                    gp = grid::gpar(fontsize = 11))
    y <- y - line_h
  }

  invisible(out_path)
}


# ------------------------- Unified pipeline
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

  # progress setup
  flags <- c(load=1, select=1, tfidf=1, embed=1, kmeans=1, mstsn=1, louvain=1, embknn=1, hc=1, pam=1,
             gc = as.integer(!is.null(gc_thresholds) && length(gc_thresholds) > 0),
             comm = as.integer(length(run_comm_methods) > 0), eval=1, write=1)
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

  # ---- KMeans (tuned)
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

  # ---- MST/SN+edit
  pred_mst_sn <- er_mst_or_sn_edit(df_text$text_for_matching, mst_cut_ratio=mst_cut_ratio, mst_k=mst_k,
                                   sn_window=sn_window, sn_method=sn_method, sn_thresh=sn_thresh)
  er_progress_tick(p, "MST/SN+edit")

  # ---- Louvain on kNN
  lv <- er_louvain_knn(Xsvd, knn=knn_k, min_sim=louvain_min_sim)
  pred_louvain <- lv$labels; g_knn <- lv$graph;                                        er_progress_tick(p, sprintf("Louvain kNN (k=%d)", knn_k))

  # ---- Embed-kNN
  pred_embed <- if (!is.null(E)) er_embed_knn(E, k=knn_k, cos_thresh=cos_thresh) else NULL
  er_progress_tick(p, sprintf("Embed-kNN: %s", ifelse(is.null(pred_embed), "skipped", "done")))

  # ---- HC (tuned)
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

  # ---- PAM (tuned)
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

  # ---- Graph Coloring via resolve_entities (threshold sweep)
  pred_gc <- NULL; gc_best_thr <- NA; gc_tune_tbl <- NULL
  if (!is.null(gc_thresholds) && length(gc_thresholds) && nrow(df_text) >= 2) {
    truth_vec <- if (!is.null(truth)) { tt <- er_truth_from_any(truth); setNames(tt$cluster_id, tt$id)[df_text$id] } else NULL
    gc_res <- er_gc_from_text(df_text$text_for_matching, thresholds=gc_thresholds, gc_method=gc_method,
                              dist_method=gc_dist_method, tune_metric=tune_metric, truth=truth_vec,
                              ca_metrics=er_ca_metrics_default, progress=p, dist_block=4000)
    pred_gc <- gc_res$labels; gc_best_thr <- gc_res$best_threshold; gc_tune_tbl <- gc_res$tuning
  }
  er_progress_tick(p, "Graph Coloring")

  # ---- Extra communities on kNN graph
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

  # 5) Predictions table
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

  # 6) Evaluation (clustering_agreement only)
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

# ------------------------- Facade (simple command)

er_main <- function(data,
                    truth = NULL,
                    fields = c("title","authors","address"),
                    k_grid = seq(10, 300, by = 10),
                    write_csv = NULL,
                    # alignment knobs
                    svd_k = 100,
                    knn_k = 15,
                    louvain_min_sim = 0.0,
                    use_methods = c("kmeans_tfidf_svd","louvain_knn","louvain_multifield")) {

  use_methods <- match.arg(use_methods,
                           c("kmeans_tfidf_svd","louvain_knn","louvain_multifield"),
                           several.ok = TRUE)

  # Load data
  df <- if (is.character(data) && file.exists(data)) {
    read.csv(data, stringsAsFactors = FALSE)
  } else if (is.data.frame(data)) {
    data
  } else {
    stop("Unsupported 'data' input. Provide a data.frame or a readable file path.")
  }
  n <- nrow(df)

  # Concatenate text exactly like original
  df_text <- er_select_fields(df, id_col = NULL, fields = fields, extra_fields = NULL,
                              normalize = TRUE, auto_fields = is.null(fields))
  Xsvd <- er_features_tfidf_svd(df_text$text_for_matching, svd_dim = svd_k)

  # Path 1: KMeans on SVD features
  km_best <- NULL
  if ("kmeans_tfidf_svd" %in% use_methods) {
    km_best <- .er_kmeans_sweep(Xsvd, k_grid)
  }

  # Path 2: Original Louvain on kNN graph from SVD features
  lv_knn <- NULL
  if ("louvain_knn" %in% use_methods) {
    lv_res <- er_louvain_knn(Xsvd, knn = knn_k, min_sim = louvain_min_sim)
    lv_knn <- as.integer(lv_res$labels)
  }

  # Path 3: Optional multi-field (equal weights; jw text, numeric year)
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

  # Collect candidates
  cand <- list()
  if (!is.null(km_best)) {
    ev <- .er_eval_external(km_best$cl, truth)
    cand$kmeans_tfidf_svd <- list(name="kmeans_tfidf_svd", cl=km_best$cl, k=km_best$k,
                                  score_ext = ev, score_int = km_best$score)
  }
  if (!is.null(lv_knn)) {
    ev <- .er_eval_external(lv_knn, truth)
    cand$louvain_knn <- list(name="louvain_knn", cl=lv_knn, k=NA_integer_,
                             score_ext = ev, score_int = NA_real_)
  }
  if (!is.null(lv_multi)) {
    ev <- .er_eval_external(lv_multi, truth)
    cand$louvain_multifield <- list(name="louvain_multifield", cl=lv_multi, k=NA_integer_,
                                    score_ext = ev, score_int = NA_real_)
  }

  if (length(cand) == 0) stop("No methods ran. Check inputs or 'use_methods'.")

  pick <- NULL
  if (!is.null(truth)) {
    scores <- sapply(cand, function(x) c(ARI = x$score_ext$ARI %||% -Inf,
                                         F1  = x$score_ext$F1  %||% -Inf))
    ord <- order(scores["ARI",], scores["F1",], decreasing = TRUE, na.last = TRUE)
    pick <- cand[[ord[1]]]
  } else {
    if (!is.null(cand$kmeans_tfidf_svd)) pick <- cand$kmeans_tfidf_svd
    else if (!is.null(cand$louvain_knn)) pick <- cand$louvain_knn
    else pick <- cand$louvain_multifield
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


.sim_text_jw <- function(a,b){
  if (is.na(a) || is.na(b) || a=="" || b=="") return(NA_real_)
  1 - stringdist::stringdist(a,b,method="jw")
}
