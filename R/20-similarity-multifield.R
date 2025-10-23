########################################

#' Build a multi-field similarity (sparse)
#' @param df data.frame containing all fields in spec
#' @param spec list of lists: list(name=..., type=..., w=..., tau? for numeric/year)
#' @param block_key optional vector to limit pairwise comps within blocks
#' @param top_k keep top-k neighbors per row (symmetric)
#' @return sparse similarity matrix (Diagonal on self = 1)
#' @export
er_similarity_multifield <- function(df, spec, block_key = NULL, top_k = 30) {
  `%||%` <- get("%||%", asNamespace("erbot"))
  .sim_text_lev <- function(a,b){ if (is.na(a)||is.na(b)||a==""||b=="") return(NA_real_); m <- max(nchar(a), nchar(b)); if (m==0) return(NA_real_); 1 - stringdist::stringdist(a,b,"lv")/m }
  .sim_text_jw  <- function(a,b){ if (is.na(a)||is.na(b)||a==""||b=="") return(NA_real_); 1 - stringdist::stringdist(a,b,"jw") }
  .tokenize     <- function(x){ x <- tolower(x %||% ""); x <- gsub("[^[:alnum:]]+"," ",x); x <- stringr::str_squish(x); if (identical(x,"")) character(0) else unique(strsplit(x,"\\s+")[[1]]) }
  .sim_text_jacc<- function(a,b){ if (is.na(a)||is.na(b)||a==""||b=="") return(NA_real_); A <- .tokenize(a); B <- .tokenize(b); if (!length(A)&&!length(B)) return(NA_real_); inter <- length(intersect(A,B)); uni <- length(union(A,B)); if (uni==0) return(NA_real_); inter/uni }
  .sim_cat      <- function(a,b){ if (is.na(a)||is.na(b)) return(NA_real_); as.numeric(identical(as.character(a), as.character(b))) }
  .sim_num      <- function(a,b,range_max,tau=1){ if (is.na(a)||is.na(b)) return(NA_real_); if (!is.finite(range_max) || range_max <= 0) return(NA_real_); d <- abs(as.numeric(a) - as.numeric(b)) / range_max; exp(-(d / tau)) }
  .keep_topk    <- function(S, k){
    n <- nrow(S); ki <- kj <- integer(0); kx <- numeric(0)
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

  n <- nrow(df); if (is.null(block_key)) block_key <- rep("ALL", n)
  ukeys <- unique(block_key[!is.na(block_key)])

  num_ranges <- list()
  for (sp in spec) {
    if (sp$type %in% c("numeric","year")) {
      vv <- suppressWarnings(as.numeric(df[[sp$name]]))
      vv <- vv[is.finite(vv)]
      num_ranges[[sp$name]] <- if (length(vv) >= 2) diff(range(vv)) else NA_real_
    }
  }

  I <- J <- integer(0); X <- numeric(0)
  for (bk in ukeys) {
    idx <- which(block_key == bk)
    if (length(idx) <= 1) next
    for (ii in seq_len(length(idx) - 1L)) {
      i <- idx[ii]
      js <- idx[(ii+1L):length(idx)]
      svec <- numeric(length(js))
      for (kk in seq_along(js)) {
        j <- js[kk]; num <- 0; den <- 0
        for (sp in spec) {
          f <- sp$name; w <- sp$w; a <- df[[f]][i]; b <- df[[f]][j]
          sij <- switch(sp$type,
                        lev        = .sim_text_lev(a,b),
                        jw         = .sim_text_jw(a,b),
                        jaccard    = .sim_text_jacc(a,b),
                        categorical= .sim_cat(a,b),
                        numeric    = .sim_num(a,b, num_ranges[[f]], tau=sp$tau %||% 1),
                        year       = .sim_num(a,b, num_ranges[[f]], tau=sp$tau %||% 0.5),
                        stop("Unknown type: ", sp$type))
          if (!is.na(sij)) { num <- num + w*sij; den <- den + w }
        }
        s <- if (den>0) num/den else 0
        svec[kk] <- max(0, min(1, s))
      }
      I <- c(I, rep(i, length(js))); J <- c(J, js); X <- c(X, svec)
    }
  }
  keep <- which(!is.na(I) & !is.na(J) & !is.na(X))
  if (!length(keep)) {
    S <- Matrix::Diagonal(n = n, x = 1)
  } else {
    S <- Matrix::sparseMatrix(i = c(I[keep], J[keep]),
                              j = c(J[keep], I[keep]),
                              x = c(X[keep], X[keep]),
                              dims = c(n, n))
    diag(S) <- 1
  }
  if (!is.null(top_k) && top_k > 0) S <- .keep_topk(S, top_k)
  S
}


########################################
