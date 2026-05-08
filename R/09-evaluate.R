########################################
# File: R/09-evaluate.R
# Evaluation: ARI, NMI, VI (via GCMER), B-cubed P/R/F, Homogeneity,
# Completeness, V-measure, Pairwise F (for compatibility).
# Primary metrics: ARI, NMI, B-cubed F.
# Pairwise F is reported for comparison with prior published results only.
#
# Truth ingestion (from 22-eval-truth.R) is also here.
########################################

# ── Truth ingestion ───────────────────────────────────────────────────────────

#' Convert pairwise match list to cluster labels
#'
#' @param truth_pairs data.frame with columns \code{id1}, \code{id2}.
#' @return tibble(id, cluster_id).
#' @export
er_pairs_to_clusters <- function(truth_pairs, id1 = "id1", id2 = "id2") {
  stopifnot(all(c(id1, id2) %in% names(truth_pairs)))
  pairs <- truth_pairs %>%
    dplyr::transmute(id1 = as.character(.data[[id1]]),
                     id2 = as.character(.data[[id2]])) %>%
    dplyr::filter(!is.na(id1), !is.na(id2), id1 != "", id2 != "", id1 != id2) %>%
    dplyr::mutate(a = pmin(id1, id2), b = pmax(id1, id2)) %>%
    dplyr::distinct(a, b, .keep_all = FALSE)
  if (!nrow(pairs)) return(tibble::tibble(id = character(), cluster_id = integer()))
  g    <- igraph::graph_from_data_frame(pairs, directed = FALSE)
  memb <- igraph::components(g)$membership
  tibble::tibble(id = names(memb), cluster_id = as.integer(memb))
}

#' Ingest ground truth from various formats
#'
#' Accepts:
#' \itemize{
#'   \item Named integer vector (names = record IDs, values = cluster IDs).
#'   \item File path (CSV/TSV/Excel) with columns \code{id} + \code{cluster_id}
#'     or \code{id1} + \code{id2} (pair list).
#'   \item data.frame with the same columns.
#' }
#'
#' @param truth Named vector, file path, or data.frame.
#' @return tibble(id, cluster_id), or \code{NULL} if \code{truth} is
#'   \code{NULL}.
#' @export
er_truth_from_any <- function(truth, sep_pair = "\\|",
                              id_candidates = c("id", "record_id", "rec_id",
                                                "docid", "rowid", "paper_id")) {
  if (is.null(truth)) return(NULL)
  if (is.vector(truth) && !is.null(names(truth)))
    return(tibble::tibble(id = as.character(names(truth)),
                          cluster_id = as.integer(truth)))
  if (is.character(truth) && length(truth) == 1L) {
    ext <- tolower(tools::file_ext(truth))
    if (ext %in% c("csv","tsv","txt","psv"))
      truth <- readr::read_delim(truth,
                                  delim = if (ext == "tsv") "\t" else ",",
                                  show_col_types = FALSE, guess_max = 1e6)
    else if (ext %in% c("xlsx","xls"))
      truth <- readxl::read_excel(truth)
    else
      truth <- data.table::fread(truth, showProgress = FALSE)
  }
  if (is.data.frame(truth)) {
    names(truth) <- tolower(names(truth))
    if (all(c("id1","id2") %in% names(truth)))
      return(er_pairs_to_clusters(truth, "id1", "id2"))
    id_col  <- intersect(names(truth), id_candidates)
    id_col  <- if (length(id_col)) id_col[1] else names(truth)[1]
    lab_col <- setdiff(names(truth), id_col)[1]
    return(truth %>%
             dplyr::transmute(id = as.character(.data[[id_col]]),
                              cluster_id = .data[[lab_col]]) %>%
             dplyr::distinct(id, .keep_all = TRUE))
  }
  stop("Unsupported truth type.")
}

# ── B-cubed ───────────────────────────────────────────────────────────────────

#' B-cubed Precision, Recall, and F-score
#'
#' Every record contributes equally to the score, regardless of cluster size.
#' Singletons are handled correctly (they contribute P = R = 1 if correctly
#' isolated, and P or R = 0 if wrongly merged or wrongly split).
#'
#' @param pred Integer vector of predicted cluster labels (length n).
#' @param truth Integer vector of true cluster labels (length n).
#'
#' @return Named list: \code{P} (B3-Precision), \code{R} (B3-Recall),
#'   \code{F} (B3-F-score).
#' @export
er_bcubed <- function(pred, truth) {
  pred  <- as.integer(pred)
  truth <- as.integer(truth)
  n     <- length(pred)
  stopifnot(length(truth) == n)

  p_vec <- numeric(n)
  r_vec <- numeric(n)
  for (i in seq_len(n)) {
    same_pred  <- which(pred  == pred[i])
    same_truth <- which(truth == truth[i])
    overlap    <- length(intersect(same_pred, same_truth))
    p_vec[i]   <- overlap / length(same_pred)
    r_vec[i]   <- overlap / length(same_truth)
  }
  P <- mean(p_vec, na.rm = TRUE)
  R <- mean(r_vec, na.rm = TRUE)
  F <- if ((P + R) > 0) 2 * P * R / (P + R) else 0
  list(P = P, R = R, F = F)
}

# ── Homogeneity, Completeness, V-measure ─────────────────────────────────────

#' Homogeneity, Completeness, and V-measure
#'
#' \describe{
#'   \item{Homogeneity}{Each predicted cluster contains only members of a
#'     single true entity. \eqn{H = 1 - H(T|C) / H(T)}.}
#'   \item{Completeness}{All members of a true entity are in the same predicted
#'     cluster. \eqn{C = 1 - H(C|T) / H(C)}.}
#'   \item{V-measure}{Harmonic mean of H and C.}
#' }
#'
#' @param pred Integer vector of predicted labels.
#' @param truth Integer vector of true labels.
#'
#' @return Named list: \code{H} (Homogeneity), \code{C} (Completeness),
#'   \code{V} (V-measure).
#' @export
er_vmeasure <- function(pred, truth) {
  pred  <- as.integer(pred)
  truth <- as.integer(truth)
  n     <- length(pred)
  stopifnot(length(truth) == n)

  .entropy <- function(labels) {
    tbl <- table(labels)
    pk  <- tbl / sum(tbl)
    -sum(pk * log(pk + 1e-12))
  }
  .cond_entropy <- function(a_labels, b_labels) {
    # H(a | b)
    tbl_b <- table(b_labels)
    h <- 0
    for (bv in names(tbl_b)) {
      idx   <- b_labels == as.integer(bv)
      h_ab  <- .entropy(a_labels[idx])
      pb    <- tbl_b[bv] / n
      h     <- h + pb * h_ab
    }
    h
  }

  H_T  <- .entropy(truth)
  H_C  <- .entropy(pred)
  H_TC <- .cond_entropy(truth, pred)   # H(truth | pred)
  H_CT <- .cond_entropy(pred, truth)   # H(pred | truth)

  homogeneity   <- if (H_T > 1e-10) 1 - H_TC / H_T else 1
  completeness  <- if (H_C > 1e-10) 1 - H_CT / H_C else 1
  vmeasure      <- if ((homogeneity + completeness) > 0)
    2 * homogeneity * completeness / (homogeneity + completeness) else 0

  list(H = homogeneity, C = completeness, V = vmeasure)
}

# ── Pairwise F (for compatibility) ────────────────────────────────────────────

#' Pairwise Precision, Recall, and F-measure
#'
#' Standard pair-level metric.  Reported for comparison with prior published
#' results only; \strong{B-cubed and ARI are the primary metrics}.
#'
#' @param pred Integer vector of predicted labels.
#' @param truth Integer vector of true labels.
#' @return Named list: \code{P}, \code{R}, \code{F}.
#' @export
er_pairwise_f <- function(pred, truth) {
  pred  <- as.integer(pred)
  truth <- as.integer(truth)
  tab   <- table(pred, truth)
  TP    <- sum(choose(as.numeric(tab), 2))
  PP    <- sum(choose(as.numeric(rowSums(tab)), 2))
  GP    <- sum(choose(as.numeric(colSums(tab)), 2))
  P     <- if (PP > 0) TP / PP else 0
  R     <- if (GP > 0) TP / GP else 0
  F     <- if ((P + R) > 0) 2 * P * R / (P + R) else 0
  list(P = P, R = R, F = F)
}

# ── er_evaluate() ─────────────────────────────────────────────────────────────

#' Evaluate clustering results against ground truth
#'
#' Computes ARI (via GCMER), NMI, VI (both via GCMER \code{mutual_info()}),
#' B-cubed (P/R/F), Homogeneity, Completeness, V-measure, and pairwise F for
#' each method in \code{pred_list}.  GCMER is the primary source for ARI, NMI,
#' and VI; \code{mclust} is used as a fallback for ARI if GCMER is unavailable.
#'
#' @param pred_list Named list of integer label vectors, or a single integer
#'   vector.
#' @param truth Named integer vector, data.frame(id, cluster_id), or file path.
#' @param id_vec Character vector of record IDs aligned to the label vectors.
#'   Required when truth is not a vector of length n.
#' @param eval_mode Character. \code{"labeled_only"} uses only records with a
#'   truth label. \code{"singleton_fill"} assigns a unique cluster to unlabelled
#'   records before evaluation.
#'
#' @return A tibble with one row per method and columns:
#'   \code{method}, \code{ARI}, \code{NMI}, \code{VI},
#'   \code{Bcubed_P}, \code{Bcubed_R}, \code{Bcubed_F},
#'   \code{Homogeneity}, \code{Completeness}, \code{Vmeasure},
#'   \code{PairF_P}, \code{PairF_R}, \code{PairF_F}.
#' @export
er_evaluate <- function(pred_list,
                        truth,
                        id_vec    = NULL,
                        eval_mode = c("labeled_only", "singleton_fill")) {
  eval_mode <- match.arg(eval_mode)

  # Coerce single vector
  if (is.integer(pred_list) || is.numeric(pred_list)) {
    pred_list <- list(result = pred_list)
  }
  n <- length(pred_list[[1]])

  # Parse truth
  truth_tbl <- er_truth_from_any(truth)
  if (is.null(truth_tbl) || !nrow(truth_tbl))
    stop("er_evaluate: could not parse 'truth'.")

  truth_map <- setNames(as.integer(truth_tbl$cluster_id),
                        as.character(truth_tbl$id))
  if (is.null(id_vec)) id_vec <- as.character(seq_len(n))

  gold <- truth_map[id_vec]  # NA for unlabelled records

  # Select evaluation indices
  if (eval_mode == "singleton_fill") {
    max_id <- suppressWarnings(max(gold, na.rm = TRUE))
    max_id <- if (is.finite(max_id)) max_id else 0L
    miss   <- which(is.na(gold))
    gold[miss] <- max_id + seq_along(miss)
    eval_idx <- seq_len(n)
  } else {
    eval_idx <- which(!is.na(gold))
  }
  if (length(eval_idx) < 2L) {
    warning("er_evaluate: fewer than 2 labelled records; returning empty table.")
    return(tibble::tibble())
  }
  gold_sub <- as.integer(gold[eval_idx])

  rows <- lapply(names(pred_list), function(mname) {
    labs <- as.integer(pred_list[[mname]])[eval_idx]

    # ── ARI: GCMER primary, mclust fallback ─────────────────────────────────
    ari_val <- tryCatch({
      if (requireNamespace("GCMER", quietly = TRUE))
        GCMER::adj_rand(labs, gold_sub)
      else if (requireNamespace("mclust", quietly = TRUE))
        mclust::adjustedRandIndex(labs, gold_sub)
      else NA_real_
    }, error = function(e) NA_real_)

    # ── NMI (harmonic) and VI: GCMER mutual_info() ─────────────────────────
    nmi_val <- NA_real_
    vi_val  <- NA_real_
    if (requireNamespace("GCMER", quietly = TRUE)) {
      mi_res <- tryCatch(GCMER::mutual_info(labs, gold_sub),
                         error = function(e) NULL)
      if (!is.null(mi_res)) {
        nmi_val <- as.numeric(mi_res["FJ"])   # harmonic-mean NMI (Fred & Jain)
        vi_val  <- as.numeric(mi_res["VI"])   # Variation of Information
      }
    }

    b3  <- tryCatch(er_bcubed(labs, gold_sub),
                    error = function(e) list(P=NA_real_, R=NA_real_, F=NA_real_))
    hcv <- tryCatch(er_vmeasure(labs, gold_sub),
                    error = function(e) list(H=NA_real_, C=NA_real_, V=NA_real_))
    pf  <- tryCatch(er_pairwise_f(labs, gold_sub),
                    error = function(e) list(P=NA_real_, R=NA_real_, F=NA_real_))

    tibble::tibble(
      method       = mname,
      ARI          = round(ari_val,  4),
      NMI          = round(nmi_val,  4),
      VI           = round(vi_val,   4),
      Bcubed_P     = round(b3$P,     4),
      Bcubed_R     = round(b3$R,     4),
      Bcubed_F     = round(b3$F,     4),
      Homogeneity  = round(hcv$H,    4),
      Completeness = round(hcv$C,    4),
      Vmeasure     = round(hcv$V,    4),
      PairF_P      = round(pf$P,     4),
      PairF_R      = round(pf$R,     4),
      PairF_F      = round(pf$F,     4)
    )
  })
  dplyr::bind_rows(rows)
}

# ── Unsupervised quality scores (no truth) ────────────────────────────────────

#' Unsupervised clustering quality scores
#'
#' When no ground truth is available, computes:
#' \enumerate{
#'   \item Within-cluster similarity (mean S(i,j) for same-cluster pairs).
#'   \item Between-cluster similarity (mean S(i,j) for different-cluster pairs).
#'   \item Similarity gap (within - between; larger is better).
#'   \item Average silhouette width on D = 1 - S.
#' }
#'
#' @param pred Integer vector of cluster labels.
#' @param S Symmetric \code{dgCMatrix} (n × n) of combined similarities.
#' @param max_pairs_for_sil Integer. Max pairs sampled for silhouette
#'   (expensive for large n). Default \code{5000L}.
#'
#' @return Named list: \code{within_sim}, \code{between_sim}, \code{sim_gap},
#'   \code{silhouette}.
#' @export
er_unsupervised_quality <- function(pred, S, max_pairs_for_sil = 5000L) {
  pred <- as.integer(pred)
  n    <- length(pred)

  E <- Matrix::summary(S)
  E <- E[E$i < E$j, , drop = FALSE]
  if (!nrow(E)) return(list(within_sim=NA, between_sim=NA, sim_gap=NA, silhouette=NA))

  same_cl  <- pred[E$i] == pred[E$j]
  w_sim    <- if (any(same_cl))   mean(E$x[same_cl],  na.rm = TRUE) else NA_real_
  b_sim    <- if (any(!same_cl))  mean(E$x[!same_cl], na.rm = TRUE) else NA_real_
  gap      <- if (!is.na(w_sim) && !is.na(b_sim)) w_sim - b_sim else NA_real_

  # Silhouette: subsample if n is large
  sil_val <- NA_real_
  if (n <= max_pairs_for_sil) {
    D_sil <- stats::as.dist(pmax(0, 1 - as.matrix(S)))
    sil_val <- er_silhouette_avg(pred, D_sil)
  } else {
    idx <- sample(n, max_pairs_for_sil)
    S_sub <- S[idx, idx, drop = FALSE]
    D_sub <- stats::as.dist(pmax(0, 1 - as.matrix(S_sub)))
    sil_val <- er_silhouette_avg(pred[idx], D_sub)
  }

  list(within_sim  = round(w_sim,  4),
       between_sim = round(b_sim,  4),
       sim_gap     = round(gap,    4),
       silhouette  = round(sil_val, 4))
}
