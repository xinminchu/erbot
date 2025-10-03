#' Normalize named field weights
#' @export
normalize_weights <- function(weights, field_names) {
  w <- unlist(weights)
  if (is.null(names(w))) {
    if (length(w) != length(field_names)) stop("weights length must match fields")
    names(w) <- field_names
  } else {
    w <- w[field_names]; w[is.na(w)] <- 0
  }
  if (any(w < 0)) stop("weights must be non-negative")
  s <- sum(w); if (s <= 0) stop("sum(weights) must be > 0")
  w / s
}

#' Expand fields proportionally to weights (fallback)
#' @export
expand_fields_by_weights <- function(fields, weights, base_rep = 10L) {
  w <- normalize_weights(weights, fields)
  reps <- pmax(1L, round(w * base_rep))
  expanded <- unlist(mapply(function(f, r) rep(f, r), fields, reps, SIMPLIFY = FALSE), use.names = FALSE)
  attr(expanded, "weights") <- w
  attr(expanded, "reps") <- reps
  expanded
}

#' Run er_main using weights (native or fallback)
#' @export
run_with_weights <- function(data, truth, fields, weights, id_col = "id",
                             k_grid = seq(10, 300, by = 10), base_rep = 12L,
                             write_csv = NULL, er_method = c("kmeans","gc","louvain"),
                             save_pdf = NULL, pdf_title = "ER Report") {
  er_method <- match.arg(er_method)
  w_norm <- normalize_weights(weights, fields)

  res <- NULL; used_native <- FALSE
  try({
    res <- er_main(
      data = data, truth = truth, fields = fields,
      field_weights = w_norm, k_grid = k_grid, write_csv = write_csv
    )
    used_native <- TRUE
  }, silent = TRUE)

  if (is.null(res)) {
    expanded_fields <- expand_fields_by_weights(fields, w_norm, base_rep = base_rep)
    res <- er_main(
      data = data, truth = truth, fields = expanded_fields,
      k_grid = k_grid, write_csv = write_csv
    )
  } else {
    attr(res, "weights_used") <- w_norm
  }
  if (!is.null(save_pdf)) try(er_save_report_pdf(res, save_pdf, pdf_title), silent = TRUE)

  extract_adj_rand <- function(res) {
    tryCatch({
      if (!is.null(res$performance)) {
        row <- res$performance
        m <- row$adj_rand
        if (length(m)) as.numeric(m[which.max(m)]) else NA_real_
      } else NA_real_
    }, error=function(e) NA_real_)
  }

  list(result = res, weights = w_norm, method = er_method, adj_rand = extract_adj_rand(res), native_weights = used_native)
}

#' Learn best field weights via CV and run final
#' @export
er_general_pipeline <- function(data, truth, id_col = "id",
                                fields = setdiff(names(data), id_col),
                                weights = NULL, learn_weights = FALSE,
                                folds = 3, budget = 20, dirichlet_alpha = NULL,
                                base_rep = 12L, er_method = c("kmeans","gc","louvain"),
                                k_grid = seq(10, 300, by = 10),
                                write_csv = NULL, save_pdf = NULL, pdf_title = "ER Report",
                                max_records_per_fold = NULL,
                                seed = 42, verbose = TRUE) {
  er_method <- match.arg(er_method); set.seed(seed)

  if (isTRUE(learn_weights) || is.null(weights)) {
    if (verbose) cat("[er_general_pipeline] Learning weights via CV...\n")
    if (is.null(dirichlet_alpha)) dirichlet_alpha <- rep(1, length(fields))
    names(dirichlet_alpha) <- fields
    rdirichlet_simple <- function(n, alpha) {
      k <- length(alpha)
      out <- matrix(NA_real_, nrow = n, ncol = k)
      for (i in seq_len(n)) { g <- stats::rgamma(k, shape = alpha, rate = 1); out[i, ] <- g / sum(g) }
      colnames(out) <- names(alpha); out
    }
    ws <- rdirichlet_simple(budget, dirichlet_alpha)
    candidates <- lapply(seq_len(nrow(ws)), function(i) { w <- ws[i,]; names(w) <- colnames(ws); w })

    all_ids <- if (id_col %in% names(data)) as.character(data[[id_col]]) else as.character(seq_len(nrow(data)))
    fold_ids <- split(all_ids, sort.int(seq_along(all_ids) %% folds))
    eval_table <- dplyr::tibble()

    for (ci in seq_along(candidates)) {
      w <- normalize_weights(candidates[[ci]], fields)
      ari_folds <- c()
      for (fi in seq_along(fold_ids)) {
        ids_fold <- fold_ids[[fi]]
        if (!is.null(max_records_per_fold) && length(ids_fold) > max_records_per_fold) {
          ids_fold <- sample(ids_fold, max_records_per_fold)
        }
        subset_data <- function(dat, keep_ids) if (id_col %in% names(dat)) dat[dat[[id_col]] %in% keep_ids, , drop = FALSE] else dat[as.integer(keep_ids), , drop = FALSE]
        dat_fold <- subset_data(data, ids_fold)
        tru <- truth
        res <- try(run_with_weights(dat_fold, tru, fields, w, id_col = id_col,
                                    k_grid = k_grid, base_rep = base_rep, write_csv = NULL,
                                    er_method = er_method, save_pdf = NULL), silent = TRUE)
        ari <- if (inherits(res, "try-error")) NA_real_ else res$adj_rand
        ari_folds <- c(ari_folds, ari)
        if (verbose) cat(sprintf("Candidate %d fold %d: ARI=%.4f\n", ci, fi, ifelse(is.na(ari), NaN, ari)))
      }
      eval_table <- dplyr::bind_rows(eval_table,
                                     dplyr::tibble(candidate = ci, mean_ari = mean(ari_folds, na.rm = TRUE),
                                                   weights = list(w), ari_folds = list(ari_folds)))
    }
    best_row <- eval_table %>% dplyr::arrange(dplyr::desc(mean_ari)) %>% dplyr::slice(1)
    weights <- best_row$weights[[1]]
    if (verbose) cat(sprintf("[er_general_pipeline] Best weights: %s\n", paste(sprintf("%s=%.3f", names(weights), weights), collapse=", ")))
  } else {
    weights <- normalize_weights(weights, fields)
  }

  if (verbose) cat("[er_general_pipeline] Final run with selected weights...\n")
  final <- run_with_weights(
    data = data, truth = truth, fields = fields, weights = weights, id_col = id_col,
    k_grid = k_grid, base_rep = base_rep, write_csv = write_csv,
    er_method = er_method, save_pdf = save_pdf, pdf_title = pdf_title
  )
  list(final = final, fields = fields, weights = weights)
}
