########################################
# File: R/17-compare_paradigms.R
# Compare three multi-view ER fusion strategies on a benchmark dataset.
#
# er_compare_paradigms() -- runs A (late clustering), B (early clustering),
#                           C (SNF iterative) and returns a full metrics table.
#
# Internal helper:
#   .cc_from_binary  -- connected components from binary match matrix
########################################

#' Compare three multi-view ER fusion strategies
#'
#' Runs Strategy~A (\emph{late clustering}), Strategy~B (\emph{early
#' clustering} via per-field consensus), and Strategy~C (\emph{SNF iterative
#' fusion}) on pre-computed per-field similarity vectors.  Returns a
#' comparison table with ARI, B-cubed P/R/F, and pairwise P/R/F for every
#' strategy.
#'
#' \strong{Strategy pipelines:}
#' \describe{
#'   \item{A}{Normalize each field \eqn{\to} combine with weights \eqn{\to}
#'     \code{er_classify()} \eqn{\to} connected components.}
#'   \item{B}{\code{er_field_ensemble()}: per-field threshold-CC \eqn{\to}
#'     majority-vote consensus (\code{alpha_B}).}
#'   \item{C}{\code{er_snf()} on per-field sparse matrices \eqn{\to}
#'     \code{er_classify()} \eqn{\to} connected components.}
#' }
#'
#' @param sim_list Named list of numeric vectors from \code{er_similarity()},
#'   one per field.
#' @param pairs \code{tibble(idx1, idx2)} from \code{er_block()}.
#' @param n Integer.  Total number of records.
#' @param truth Ground truth accepted by \code{er_truth_from_any()}: named
#'   integer vector, \code{data.frame(id, cluster_id)}, or file path.
#' @param id_vec Optional character vector of record IDs (length \code{n}).
#'   Defaults to \code{"1"}, \code{"2"}, \ldots
#' @param strategy Character vector.  Which strategies to run.
#'   Any subset of \code{c("A","B","C","C_multiplex")}.  \code{"C"} runs
#'   SNF; \code{"C_multiplex"} runs multiplex community detection.
#'   Default: all four.
#' @param weights_A Weight method for Strategy~A passed to \code{er_weights()}:
#'   \code{"equal"} (default), \code{"fellegi_sunter"}, \code{"ari"},
#'   \code{"bimodal"}, \code{"variance"}.
#' @param norm_method Per-field normalization before Strategy~A combination.
#'   Passed to \code{er_normalize()}.  Default \code{"minmax"}.
#' @param threshold Similarity cut-off for binary classification in
#'   Strategies~A and~C.  Default \code{0.5}.
#' @param alpha_B Consensus fraction for Strategy~B: fraction of fields that
#'   must agree to classify a pair as a match
#'   (\eqn{p_{ij}} adapts to missingness).  Default \code{0.5}.
#' @param K_snf KNN count for SNF status-matrix sparsification.
#'   Default \code{20L}.
#' @param t_snf Diffusion iterations for SNF.  Default \code{20L}.
#' @param gamma_mp Resolution parameter for multiplex Louvain/Leiden.
#'   Corresponds to \eqn{\gamma} in Exp~1.  Default \code{1.0}.
#' @param omega_mp Inter-layer coupling weight for the supra-adjacency
#'   multiplex variant.  Default \code{0.5}.
#' @param verbose Logical.  Print per-strategy progress and timing.
#'   Default \code{TRUE}.
#'
#' @return A \code{tibble} with one row per strategy and columns:
#'   \code{strategy}, \code{ARI}, \code{NMI}, \code{VI},
#'   \code{Bcubed_P}, \code{Bcubed_R}, \code{Bcubed_F},
#'   \code{PairF_P}, \code{PairF_R}, \code{PairF_F},
#'   \code{n_clusters}, \code{elapsed_sec}.
#'
#' @examples
#' \dontrun{
#' df    <- er_load("restaurant")
#' diag  <- er_diagnose(df)
#' pairs <- er_block(df, diag)
#' sim   <- er_similarity(df, pairs)
#' gold  <- er_load_gold("restaurant")
#' n     <- nrow(df)
#' ids   <- as.character(df[[1]])   # first column = record ID
#'
#' cmp <- er_compare_paradigms(
#'   sim_list = sim,
#'   pairs    = pairs,
#'   n        = n,
#'   truth    = gold,
#'   id_vec   = ids
#' )
#' print(cmp[, c("strategy","ARI","Bcubed_F","PairF_P","PairF_R")])
#' }
#' @export
er_compare_paradigms <- function(sim_list,
                                 pairs,
                                 n,
                                 truth,
                                 id_vec      = NULL,
                                 strategy    = c("A", "B", "C",
                                                 "C_multiplex"),
                                 weights_A   = "equal",
                                 norm_method = "minmax",
                                 threshold   = 0.5,
                                 alpha_B     = 0.5,
                                 K_snf       = 20L,
                                 t_snf       = 20L,
                                 gamma_mp    = 1.0,
                                 omega_mp    = 0.5,
                                 verbose     = TRUE) {

  valid_strats <- c("A", "B", "C", "C_MULTIPLEX")
  strategy <- intersect(toupper(strategy), valid_strats)
  if (!length(strategy))
    stop("er_compare_paradigms: no valid strategy in {A, B, C, C_multiplex}.")
  if (!length(sim_list))
    stop("er_compare_paradigms: sim_list is empty.")
  stopifnot(is.data.frame(pairs),
            all(c("idx1", "idx2") %in% names(pairs)))

  n <- as.integer(n)
  if (is.null(id_vec)) id_vec <- as.character(seq_len(n))

  truth_tbl <- er_truth_from_any(truth)
  if (is.null(truth_tbl) || !nrow(truth_tbl))
    stop("er_compare_paradigms: could not parse 'truth'.")

  .msg <- function(fmt, ...) if (verbose) message(sprintf(fmt, ...))
  pred_list    <- list()
  elapsed_list <- list()

  # ── Strategy A: normalize → combine → classify → CC ───────────────────────
  if ("A" %in% strategy) {
    .msg("[er_compare_paradigms] Strategy A: late clustering...")
    t0 <- proc.time()[["elapsed"]]

    sim_norm <- er_normalize(sim_list, method = norm_method)
    wt_a     <- er_weights(sim_norm, pairs = pairs,
                           truth = truth_tbl, id_vec = id_vec,
                           method = weights_A)
    comb_a   <- er_combine(sim_norm, weights = wt_a)
    s_a      <- er_pairs_to_sparse(pairs, comb_a, n)
    m_a      <- er_classify(s_a, method = "threshold", threshold = threshold)
    labs_a   <- .cc_from_binary(m_a, n)

    elapsed_list[["A"]] <- proc.time()[["elapsed"]] - t0
    pred_list[["A"]]    <- labs_a
    .msg("  done: %d clusters in %.1f s.",
         length(unique(labs_a)), elapsed_list[["A"]])
  }

  # ── Strategy B: per-field classify → majority-vote consensus ───────────────
  if ("B" %in% strategy) {
    .msg("[er_compare_paradigms] Strategy B: early clustering...")
    t0 <- proc.time()[["elapsed"]]

    labs_b <- er_field_ensemble(
      sim_list       = sim_list,
      pairs          = pairs,
      n              = n,
      cluster_method = "threshold_cc",
      merge_alpha    = alpha_B,
      threshold      = threshold
    )

    elapsed_list[["B"]] <- proc.time()[["elapsed"]] - t0
    pred_list[["B"]]    <- labs_b
    .msg("  done: %d clusters in %.1f s.",
         length(unique(labs_b)), elapsed_list[["B"]])
  }

  # ── Strategy C: SNF → classify → CC ────────────────────────────────────────
  if ("C" %in% strategy) {
    .msg("[er_compare_paradigms] Strategy C: SNF iterative fusion...")
    t0 <- proc.time()[["elapsed"]]

    # Build per-field sparse matrices (NA -> 0) and row-normalize for SNF
    s_list_c <- lapply(sim_list, function(sv) {
      s_k <- er_pairs_to_sparse(pairs, sv, n, na_fill = 0)
      er_normalize(s_k, method = "row_stochastic")
    })

    # Drop fields that are entirely zero (no candidate pairs for that field)
    has_data <- vapply(s_list_c,
                       function(s_k) Matrix::nnzero(s_k) > 0, logical(1))
    if (sum(has_data) < 2L) {
      warning("er_compare_paradigms: fewer than 2 fields have data for SNF; ",
              "Strategy C skipped.")
    } else {
      s_fused <- tryCatch(
        er_snf(s_list_c[has_data], K = K_snf, t = t_snf),
        error = function(e) {
          warning("er_compare_paradigms: SNF failed (", conditionMessage(e),
                  "); Strategy C skipped.")
          NULL
        }
      )
      if (!is.null(s_fused)) {
        # Row-stochastic SNF output has very small values; rescale to [0,1]
        # so 'threshold' is on the same scale as Strategies A and B.
        s_fused_sc <- er_normalize(s_fused, method = "minmax",
                                   symmetric = TRUE)
        m_c    <- er_classify(s_fused_sc, method = "threshold",
                              threshold = threshold)
        labs_c <- .cc_from_binary(m_c, n)
        elapsed_list[["C"]] <- proc.time()[["elapsed"]] - t0
        pred_list[["C"]]    <- labs_c
        .msg("  done: %d clusters in %.1f s.",
             length(unique(labs_c)), elapsed_list[["C"]])
      }
    }
  }

  # ── Strategy C-Multiplex: multiplex community detection ────────────────────
  if ("C_MULTIPLEX" %in% strategy) {
    .msg("[er_compare_paradigms] Strategy C-Multiplex (gamma=%.2f, omega=%.2f)...",
         gamma_mp, omega_mp)
    t0 <- proc.time()[["elapsed"]]

    s_list_mp <- lapply(sim_list, function(sv)
      er_pairs_to_sparse(pairs, sv, n, na_fill = 0))

    has_data <- vapply(s_list_mp,
                       function(s_k) Matrix::nnzero(s_k) > 0, logical(1))
    if (sum(has_data) < 2L) {
      warning("er_compare_paradigms: fewer than 2 fields have data; ",
              "Strategy C_multiplex skipped.")
    } else {
      labs_mp <- tryCatch(
        er_multiplex(s_list_mp[has_data], gamma = gamma_mp,
                     omega = omega_mp, verbose = FALSE),
        error = function(e) {
          warning("er_compare_paradigms: er_multiplex failed (",
                  conditionMessage(e), "); Strategy C_multiplex skipped.")
          NULL
        }
      )
      if (!is.null(labs_mp)) {
        elapsed_list[["C_multiplex"]] <- proc.time()[["elapsed"]] - t0
        pred_list[["C_multiplex"]]    <- labs_mp
        .msg("  done: %d clusters in %.1f s.",
             length(unique(labs_mp)), elapsed_list[["C_multiplex"]])
      }
    }
  }

  if (!length(pred_list))
    stop("er_compare_paradigms: no strategy produced results.")

  # ── Evaluate all strategies ─────────────────────────────────────────────────
  .msg("[er_compare_paradigms] Evaluating %d strategy/strategies...",
       length(pred_list))

  rows <- lapply(names(pred_list), function(s) {
    labs    <- pred_list[[s]]
    elapsed <- elapsed_list[[s]] %||% NA_real_

    ev <- er_evaluate(
      pred_list = list(result = labs),
      truth     = truth_tbl,
      id_vec    = id_vec,
      eval_mode = "labeled_only"
    )

    if (!nrow(ev)) {
      tibble::tibble(
        strategy    = paste0("Strategy_", s),
        ARI         = NA_real_, NMI = NA_real_, VI = NA_real_,
        Bcubed_P    = NA_real_, Bcubed_R = NA_real_, Bcubed_F = NA_real_,
        PairF_P     = NA_real_, PairF_R  = NA_real_, PairF_F  = NA_real_,
        n_clusters  = NA_integer_,
        elapsed_sec = round(elapsed, 2)
      )
    } else {
      tibble::tibble(
        strategy    = paste0("Strategy_", s),
        ARI         = ev$ARI,
        NMI         = ev$NMI,
        VI          = ev$VI,
        Bcubed_P    = ev$Bcubed_P,
        Bcubed_R    = ev$Bcubed_R,
        Bcubed_F    = ev$Bcubed_F,
        PairF_P     = ev$PairF_P,
        PairF_R     = ev$PairF_R,
        PairF_F     = ev$PairF_F,
        n_clusters  = length(unique(labs)),
        elapsed_sec = round(elapsed, 2)
      )
    }
  })

  out <- dplyr::bind_rows(rows)
  .msg("[er_compare_paradigms] Done.")
  out
}

# ── Internal: connected components from binary match matrix ────────────────────

# Takes a binary (0/1) sparse matrix M and returns an integer cluster-label
# vector of length n.  Records with no match edges are singletons.
.cc_from_binary <- function(match_mat, n) {
  tr    <- Matrix::summary(match_mat)
  edges <- tr[tr$i < tr$j & tr$x > 0, c("i", "j"), drop = FALSE]

  if (!nrow(edges)) return(seq_len(n))

  graph <- igraph::graph_from_data_frame(
    edges,
    directed = FALSE,
    vertices = data.frame(name = seq_len(n))
  )
  as.integer(igraph::components(graph)$membership)
}
