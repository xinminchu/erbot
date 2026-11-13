########################################
# File: R/14-datasets.R
#
# Builder functions for the Restaurant (Fodors-Zagats) and DBLP-ACM benchmarks.
#
# Both datasets follow the DeepMatcher two-table format:
#   - tableA.csv / tableB.csv  (records, with an integer 'id' column)
#   - matches.csv              (two columns: ltable_id, rtable_id)
#
# The builders merge the two tables into a single combined CSV (with a 'source'
# column so erbot's linkage mode activates) and write a gold CSV in erbot's
# standard (id, cluster_id) format (matching pairs share a cluster_id;
# unmatched records get unique singleton cluster_ids).
#
# Download sources:
#   Restaurant: https://github.com/anhaidgroup/deepmatcher/tree/master/Datasets
#               Structured/Dirty/Fodors-Zagats/
#   DBLP-ACM:  https://github.com/anhaidgroup/deepmatcher/tree/master/Datasets
#               Structured/DBLP-ACM/
#
# Public API
# ----------
#   er_build_restaurant()  -- build data/restaurant.csv + data/restaurant_gold.csv
#   er_build_dblp_acm()    -- build data/dblp_acm.csv  + data/dblp_acm_gold.csv
#   er_load_gold()         -- load ground truth for any built-in benchmark
########################################


# ── Internal: union-find for cluster assignment ───────────────────────────────

.uf_make   <- function(n) seq_len(n)

.uf_find   <- function(parent, i) {
  while (parent[i] != i) i <- parent[i]
  i
}

.uf_union  <- function(parent, i, j) {
  ri <- .uf_find(parent, i)
  rj <- .uf_find(parent, j)
  if (ri != rj) parent[rj] <- ri
  parent
}

.uf_labels <- function(parent, n) {
  roots <- vapply(seq_len(n), function(i) .uf_find(parent, i), integer(1L))
  match(roots, unique(roots))
}


# ── Internal: build combined + gold from two tables + matches ─────────────────

.build_two_table <- function(tableA, tableB, matches,
                             source_a, source_b,
                             id_col = "id") {
  # Prefix record IDs to avoid collision across tables
  tableA[[id_col]] <- paste0("A_", tableA[[id_col]])
  tableB[[id_col]] <- paste0("B_", tableB[[id_col]])

  tableA$source <- source_a
  tableB$source <- source_b

  # Align columns: union of both tables; missing fields become NA
  all_cols <- union(names(tableA), names(tableB))
  for (col in setdiff(all_cols, names(tableA))) tableA[[col]] <- NA_character_
  for (col in setdiff(all_cols, names(tableB))) tableB[[col]] <- NA_character_
  combined <- rbind(tableA[, all_cols], tableB[, all_cols])
  combined  <- tibble::as_tibble(combined)

  # Build cluster_id via union-find over matching pairs
  n      <- nrow(combined)
  id_vec <- combined[[id_col]]
  parent <- .uf_make(n)

  for (k in seq_len(nrow(matches))) {
    left_id  <- paste0("A_", matches$ltable_id[k])
    right_id <- paste0("B_", matches$rtable_id[k])
    li <- match(left_id,  id_vec)
    ri <- match(right_id, id_vec)
    if (!is.na(li) && !is.na(ri))
      parent <- .uf_union(parent, li, ri)
  }

  combined$cluster_id <- .uf_labels(parent, n)

  # Gold: (id, cluster_id)
  gold <- tibble::tibble(
    id         = combined[[id_col]],
    cluster_id = combined$cluster_id
  )

  list(combined = combined, gold = gold)
}


# ── er_build_restaurant() ────────────────────────────────────────────────────

#' Build the Restaurant (Fodors-Zagats) benchmark files
#'
#' Merges the two source tables and matches file (DeepMatcher format) into
#' \file{data/restaurant.csv} and \file{data/restaurant_gold.csv}.  After
#' building, \code{er_load("restaurant")} and
#' \code{er_load_gold("restaurant")} will work.
#'
#' Fields retained: \code{id, source, name, addr, city, phone, type}.
#'
#' @section Data source:
#' Download \file{tableA.csv}, \file{tableB.csv}, and \file{matches.csv} from
#' the DeepMatcher benchmark repository (Structured/Dirty/Fodors-Zagats/).
#'
#' @param tableA_path Path to Fodors CSV (\code{id, name, addr, city, phone, type}).
#' @param tableB_path Path to Zagats CSV (\code{id, name, addr, city, phone, type}).
#' @param matches_path Path to matches CSV (\code{ltable_id, rtable_id}).
#' @param out_dir Directory to write output files (default: \code{data/} under
#'   the package root, or current working directory if not found).
#' @param verbose Logical.
#' @return Invisibly returns a named list with \code{combined} and \code{gold}
#'   tibbles.
#' @seealso \code{\link{er_load}}, \code{\link{er_load_gold}},
#'   \code{\link{er_build_dblp_acm}}
#' @export
er_build_restaurant <- function(tableA_path, tableB_path, matches_path,
                                 out_dir = NULL, verbose = TRUE) {
  ts <- function(...) if (verbose) message(sprintf("[er_build_restaurant] %s", paste0(...)))

  ts("Reading Fodors (tableA)...")
  tA <- tibble::as_tibble(data.table::fread(tableA_path, showProgress = FALSE))
  names(tA) <- tolower(names(tA))

  ts("Reading Zagats (tableB)...")
  tB <- tibble::as_tibble(data.table::fread(tableB_path, showProgress = FALSE))
  names(tB) <- tolower(names(tB))

  ts("Reading matches...")
  m_raw <- if (length(matches_path) > 1L) {
    data.table::rbindlist(
      lapply(matches_path, data.table::fread, showProgress = FALSE),
      use.names = TRUE, fill = TRUE)
  } else {
    data.table::fread(matches_path, showProgress = FALSE)
  }
  m <- tibble::as_tibble(m_raw)
  names(m) <- tolower(names(m))
  if (!all(c("ltable_id", "rtable_id") %in% names(m)))
    stop("matches_path file(s) must have columns 'ltable_id' and 'rtable_id'.")
  if ("label" %in% names(m)) m <- m[m$label == 1L, ]

  ts(sprintf("Building combined table (%d Fodors + %d Zagats, %d match pairs)...",
             nrow(tA), nrow(tB), nrow(m)))
  out <- .build_two_table(tA, tB, m,
                           source_a = "fodors", source_b = "zagats",
                           id_col   = "id")

  # Determine output directory
  if (is.null(out_dir)) {
    pkg_root <- tryCatch(
      normalizePath(file.path(dirname(sys.frame(0)$ofile), "..")),
      error = function(e) getwd()
    )
    out_dir <- file.path(pkg_root, "data")
    if (!dir.exists(out_dir)) out_dir <- getwd()
  }

  data_path <- file.path(out_dir, "restaurant.csv")
  gold_path <- file.path(out_dir, "restaurant_gold.csv")

  ts(sprintf("Writing %s ...", data_path))
  data.table::fwrite(out$combined, data_path)

  ts(sprintf("Writing %s ...", gold_path))
  data.table::fwrite(out$gold, gold_path)

  n_match    <- nrow(m)
  n_entities <- length(unique(out$gold$cluster_id))
  ts(sprintf("Done. %d records, %d entities (%d matched pairs, %d singletons).",
             nrow(out$combined), n_entities, n_match,
             n_entities - n_match))

  invisible(out)
}


# ── er_build_dblp_acm() ──────────────────────────────────────────────────────

#' Build the DBLP-ACM benchmark files
#'
#' Merges the two source tables and matches file (DeepMatcher format) into
#' \file{data/dblp_acm.csv} and \file{data/dblp_acm_gold.csv}.  After
#' building, \code{er_load("dblp_acm")} and
#' \code{er_load_gold("dblp_acm")} will work.
#'
#' Fields retained: \code{id, source, title, authors, venue, year}.
#'
#' @section Data source:
#' Download \file{tableA.csv}, \file{tableB.csv}, and \file{matches.csv} from
#' the DeepMatcher benchmark repository (Structured/DBLP-ACM/).
#'
#' @param tableA_path Path to DBLP CSV (\code{id, title, authors, venue, year}).
#' @param tableB_path Path to ACM CSV (\code{id, title, authors, venue, year}).
#' @param matches_path Path to matches CSV (\code{ltable_id, rtable_id}).
#' @param out_dir Directory to write output files (default: \code{data/} under
#'   the package root, or current working directory if not found).
#' @param verbose Logical.
#' @return Invisibly returns a named list with \code{combined} and \code{gold}
#'   tibbles.
#' @seealso \code{\link{er_load}}, \code{\link{er_load_gold}},
#'   \code{\link{er_build_restaurant}}
#' @export
er_build_dblp_acm <- function(tableA_path, tableB_path, matches_path,
                               out_dir = NULL, verbose = TRUE) {
  ts <- function(...) if (verbose) message(sprintf("[er_build_dblp_acm] %s", paste0(...)))

  ts("Reading DBLP (tableA)...")
  tA <- tibble::as_tibble(data.table::fread(tableA_path, showProgress = FALSE))
  names(tA) <- tolower(names(tA))

  ts("Reading ACM (tableB)...")
  tB <- tibble::as_tibble(data.table::fread(tableB_path, showProgress = FALSE))
  names(tB) <- tolower(names(tB))

  ts("Reading matches...")
  m_raw <- if (length(matches_path) > 1L) {
    data.table::rbindlist(
      lapply(matches_path, data.table::fread, showProgress = FALSE),
      use.names = TRUE, fill = TRUE)
  } else {
    data.table::fread(matches_path, showProgress = FALSE)
  }
  m <- tibble::as_tibble(m_raw)
  names(m) <- tolower(names(m))
  if (!all(c("ltable_id", "rtable_id") %in% names(m)))
    stop("matches_path file(s) must have columns 'ltable_id' and 'rtable_id'.")
  if ("label" %in% names(m)) m <- m[m$label == 1L, ]

  ts(sprintf("Building combined table (%d DBLP + %d ACM, %d match pairs)...",
             nrow(tA), nrow(tB), nrow(m)))
  out <- .build_two_table(tA, tB, m,
                           source_a = "dblp", source_b = "acm",
                           id_col   = "id")

  # Determine output directory
  if (is.null(out_dir)) {
    pkg_root <- tryCatch(
      normalizePath(file.path(dirname(sys.frame(0)$ofile), "..")),
      error = function(e) getwd()
    )
    out_dir <- file.path(pkg_root, "data")
    if (!dir.exists(out_dir)) out_dir <- getwd()
  }

  data_path <- file.path(out_dir, "dblp_acm.csv")
  gold_path <- file.path(out_dir, "dblp_acm_gold.csv")

  ts(sprintf("Writing %s ...", data_path))
  data.table::fwrite(out$combined, data_path)

  ts(sprintf("Writing %s ...", gold_path))
  data.table::fwrite(out$gold, gold_path)

  n_match    <- nrow(m)
  n_entities <- length(unique(out$gold$cluster_id))
  ts(sprintf("Done. %d records, %d entities (%d matched pairs, %d singletons).",
             nrow(out$combined), n_entities, n_match,
             n_entities - n_match))

  invisible(out)
}


# ── er_load_gold() ────────────────────────────────────────────────────────────

#' Load ground truth for a built-in benchmark
#'
#' Returns a two-column tibble \code{(id, cluster_id)} suitable for passing
#' as the \code{truth} argument throughout the ERBOT pipeline.
#'
#' @param dataset Character.  One of \code{"cora"}, \code{"restaurant"},
#'   \code{"dblp_acm"}.  (Affiliation and D10K use the id-mapping files
#'   directly; NCVR ground truth is embedded in the partition CSVs.)
#' @return A tibble with columns \code{id} (character) and
#'   \code{cluster_id} (integer).
#' @export
er_load_gold <- function(dataset) {
  key <- trimws(tolower(dataset))

  .find_gold <- function(fname, candidates = character()) {
    pkg_path <- tryCatch(
      system.file("extdata", fname, package = "erbot", mustWork = FALSE),
      error = function(e) ""
    )
    if (nchar(pkg_path) > 0 && file.exists(pkg_path)) return(pkg_path)
    here <- tryCatch(
      normalizePath(file.path(dirname(sys.frame(0)$ofile), "..")),
      error = function(e) "."
    )
    p <- file.path(here, "inst", "extdata", fname)
    if (file.exists(p)) return(p)
    for (nm in c(fname, candidates)) {
      p2 <- file.path(here, "data", nm)
      if (file.exists(p2)) return(p2)
    }
    stop("Gold file not found: ", fname,
         ". Run er_build_", gsub("-", "_", key), "() first.")
  }

  path <- switch(key,
    cora       = .find_gold("cora_gold.csv"),
    restaurant = .find_gold("restaurant_gold.csv"),
    dblp_acm   = .find_gold("dblp_acm_gold.csv"),
    stop("er_load_gold: unknown dataset '", dataset,
         "'. Supported: cora, restaurant, dblp_acm.")
  )

  # Route through er_truth_from_any so both formats are accepted:
  #   (id, cluster_id)  — standard cluster-label format
  #   (id1, id2)        — pair-list format (e.g. cora_gold.csv from GCMER)
  er_truth_from_any(path)
}
