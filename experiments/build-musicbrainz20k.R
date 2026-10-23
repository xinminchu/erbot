# BUILD musicbrainz20k data files for the E1/E2 experiment scripts.
#
# The erbot package has NO er_build_musicbrainz() and er_load() does not know
# a "musicbrainz20k" key. The raw material on disk is
#   data/raw/musicbrainz-20000-A01.csv.zip   (one CSV inside, ~2.3 GB raw)
# This script converts it, once, into the two files the experiments read:
#   data/musicbrainz20k.csv        records: id + attribute columns
#   data/musicbrainz20k_gold.csv   id, cluster_id
#
# The real header (seen 2026-10-20) is:
#   TID | CID | CTID | SourceID | id | number | title | length | artist |
#   album | year | language
# The gold cluster column is an abbreviation (CID / CTID), which the generic
# detector cannot certify. So: with no arguments this script prints grouping
# statistics for every candidate column and stops. Once the gold column is
# confirmed from those statistics, run it with explicit arguments:
#
#   Rscript experiments/build-musicbrainz20k.R <id_col> <cluster_col> [target_n]
#   e.g. Rscript experiments/build-musicbrainz20k.R id CID 20000
#
# DISCOVERY (2026-10-20, from the statistics run): the "20000" file actually
# holds 19,375,000 records / 10,000,000 CID clusters (max size 5, 16.25M
# matched pairs) — a full-scale dataset, not a 20k benchmark. Gold = CID is
# structurally certain (TID is all-unique; CTID is low-cardinality). With a
# target_n argument the builder samples WHOLE clusters (seeded) until the
# record count reaches target_n, and writes the sample as musicbrainz20k;
# without target_n it would write the full 19.4M rows (not recommended).
#
# It never writes data files on an inferred mapping.
#
# The extracted CSV is cached at data/raw/musicbrainz-20000-A01.csv so
# repeated runs do not re-unzip 2.3 GB.

args <- commandArgs(trailingOnly = TRUE)
zip_path <- file.path("data", "raw", "musicbrainz-20000-A01.csv.zip")
raw_path <- file.path("data", "raw", "musicbrainz-20000-A01.csv")

if (!file.exists(raw_path)) {
  if (!file.exists(zip_path))
    stop("not found: ", zip_path, " (run from the erbot package root)")
  entries  <- utils::unzip(zip_path, list = TRUE)
  csv_name <- entries$Name[grepl("\\.csv$", entries$Name)][1]
  if (is.na(csv_name)) stop("no .csv inside ", zip_path)
  utils::unzip(zip_path, files = csv_name, exdir = file.path("data", "raw"))
  message("extracted to: ", raw_path)
}
message("raw size: ", round(file.info(raw_path)$size / 1e6), " MB")

hdr <- names(data.table::fread(raw_path, nrows = 0L, showProgress = FALSE))
message("columns found: ", paste(hdr, collapse = " | "))

if (length(args) >= 2L) {
  id_col <- args[1]; cl_col <- args[2]
  if (!(id_col %in% hdr)) stop("id column '", id_col, "' not in header")
  if (!(cl_col %in% hdr)) stop("cluster column '", cl_col, "' not in header")
  message("mapping (from arguments): id = '", id_col, "' | cluster = '", cl_col, "'")
  df <- data.table::fread(raw_path, showProgress = FALSE)
  if (length(args) >= 3L) {
    target_n <- as.integer(args[3])
    set.seed(20261020L)
    cl_all <- df[[cl_col]]
    sizes  <- table(cl_all)
    ord    <- sample.int(length(sizes))
    k      <- which(cumsum(sizes[ord]) >= target_n)[1]
    pick   <- names(sizes)[ord[seq_len(k)]]
    df <- df[as.character(cl_all) %in% pick]
    message(sprintf("cluster-sampled to %d records (target %d, seed 20261020)",
                    nrow(df), target_n))
  }
  ids <- as.character(df[[id_col]])
  if (anyDuplicated(ids)) warning("record ids are not unique: ", sum(duplicated(ids)), " duplicates")
  gold <- tibble::tibble(id = ids,
                         cluster_id = as.integer(factor(df[[cl_col]])))
  drop <- cl_col
  if (id_col != "id" && "id" %in% names(df)) drop <- c(drop, "id")
  recs <- df[, !(names(df) %in% drop), with = FALSE]
  names(recs)[names(recs) == id_col] <- "id"
  names(recs) <- tolower(names(recs))
  data.table::fwrite(recs, file.path("data", "musicbrainz20k.csv"))
  data.table::fwrite(gold, file.path("data", "musicbrainz20k_gold.csv"))
  tab <- table(gold$cluster_id)
  message("WROTE data/musicbrainz20k.csv + data/musicbrainz20k_gold.csv")
  message(sprintf("records = %d | entities = %d | matched pairs = %d | singletons = %d",
                  nrow(recs), length(tab), sum(choose(tab, 2L)), sum(tab == 1L)))
} else {
  cand <- intersect(c("TID", "CID", "CTID", "SourceID", "id"), hdr)
  message("--- candidate column statistics (no files written) ---")
  d <- data.table::fread(raw_path, select = cand, showProgress = FALSE)
  message("rows: ", nrow(d))
  for (cn in cand) {
    v <- d[[cn]]
    tab <- table(v)
    sz <- as.integer(tab)
    message(sprintf("%-9s distinct=%-8d groups>=2=%-8d max_group=%-5d matched_pairs=%.0f",
                    cn, length(tab), sum(sz >= 2L), max(sz),
                    sum(choose(as.numeric(sz), 2))))
  }
  stop("mapping not certified. Read the statistics: the gold column has ",
       "many multi-record groups and a plausible matched-pairs count. ",
       "Then rerun with arguments, e.g. Rscript experiments/build-musicbrainz20k.R id CID")
}
