#Examples, testing and running here

install.packages("devtools")
library(devtools)
devtools::install_github("https://github.com/ddegras/GCMER")

library(GCMER)
source(file.path("D:/erbot/R", "er_general_pipeline.R"))
#source(file.path("D:/erbot/R", "er_pipeline.R"))
#source(file.path("D:/erbot/R", "er_pipeline_weighted_fields.R"))

# 1) CORA
library(cora); library(igraph)
# edges <- data.frame(id1=as.character(cora_gold$id1), id2=as.character(cora_gold$id2))
# g_gt  <- graph_from_data_frame(edges, directed=FALSE)
# gt    <- igraph::components(g_gt)$membership; names(gt) <- names(igraph::components(g_gt)$membership)

res_cora <- er_main(
  data      = cora,
  truth     = cora_gold,                    # optional gold for ARI/F1 model selection
  fields    = c("title","authors","address"),
  k_grid    = seq(10, 300, by = 10),
  write_csv = "D:/erbot/data/cora_clean_pred.csv",
  # optional knobs (aligned defaults):
  svd_k = 100, knn_k = 15, louvain_min_sim = 0.0,
  use_methods = c("kmeans_tfidf_svd","louvain_knn","louvain_multifield")
)

er_save_report_pdf(res_cora, "D:/erbot/results/cora_report0918.pdf", "CORA")


res <- er_main(
  data = "cora",                 # or a CSV/XLSX path / data.frame
  truth = cora_gold,             # optional
  fields = c("title","authors","address"),
  k_grid = seq(10, 300, by = 10),
  write_csv = "D:/erbot/data/cora_only_kmeans.csv",
  er_methods = "kmeans",         # <<< run only KMeans
  auto_plot = FALSE
)


###########################################################
# CORA without truth (unsupervised selection on titles)
###########################################################
sel <- er_compare_methods_unsupervised(
  data = cora,
  fields = c("title","authors","address","date","year"),
  objective = "silhouette",
  k_grid = 1000,          # fine: the sweep caps to n_unique
  er_method = "kmeans",
  show_progress = TRUE
)







sel <- er_compare_methods_unsupervised(
  data = cora,
  fields = c("title","authors","address", "date", "year"), #get less missing
  objective = "silhouette",                       # or "silhouette", "modularity", ...
  k_grid = 1000,
  use_methods = "kmeans",
  knn_grid = seq(10, 500, by = 10),
  min_sim_grid = c(0.0, 0.05, 0.1),
  gc_thresholds = seq(0.01, 0.8, 0.02),
  embed_k_grid = seq(10, 500, by = 10),
  embed_cos_grid = c(0.85,0.88,0.90),
  show_progress = TRUE
)
# Unsupervised model selection — done. Total elapsed: 7139.4s

sel$leaderboard       # methods ranked by your objective
sel$per_method_best   # best params per method
sel$curves$kmeans     # full tuning curve for that method
print(sel$curves$kmeans, n = Inf)

plot(sel$curves$hclust$k, sel$curves$hclust$silhouette, type = "l")
plot(sel$curves$kmeans$k, sel$curves$kmeans$silhouette, type = "l")
plot(sel$curves$pam$k, sel$curves$pam$silhouette, type = "l")
plot(sel$curves$louvain_knn$knn_k, sel$curves$louvain_knn$silhouette, type = "l")
plot(sel$curves$gc$threshold, sel$curves$gc$silhouette, type = "l")

plot(sel$curves$kmeans$k, sel$curves$kmeans$db, type = "l")

best_k <- sel$per_method_best$kmeans$k
best_louvain <- sel$per_method_best$louvain_knn
best_gc      <- sel$per_method_best$gc


res <- er_main(
  data  = "cora",
  fields = c("title","authors","address", "date", "year"),
  # plug in selected params
  k_clusters = best_k,
  knn_k = best_louvain$knn_k %||% 15,
  louvain_min_sim = best_louvain$min_sim %||% 0.0,
  gc_thresholds = best_gc$threshold %||% seq(0.01, 0.8, 0.02),
  auto_tune = FALSE,  # we already tuned
  show_progress = TRUE
)

res


####################################################
# Cora with different combination of fields
####################################################
# Run A
system.time(res_cora <- er_main(data=cora,
                                truth=cora_gold, #truth=gt,
                                fields=c("title","authors","address"),
                                k_grid=seq(10, 300, by = 10),
                                write_csv="D:/erbot/data/cora_clean_pred.csv"))
# user  system elapsed
# 1724.75   22.78 1675.83

er_save_report_pdf(res_cora, "D:/erbot/results/cora_report2025091802.pdf", "CORA")

# Run B
system.time(res_cora <- er_main(data=cora,
                                truth=cora_gold, #truth=gt,
                                fields=c("title","authors","address", "date", "year"),
                                k_grid=seq(10, 300, by = 10),
                                write_csv="D:/erbot/data/cora_clean_pred2.csv"))
# user  system elapsed
# 1977.89   24.53 1917.10

er_save_report_pdf(res_cora, "D:/erbot/results/cora_report2025091803.pdf", "CORA")

# Run C
system.time(res_cora <- er_main(data=cora,
                                truth=cora_gold, #truth=gt,
                                fields=c("title","authors","address", "year"),
                                k_grid=seq(10, 300, by = 10),
                                write_csv="D:/erbot/data/cora_clean_pred3.csv"))
# user  system elapsed
# 1801.14   21.35 1750.16

er_save_report_pdf(res_cora, "D:/erbot/results/cora_report2025091804.pdf", "CORA")


# Run D: Modified
# --- Setup ---------------------------------------------------------------
library(dplyr)
library(stringr)   # for str_extract

# Use CORA data frame. If it's not a data.frame/tibble yet, coerce it:
df <- as.data.frame(cora)

# --- 1) Robust year extraction from messy `date` -------------------------
# Strategy:
#   - Prefer the explicit `year` column if it's already numeric-like.
#   - Otherwise extract a 4-digit year from `date` free text (e.g., "March 1997" -> 1997).
#   - Drop out-of-range years (e.g., <1800 or >2030) to avoid garbage.
df <- df %>%
  mutate(
    # normalize inputs to character to avoid factor issues
    date_chr = if ("date" %in% names(df)) as.character(date) else NA_character_,
    year_chr = if ("year" %in% names(df)) as.character(year) else NA_character_,

    # try to coerce existing `year` into integer
    year_from_col = suppressWarnings(as.integer(year_chr)),

    # regex-pull a 4-digit year from `date` text (returns NA if none)
    year_from_date = suppressWarnings(as.integer(str_extract(date_chr, "\\b(\\d{4})\\b"))),

    # prefer explicit column; else use extracted-from-date
    year_clean = dplyr::coalesce(year_from_col, year_from_date),

    # keep only plausible years to reduce noise
    year_clean = ifelse(!is.na(year_clean) & year_clean >= 1800 & year_clean <= 2030,
                        year_clean, NA_integer_)
  )

# --- 2) Build a 5-year bucket (e.g., "1995-1999"); mark missing as "year_unknown" ----
df <- df %>%
  mutate(
    year_bucket5 = ifelse(
      !is.na(year_clean),
      paste0(floor(year_clean / 5) * 5, "-", floor(year_clean / 5) * 5 + 4),
      "year_unknown"
    )
  )

# (Optional) Quick check of what you got:
print(table(df$year_bucket5, useNA = "ifany"))

# --- 3) Run ER with bucketed year ---------------------------------------
# Start with your established grid; you can narrow it later if needed.
res_cora <- er_main(
  data      = df,
  truth     = cora_gold,                 # or your ground-truth object
  fields    = c("title", "authors", "address", "year_bucket5"),
  k_grid    = seq(10, 300, by = 10),
  write_csv = "D:/erbot/data/cora_clean_pred_bucketed.csv"
)

# Total elapsed: 1798.5s
er_save_report_pdf(res_cora, "D:/erbot/results/cora_report2025091805.pdf", "CORA")

# --- 4) (Optional) Variants you might try quickly -----------------------
# A) Use raw year instead of 5-year buckets:
# res_year <- er_main(
#   data      = df,
#   truth     = cora_gold,
#   fields    = c("title", "authors", "address", "year_clean"),
#   k_grid    = seq(10, 300, by = 10),
#   write_csv = "D:/erbot/data/cora_clean_pred_year.csv"
# )
# er_save_report_pdf(res_year, "D:/erbot/results/cora_report_year.pdf", "CORA")

# B) If temporal features are included, constrain k to avoid over-fragmentation:
# res_bucketed_kcap <- er_main(
#   data      = df,
#   truth     = cora_gold,
#   fields    = c("title", "authors", "address", "year_bucket5"),
#   k_grid    = seq(60, 120, by = 10),   # narrower band
#   write_csv = "D:/erbot/data/cora_clean_pred_bucketed_kcap.csv"
# )
# er_save_report_pdf(res_bucketed_kcap, "D:/erbot/results/cora_report_bucketed_kcap.pdf", "CORA")

# C) (Only if your er_main supports field weights) down-weight the temporal feature:
# res_weighted <- er_main(
#   data          = df,
#   truth         = cora_gold,
#   fields        = c("title", "authors", "address", "year_bucket5"),
#   field_weights = c(title = 1.0, authors = 1.2, address = 0.8, year_bucket5 = 0.3),
#   k_grid        = seq(10, 300, by = 10),
#   write_csv     = "D:/erbot/data/cora_clean_pred_bucketed_weighted.csv"
# )
# er_save_report_pdf(res_weighted, "D:/erbot/results/cora_report_bucketed_weighted.pdf", "CORA")


# Run E: impute missing in year with last 4 digits of date

library(dplyr)
library(stringr)

# Use your CORA data frame
df <- as.data.frame(cora)

# Helper: last 4-digit sequence in a string (returns NA if none)
last4_from_str <- function(x) {
  if (is.na(x)) return(NA_integer_)
  m <- stringr::str_extract_all(x, "\\d{4}")[[1]]
  if (length(m)) as.integer(tail(m, 1)) else NA_integer_
}

# Vectorize over the column
last4_vec <- vapply(as.character(df$date), last4_from_str, integer(1))

# Build the new year column:
df <- df %>%
  mutate(
    year_num  = suppressWarnings(as.integer(as.character(year))),
    year_imp  = ifelse(!is.na(year_num), year_num, last4_vec),
    # keep only plausible years
    year_imp  = ifelse(!is.na(year_imp) & year_imp >= 1800 & year_imp <= 2030,
                       year_imp, NA_integer_)
  ) %>%
  select(-year_num)  # cleanup helper

# Quick check
table(is.na(df$year_imp))
head(df[, c("date","year","year_imp")], 10)

res_cora <- er_main(
  data      = df,
  truth     = cora_gold,                 # or your ground-truth object
  fields    = c("title", "authors", "address", "year_imp"),
  k_grid    = seq(10, 300, by = 10),
  write_csv = "D:/erbot/data/cora_clean_pred_imp.csv"
)

# Total elapsed: 2205.7s
er_save_report_pdf(res_cora, "D:/erbot/results/cora_report2025091806.pdf", "CORA")



####################
# Weighted fields
###################

fields_use <- c("title","authors","address","year_new")
user_w     <- c(title=0.45, authors=0.35, address=0.15, year_new=0.05)

fields_use <- colnames(cora)[-1]
user_w     <- c(title=0.45, authors=0.35, address=0.15, date=0, year=0.05,
                editor=0, journal=0, volume=0, pages=0, publisher=0,
                institution=0, type=0, tech=0, note=0)

out <- er_general_pipeline(
  data = cora, truth = cora_gold, id_col = "id",
  fields = fields_use, weights = user_w, learn_weights = FALSE,
  er_method = "kmeans", k_grid = seq(10, 300, by=10), base_rep = 12,
  write_csv = "D:/erbot/data/cora_pred_weighted.csv",
  save_pdf  = "D:/erbot/results/cora_report_weighted.pdf",
  pdf_title = "CORA — Weighted Fields"
)
out$final$adj_rand  # ARI of the final run
out$final$weights   # normalized weights actually used


###################################
# 2) Affiliation
###################################
system.time(res_aff <- er_main(data="D:/erbot/data/affiliationstrings_ids.csv",
                               truth="D:/erbot/data/affiliationstrings_mapping.csv",
                               fields="affil1",
                               k_grid=seq(10, 500, by = 10),
                               write_csv="D:/erbot/data/affiliation_pred.csv"))
# user  system elapsed
# 432.61   16.37  414.10
er_save_report_pdf(res_aff, "D:/erbot/results/affiliation_report.pdf", "Affiliation")

# 3) D10K
res_d10k <- er_main(data="D:/erbot/data/10kfull.csv",
                    fields=c("clean ag.value","aggregate value"),
                    embed_col="embedded clean ag.value",
                    knn_k=15, cos_thresh=0.88, auto_tune=FALSE,
                    write_csv="D:/erbot/data/D10K_pred.csv")
er_save_report_pdf(res_d10k, "D:/erbot/results/D10K_report.pdf", "D10K")

# 4) NCVR (10-way)
df10 <- ncvr_read("D:/ncvr/10Party-ocp20", which="10")
fields10 <- ncvr_guess_fields(df10)  # can be NULL (auto-guess)
res_nc10 <- er_main(data=df10, fields=fields10, k_clusters=60, knn_k=25, auto_tune=TRUE,
                    write_csv="D:/erbot/data/ncvr10_pred.csv", show_progress=TRUE)
er_save_report_pdf(res_nc10, "D:/erbot/results/ncvr10_report.pdf", "NCVR 10-way")


df5 <- ncvr_read("D:/ncvr/5Party-ocp20", which="5")
fields5 <- ncvr_guess_fields(df5)  # can be NULL (auto-guess)
res_nc5 <- er_main(data=df5, fields=fields5, k_clusters=60, knn_k=25, auto_tune=TRUE,
                    write_csv="D:/erbot/data/ncvr5_pred.csv", show_progress=TRUE)
er_save_report_pdf(res_nc5, "D:/erbot/results/ncvr5_report.pdf", "NCVR 5-way")






# # build truth from pair list
# edges <- data.frame(id1 = as.character(cora_gold$id1), id2 = as.character(cora_gold$id2))
# g_gt <- graph_from_data_frame(edges, directed = FALSE)
# gt <- igraph::components(g_gt)$membership; names(gt) <- names(igraph::components(g_gt)$membership)

res_cora <- er_main(
  data  = "cora",
  truth = cora_gold,                                   # or pass truth = cora_gold (pair list)
  fields = c("title","authors","address"), # any combination you like
  k_grid = c(5,10,15,20),
  write_csv = "D:/erbot/data/cora_clean_pred.csv"
)


er_save_report_pdf(res_cora, file="D:/erbot/results/cora_report.pdf", dataset_name="CORA", top_n=5)



res_aff <- er_main(
  data  = "D:/erbot/data/affiliationstrings_ids.csv",
  truth = "D:/erbot/data/affiliationstrings_mapping.csv",  # pair list OK
  fields = "affil1",                                       # or "affiliation"
  write_csv = "D:/erbot/data/clean_affiliations.csv"
)

er_save_report_pdf(res_aff, file="D:/erbot/results/aff_report.pdf", dataset_name="AFFILIATION", top_n=5)


library(data.table); library(dplyr)

# 1) Read with the correct delimiter and quoting
d10k <- fread("D:/erbot/data/10Kfull.csv",
              sep = "|", quote = "\"", header = TRUE, fill = TRUE, showProgress = TRUE)

# 2) Normalize column names
names(d10k) <- tolower(names(d10k))

# 3) Run the unified pipeline on the in-memory data.frame
res_d10k0 <- er_main(
  data = as.data.frame(d10k),                       # pass DF, not path
  truth = "D:/erbot/data/10Kduplicates.csv",
  fields = c("clean ag.value","aggregate value"),   # pick your text fields
  embed_col = "embedded clean ag.value",            # auto-detected if you omit
  knn_k = 15, k_clusters = 10, cos_thresh = 0.88,
  write_csv = "D:/erbot/data/D10K_clean_pred.csv"
)

er_save_report_pdf(res_d10k0, file="D:/erbot/results/d10k0_report.pdf", dataset_name="D10K", top_n=5)

# loader auto-fixes 1-column pipe files; or read explicitly with sep="|"
res_d10k <- er_main(
  data = "D:/erbot/data/10kfull.csv",
  fields = c("clean ag.value","aggregate value"),
  embed_col = "embedded clean ag.value",
  knn_k = 15, cos_thresh = 0.88,
  auto_tune = FALSE,                 # usually no truth for 10k
  write_csv = "D:/erbot/data/D10K_clean_pred.csv"
)

er_save_report_pdf(res_d10k, file="D:/erbot/results/d10k_report.pdf", dataset_name="D10K", top_n=5)

# If you know the columns, pass them:
res_nc5 <- er_main(
  data = data.table::rbindlist(
    lapply(list.files("D:/erbot/data/5Party-ocp20", pattern = "^ncvr_.*_nump_5\\.csv$", full.names = TRUE),
           data.table::fread, showProgress = TRUE),
    use.names = TRUE, fill = TRUE),
  fields = c("givenname","surname","suburb","postcode"),
  k_clusters = 50, knn_k = 20, auto_tune = TRUE,
  write_csv = "D:/erbot/data/ncvr5_pred.csv"
)

# To be continue


