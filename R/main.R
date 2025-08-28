#Examples, testing and running here

library(cora); library(igraph)
source(file.path("D:/erbot/R", "er_pipeline.R"))


res_cora <- er_main(
  data = "cora",                # loader knows how to fetch from the package
  truth = cora_gold,                   # or truth = cora_gold (pair list)
  fields = c("title","authors", "address"),  # any combination you like
  k_clusters = 10, knn_k = 10,
  write_csv = "D:/erbot/data/cora_clean_pred.csv"
)


res_aff <- er_main(
  data = "D:/erbot/data/affiliationstrings_ids.csv",
  truth = "D:/erbot/data/affiliationstrings_mapping.csv",   # id+cluster or pair list
  fields = "affil1",  # if parsed; else "affiliation" alone
  k_clusters = 10, knn_k = 10,
  write_csv = "D:/erbot/data/clean_affiliations.csv"
)


library(data.table); library(dplyr)

# 1) Read with the correct delimiter and quoting
d10k <- fread("D:/erbot/data/10Kfull.csv",
              sep = "|", quote = "\"", header = TRUE, fill = TRUE, showProgress = TRUE)

# 2) Normalize column names
names(d10k) <- tolower(names(d10k))

# 3) Run the unified pipeline on the in-memory data.frame
res_d10k <- er_main(
  data = as.data.frame(d10k),                       # pass DF, not path
  truth = "D:/erbot/data/10Kduplicates.csv",
  fields = c("clean ag.value","aggregate value"),   # pick your text fields
  embed_col = "embedded clean ag.value",            # auto-detected if you omit
  knn_k = 15, k_clusters = 10, cos_thresh = 0.88,
  write_csv = "D:/erbot/data/D10K_clean_pred.csv"
)





