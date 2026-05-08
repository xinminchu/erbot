# erbot
**Entity Resolution Pipeline & Utilities (R package)**

An end-to-end toolkit for practical Entity Resolution (ER): feature building (TF-IDF+SVD, embeddings), multiple clustering methods (K-means, Louvain kNN, MST/SN+edit, HC, PAM), multi-field similarity graphs, auto-tuning, evaluation, and PDF reporting.

---

## Installation

# fastest, with retries and good error messages
install.packages("pak", repos = "https://r-lib.github.io/p/pak/stable")
pak::pkg_install("github::xinminchu/erbot")

# remotes fallback (avoid surprise upgrades)
remotes::install_github("xinminchu/erbot", upgrade = "never", build_vignettes = FALSE)


```r
# Core dependencies
install.packages(c(
  "data.table","dplyr","tibble","stringr","stringi","tidyr","purrr","readr","readxl",
  "text2vec","Matrix","irlba","stringdist","igraph","FNN","cluster","scales",
  "grid","grDevices","gridExtra","stats","utils","magrittr"
))

# Optional but recommended
install.packages("aricode")                 # ARI and related external metrics
remotes::install_github("ddegras/GCMER")    # agreement metrics & graph coloring

# Install erbot
remotes::install_github("xinminchu/erbot", upgrade = "never")

R ≥ 4.1 recommended.

Quick Start
1) One-liner pipeline (with tuning, eval & report)
library(erbot)
# Optional datasets & metrics:
# library(cora)   # provides `cora` and `cora_gold`
# library(GCMER)

res <- er_main(
  data        = "cora",                          # or a data.frame / file path
  truth       = cora_gold,                       # optional ground truth
  fields      = c("title","authors","address"),
  k_grid      = seq(10, 300, by = 10),
  write_csv   = "cora_clean_pred.csv",
  auto_plot   = TRUE,
  show_tables = TRUE,
  show_progress = TRUE
)

# Save a PDF report (params, tuning curves, performance tables)
er_save_report_pdf(res, "cora_report.pdf", dataset_name = "CORA", top_n = 5)

2) Light-weight comparison mode
res2 <- er_main_simple(
  data   = cora,
  truth  = cora_gold,
  fields = c("title","authors","address"),
  k_grid = 10:50,
  use_methods = c("kmeans_tfidf_svd","louvain_knn","louvain_multifield")
)
res2$chosen

3) Multi-field similarity → Louvain
df <- as.data.frame(cora)
spec <- list(
  list(name="title",   type="jw",   w=1),
  list(name="authors", type="jw",   w=1),
  list(name="address", type="jw",   w=1),
  list(name="year",    type="year", w=1, tau=0.5)
)
blk <- tolower(substr(df$title, 1, 1))      # simple blocking
S   <- er_similarity_multifield(df, spec, block_key = blk, top_k = 30)
labs <- er_louvain_from_S(S, min_sim = 0.0)

4) Learn field weights via CV and run
set.seed(42)
gw <- er_general_pipeline(
  data = cora, truth = cora_gold, id_col = "id",
  fields = c("title","authors","address"),
  learn_weights = TRUE, folds = 3, budget = 20
)
gw$weights

final <- run_with_weights(
  data = cora, truth = cora_gold,
  fields = c("title","authors","address"),
  weights = gw$weights,
  er_method = "kmeans",
  save_pdf = "cora_weighted.pdf", pdf_title = "CORA Weighted"
)


Note: Examples above may be compute-intensive on large datasets.

What’s Inside
Feature building

er_features_tfidf_svd() — TF-IDF → truncated SVD dense features

er_safe_parse_embedding_col() — parse serialized embeddings into a numeric matrix

Core methods

Centroidal: er_kmeans_from_X(), er_hclust_from_X(), er_pam_from_X()

Graph-based: er_louvain_knn() (kNN over features), er_louvain_from_S() (from multi-field similarity S)

Embeddings: er_embed_knn() (cosine threshold components)

Strings: er_mst_or_sn_edit() (Sorted-Neighborhood with edit-distance, optional MST pruning)

Multi-field similarity

er_similarity_multifield(df, spec, block_key, top_k)

Text: lev, jw, jaccard

Categorical, numeric, year (with decay tau)

Sparse output with symmetric top-k pruning

Pipelines & reporting

er_unified_pipeline() — end-to-end run (features → methods → tuning → eval → report)

er_main() — user-friendly facade with plots/tables

er_main_simple() — quick comparisons

er_save_report_pdf() — parameters, tuning curves, and performance tables to PDF

Evaluation & truth ingestion

er_truth_from_any() — ingest id+cluster tables, pair lists, named vectors, or file paths

er_pairs_to_clusters() — convert pair truth to clusters

er_eval_ca_one() — wrapper over GCMER::clustering_agreement

Utilities & I/O

er_load_input() — data.frame / CSV / Excel / "cora"

er_select_fields() — pick ID and build normalized text_for_matching

ncvr_read() / ncvr_guess_fields() — helpers for NC voter data

Progress: er_progress_start() / er_progress_tick() / er_progress_done()

Tips & Gotchas

%>% not found — ensure magrittr is installed; erbot imports it internally.

Agreement/graph-coloring — install GCMER if you use er_eval_ca_one() or graph coloring helpers.

Large data — prefer blocking (block_key) and top_k when building er_similarity_multifield(); keep svd_dim modest (e.g., 100).

No code at load time — erbot contains only functions; nothing runs on library(erbot).

Development
# From the package root
devtools::document()
devtools::load_all()
devtools::check(build_vignettes = FALSE, manual = FALSE)


If you modify roxygen headers, re-run devtools::document() to refresh NAMESPACE and help pages.

Citation

If you use erbot, please cite this repository and the underlying methods you select (e.g., Louvain, TF-IDF+SVD). If you rely on GCMER for agreement metrics or graph coloring, cite that package as well.

License

MIT — see LICENSE.

Contributing

Issues and pull requests are welcome. Ideas for next features: HDBSCAN, MinHash/LSH blocking, approximate nearest neighbors for large-scale kNN.

