unlink("NAMESPACE")

# From the package root
renv::activate()          # if you use renv
devtools::document()      # regenerates NAMESPACE + Rd
devtools::load_all()      # reloads the pkg
getNamespaceExports("erbot")

readLines("NAMESPACE")




# ---- Unload if loaded
if ("erbot" %in% loadedNamespaces()) try(unloadNamespace("erbot"), silent = TRUE)
suppressWarnings(try(detach("package:erbot", unload = TRUE, character.only = TRUE), silent = TRUE))

# ---- Remove from each library in .libPaths()
suppressWarnings(try(remove.packages("erbot"), silent = TRUE))
for (lib in .libPaths()) {
  p <- file.path(lib, "erbot")
  if (dir.exists(p)) unlink(p, recursive = TRUE, force = TRUE)
}

# ---- Remove renv-cached copies (common cache locations on Windows)
paths_to_check <- c(
  file.path(Sys.getenv("LOCALAPPDATA"), "R", "cache", "R", "renv", "library"),
  file.path(Sys.getenv("LOCALAPPDATA"), "renv", "cache"),
  file.path(Sys.getenv("APPDATA"), "R", "renv", "library") # extra safety
)
for (root in paths_to_check) {
  if (dir.exists(root)) {
    cand <- list.files(root, pattern = "^erbot", full.names = TRUE, recursive = TRUE)
    if (length(cand)) unlink(cand, recursive = TRUE, force = TRUE)
  }
}

# ---- Sanity check: no installed 'erbot' remains
suppressWarnings(try(find.package("erbot"), silent = TRUE))


if (requireNamespace("renv", quietly = TRUE)) {
  try(renv::cache_clean(packages = "erbot"), silent = TRUE)
}


setwd("D:/erbot")  # <- adjust to your real path
options(repos = c(CRAN = "https://cran.r-project.org"))

devtools::document()              # rebuild NAMESPACE & man/ from source
devtools::load_all(reset = TRUE)  # load from source only (no cached rdb)

devtools::install(upgrade = "never", force = TRUE)

library(erbot)
library(cora)
library(GCMER)

res <- er_main(
  data   = "cora",
  truth  = cora_gold,
  fields = c("title","authors","address"),
  k_grid = seq(10, 300, by = 10),
  write_csv = "D:/erbot/data/cora_clean_pred.csv"
)

rand_digits <- sprintf("%03d", sample(0:999, 1))
file_name <- paste0("D:/erbot/results/cora_report", format(Sys.Date(), "%Y%m%d"), rand_digits, ".pdf")
er_save_report_pdf(res, file = file_name, dataset_name = "CORA", top_n = 5)

