
# from the package root
# setwd("D:/erbot")
# usethis::use_build_ignore("README.Rmd")
#
# # Ensure DESCRIPTION has magrittr in Imports
# usethis::use_package("magrittr", type = "Imports")
#
# # Rebuild docs / NAMESPACE
#
devtools::document()
# devtools::check()
#
# options(repos = c(CRAN = "https://cran.r-project.org"))
# install.packages("aricode")   # so check won't complain




setwd("D:/erbot")

# Start running
library(erbot)
library(GCMER)
library(cora)



# res <- er_main(
#   data   = "cora",
#   truth  = cora_gold,
#   fields = c("title","authors","address"),
#   k_grid = seq(10, 300, by = 10),
#   write_csv = "cora_clean_pred.csv"
# )
#
#



# Requires the 'cora' package providing `cora` and `cora_gold`.

out_dir <- "D:/erbot"
save_dir <- file.path(out_dir, "results", paste0("cora_perf_", as.integer(Sys.time()), ".csv"))
fields_cora <- c("authors", "title", "editor", "pages", "year", "date")

res_cora <- run_cora(fields_cora = fields_cora,
                     out_dir = out_dir,
                     save_perf_file = save_dir)




