remove.packages("erbot")
### This is normal installation from local ###
setwd("D:/erbot")
devtools::document()
devtools::install()




remove.packages("erbot")
### This is normal installation from GitHub ###
remotes::install_git("https://github.com/xinminchu/erbot.git", upgrade = "never")



library(erbot)
packageVersion("erbot")

exists("er_main")
exists("run_cora")
exists("run_affiliation")



library(GCMER)
library(cora)



#################################
# Run Cora
#################################

# Requires the 'cora' package providing `cora` and `cora_gold`.

out_dir <- "D:/erbot/outputs"

save_perf_file <- file.path(
  out_dir, "results",
  sprintf("cora_perf_%s.csv", format(Sys.time(), "%Y%m%d%H%M%S"))
)


fields_cora <- c("title", "authors", "address", "date", "year")

res_cora <- run_cora(fields_cora = fields_cora,
                     out_dir = out_dir,
                     save_perf_file = save_perf_file)

#################################
# Run Affiliation
#################################

# where to put outputs
out_dir <- "D:/erbot/outputs"

# input files
#data_path  <- "D:/erbot/data/affiliationstrings_ids.csv"
data_path  <- "D:/erbot/data/clean_affiliations_2024_05_15.csv"

truth_path <- "D:/erbot/data/affiliationstrings_mapping.csv"   # or NULL

# which fields to use (adjust to your columns)
#fields_affil <- c("affil1")   # or c("affil1","affil2") if you parsed more

fields_affil <- c("Name1", "Name2",	"Name3",	"Street1",	"Street2",	"City",	"State",	"Zipcode",	"Country")

# dynamic filename for the rounded performance table
save_perf_file <- file.path(
  out_dir, "results",
  sprintf("affil_perf_%s.csv", format(Sys.time(), "%Y%m%d%H%M%S"))
)

exists("run_affiliation")

res_affil <- run_affiliation(
  data_path     = data_path,
  truth_path    = truth_path,        # set to NULL if you don't have truth
  fields_affil  = fields_affil,
  out_dir       = out_dir,
  save_perf_file = save_perf_file,   # writes rounded perf/agreement table
  perf_source    = "auto",           # "performance" | "agreement" | "auto"
  digits         = 5,                # round to 5 d.p. (CSV/TXT + PDF)
  top_n          = 5                 # #items in the PDF "Top items" section
)



####################################
# Unified running
####################################

# 1) CORA (uses cora package, internal loader, and cora_gold truth)
res_cora <- run_erbot_dataset(
  dataset   = "cora",
  out_dir   = "D:/erbot/outputs_cora",
  fields_cora = c("title", "authors", "address")
)

# 2) Affiliation (CSV input + optional mapping file as truth)
res_affil <- run_erbot_dataset(
  dataset      = "affiliation",
  out_dir      = "D:/erbot/outputs_affil",
  data_affil   = "D:/erbot/data/affiliationstrings_ids.csv",
  truth_affil  = "D:/erbot/data/affiliationstrings_mapping.csv",
  fields_affil = "affil1"
)

# 3) D10K synthetic (large CSV, fields tuned for your schema)
res_d10k <- run_erbot_dataset(
  dataset     = "d10k",
  out_dir     = "D:/erbot/outputs_d10k",
  data_d10k   = "D:/erbot/data/D10K.csv",
  truth_d10k  = "D:/erbot/data/D10K_truth.csv"   # or NULL if no gold
)

###################################
# Unified running with tuning
###################################

## 1) CORA: internal data + cora_gold
tuned_cora <- run_erbot_tuned_dataset(
  dataset = "cora",
  out_dir = "D:/erbot/outputs_cora"
)

## 2) Affiliation: CSV + mapping truth
tuned_affil <- run_erbot_tuned_dataset(
  dataset     = "affiliation",
  out_dir     = "D:/erbot/outputs_affil",
  data_affil  = "D:/erbot/data/affiliationstrings_ids.csv",
  truth_affil = "D:/erbot/data/affiliationstrings_mapping.csv"
)

## 3) D10K: large synthetic dataset, coarser grids
tuned_d10k <- run_erbot_tuned_dataset(
  dataset    = "d10k",
  out_dir    = "D:/erbot/outputs_d10k",
  data_d10k  = "D:/erbot/data/D10K.csv",
  truth_d10k = "D:/erbot/data/D10K_truth.csv"  # or NULL if no gold
)
