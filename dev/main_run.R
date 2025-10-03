### This is normal installation ###
remotes::install_git("https://github.com/xinminchu/erbot.git", upgrade = "never")
library(erbot)
packageVersion("erbot")

library(erbot)
exists("er_main")




library(GCMER)
library(cora)



#################################
# Run Cora
#################################

# Requires the 'cora' package providing `cora` and `cora_gold`.

out_dir <- "D:/erbot"
save_dir <- file.path(out_dir, "results", paste0("cora_perf_", as.integer(Sys.time()), ".csv"))
fields_cora <- c("authors", "title", "address", "year", "date")

res_cora <- run_cora(fields_cora = fields_cora,
                     out_dir = out_dir,
                     save_perf_file = save_dir)

#################################
# Run Affiliation
#################################

# where to put outputs
out_dir <- "D:/erbot/outputs"

# input files
data_path  <- "D:/erbot/data/affiliationstrings_ids.csv"
truth_path <- "D:/erbot/data/affiliationstrings_mapping.csv"   # or NULL

# which fields to use (adjust to your columns)
fields_affil <- c("affil1")   # or c("affil1","affil2") if you parsed more

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

