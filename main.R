

devtools::document()


##############################
# Installation Issues Solver
##############################
# Start running
library(devtools)
install.packages(c("magrittr","purrr"))
library(magrittr)
library(purrr)
install_github("xinminchu/erbot")


# Fresh session, nothing loaded
options(timeout = 600)

# Use libcurl first (best on R 4.2+)
options(download.file.method = "libcurl")

# Make sure a PAT is set (replace with yours if needed)
Sys.setenv(GITHUB_PAT = Sys.getenv("GITHUB_PAT"))  # no-op if already set

remotes::install_github("xinminchu/erbot", upgrade = "never")

remotes::install_git("https://github.com/xinminchu/erbot.git", upgrade = "never")
# or pick a branch explicitly:
# remotes::install_git("https://github.com/xinminchu/erbot.git", ref = "main", upgrade = "never")

##################################################
# Cleanly unload and reinstall (fixes “in use”)
###################################################

# 0) Make sure nothing holds the DLLs/namespaces
for (p in c("erbot","magrittr","purrr","tidyverse","dplyr","readr","stringr")) {
  if (p %in% loadedNamespaces()) try(unloadNamespace(p), silent = TRUE)
}

# 1) Remove any 00LOCK* folders that block installs
locks <- list.dirs(.libPaths()[1], full.names = TRUE, recursive = FALSE)
unlink(locks[grepl("00LOCK", basename(locks), fixed = TRUE)], recursive = TRUE, force = TRUE)

# 2) Reinstall from git (or from your local source folder) with force
remotes::install_git("https://github.com/xinminchu/erbot.git",
                     upgrade = "never",
                     build_vignettes = FALSE,
                     force = TRUE)


# Alternative (if your local copy is at D:/erbot-src/erbot-main):
# remotes::install_local("D:/erbot-src/erbot-main", upgrade="never", build_vignettes=FALSE, force=TRUE)




library(erbot)
packageVersion("erbot")
"run_cora" %in% getNamespaceExports("erbot")




# 1) Use Windows' downloader for reliability
options(download.file.method = "wininet", timeout = 600)

# 2) Download the repo ZIP from codeload (not the API)
zip_url <- "https://codeload.github.com/xinminchu/erbot/zip/refs/heads/main"
dest    <- file.path(tempdir(), "erbot.zip")
utils::download.file(zip_url, dest, mode = "wb")

# 3) Install the ZIP
install.packages("remotes", repos = "https://packagemanager.posit.co/cran/latest")
remotes::install_local(dest, upgrade = "never", build_vignettes = FALSE)


##### If install_github failed ######
##### Trt following way  ############
# 0) Clean slate
gc(); unlink(c(file.path(tempdir(), "erbot.zip"),
               file.path(tempdir(), "erbot-extract")), recursive = TRUE, force = TRUE)

# 1) Use the Windows downloader and longer timeout
options(download.file.method = "wininet", timeout = 600)

# 2) Download the repo ZIP from codeload (NOT the API tarball)
zip_url <- "https://codeload.github.com/xinminchu/erbot/zip/refs/heads/main"
dest    <- "D:/erbot.zip"  # simple, non-temp path helps avoid AV locking
if (file.exists(dest)) file.remove(dest)
utils::download.file(zip_url, dest, mode = "wb")

# 3) Sanity check: file exists and is non-trivial size (>100 KB)
stopifnot(file.exists(dest), file.info(dest)$size > 1e5)

# 4) Verify we can read the ZIP (list contents). If this errors, ZIP is corrupt.
zip_list <- utils::unzip(dest, list = TRUE)
head(zip_list)

# 5) Extract to a clean folder
exdir <- "D:/erbot-src"
unlink(exdir, recursive = TRUE, force = TRUE)
utils::unzip(dest, exdir = exdir)

# 6) Find the package root inside the ZIP (usually erbot-main/)
pkg_root <- list.dirs(exdir, full.names = TRUE, recursive = FALSE)[1]
pkg_root
stopifnot(file.exists(file.path(pkg_root, "DESCRIPTION")))

# 7) Install from the extracted folder (bypasses reading-from-zip)
install.packages("remotes", repos = "https://packagemanager.posit.co/cran/latest")
remotes::install_local(pkg_root, upgrade = "never", build_vignettes = FALSE)

# 8) Quick check
library(erbot)
exists("er_main")




library(GCMER)
library(cora)





# Requires the 'cora' package providing `cora` and `cora_gold`.

out_dir <- "D:/erbot"
save_dir <- file.path(out_dir, "results", paste0("cora_perf_", as.integer(Sys.time()), ".csv"))
fields_cora <- c("authors", "title", "address", "year", "date")

res_cora <- run_cora(fields_cora = fields_cora,
                     out_dir = out_dir,
                     save_perf_file = save_dir)


