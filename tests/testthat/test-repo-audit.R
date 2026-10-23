repo_root <- normalizePath(testthat::test_path("../.."), winslash = "/", mustWork = TRUE)
scan_dirs <- c("R", "scripts", "tests")
scan_files <- unlist(lapply(scan_dirs, function(dir) {
  dir_path <- file.path(repo_root, dir)
  if (dir.exists(dir_path)) {
    list.files(dir_path, pattern = "\\.(R|r|Rmd|qmd|Rnw|Rprofile|sh|ps1|bat|cmd)$", recursive = TRUE, full.names = TRUE)
  } else {
    character()
  }
}))
scan_files <- scan_files[!grepl("test-repo-audit\\.R$", scan_files)]

for (f in scan_files) {
  content <- paste(readLines(f, warn = FALSE), collapse = "\n")
  expect_false(grepl("setwd\\(", content, perl = TRUE), info = paste("setwd found in", f))
  expect_false(grepl("D:\\\\|D:/|R:\\\\|R:/|C:\\\\Users\\\\|OneDrive|ResearchHub", content, perl = TRUE), info = paste("absolute path found in", f))
}
