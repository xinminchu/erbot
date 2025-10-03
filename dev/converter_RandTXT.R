# Combine multiple R files into one text file
combine_r_files <- function(input_files, output_file = "combined_scripts.txt") {
  # input_files: character vector of file paths
  # output_file: name of the combined file

  # Open a connection to write output
  con <- file(output_file, open = "w")

  for (f in input_files) {
    cat("########################################\n", file = con, append = TRUE)
    cat("# File:", f, "\n", file = con, append = TRUE)
    cat("########################################\n\n", file = con, append = TRUE)

    # Read lines from the file
    lines <- readLines(f, warn = FALSE)

    # Write lines to the output
    writeLines(lines, con = con, sep = "\n")
    cat("\n\n", file = con, append = TRUE)
  }

  close(con)
  message("Combined file written to: ", output_file)
}

# Example usage:
r_files <- list.files("D:/erbot/R", pattern = "\\.R$", full.names = TRUE) # all .R files in folder "erbot/R"
combine_r_files(r_files, "D:/erbot/docs/erbot_scripts.txt")


# Split a unified text file into separate R files, using headers like:
# ########################################
# # File: 10_io.R
# ########################################
# (Paths like "R/10_io.R" are okay too.)

split_r_files <- function(input_file,
                          output_dir = "R",
                          header_regex = "^\\s*#\\s*File\\s*:\\s*(.+)\\s*$",
                          preserve_header_paths = FALSE,  # FALSE = use basename only
                          overwrite = TRUE,
                          encoding = "UTF-8") {
  lines <- readLines(input_file, warn = FALSE, encoding = encoding)

  # Ensure output directory exists
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  cur_relpath <- NULL
  buffer <- character()

  write_buffer <- function() {
    if (is.null(cur_relpath)) return()
    # Decide final relative path
    rel <- if (preserve_header_paths) cur_relpath else basename(cur_relpath)

    # Build full path
    out_path <- file.path(output_dir, rel)

    # Create subdirs if needed
    out_dir <- dirname(out_path)
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    # Handle duplicates if overwrite = FALSE
    if (!overwrite && file.exists(out_path)) {
      stem <- sub("(.*)\\.[^.]*$", "\\1", out_path)
      ext  <- sub(".*\\.([^.]+)$", "\\1", out_path)
      i <- 2L
      alt <- paste0(stem, "_", i, ".", ext)
      while (file.exists(alt)) {
        i <- i + 1L
        alt <- paste0(stem, "_", i, ".", ext)
      }
      out_path <- alt
    }

    writeLines(buffer, out_path, useBytes = TRUE)
  }

  for (ln in lines) {
    m <- regexec(header_regex, ln)
    hit <- regmatches(ln, m)[[1]]
    if (length(hit) > 1) {
      # Write the previous file (if any)
      write_buffer()
      # Start a new file
      header_path <- trimws(hit[2])
      # Normalize slashes for Windows/Unix
      header_path <- gsub("\\\\", "/", header_path)
      cur_relpath <- header_path
      buffer <- character()
    } else {
      # Collect content only after we've seen the first header
      if (!is.null(cur_relpath)) buffer <- c(buffer, ln)
    }
  }

  # Write the last file
  write_buffer()

  message("Split complete. Files written under: ", normalizePath(output_dir))
}

# ---- Examples ----

# 1) Your case: headers say "# File: R/00_utils.R" etc, and you want them in folder "R/"
#    To AVOID "R/R/...", strip header paths (default preserve_header_paths = FALSE):

split_r_files("Docs/erbot_scripts.txt", output_dir = "R")
split_r_files("Docs/erbot_scripts_consistent.txt", output_dir = "R")

# 2) If your headers include nested paths you want to KEEP (e.g., "inst/templates/x.R"):
#    This will create subfolders under output_dir as needed:
# split_r_files("Docs/erbot_scripts_consistent.txt", output_dir = "R", preserve_header_paths = TRUE)

# 3) If your header line has a different format, adjust header_regex accordingly, e.g.:
# split_r_files("Docs/erbot_scripts_consistent.txt",
#               header_regex = "^\\s*#\\s*BEGIN FILE\\s*:\\s*(.+)\\s*$")


