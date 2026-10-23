# Dependency Audit

## DESCRIPTION Imports
- data.table, dplyr, tibble, stringr, stringi, tidyr, purrr, readr, readxl, text2vec, Matrix, irlba, stringdist, igraph, FNN, cluster, stats, utils, grid, grDevices, gridExtra, scales, magrittr, jsonlite

## DESCRIPTION Suggests
- mclust, aricode, GCMER, torch, leidenbase, RANN, proxy, RSpectra, peakRAM, dbscan, textreuse, knitr, rmarkdown

## NAMESPACE consistency
- The package has a large manual namespace and should be validated with R CMD check once the path fixes are in place.

## renv.lock status
- Present but not currently validated against the installed environment; treat as a baseline dependency snapshot rather than a guaranteed lock.

## Undeclared R packages
- The repository references packages such as cora, here, testthat, and possibly reticulate-like dependencies indirectly; these should be verified against the package metadata.

## Missing packages
- The current environment should be checked before claiming package availability; no broad package install is performed in this audit phase.

## External software dependencies
- Python scripts exist in inst/python/ and may require external runtime support for deep-learning baselines.

## LaTeX dependencies
- Paper sources rely on standard LaTeX support and generated PDFs already exist, but full paper compilation is not part of this audit.

## System dependencies
- None identified from the repository text alone.

## Package-version risks
- The DESCRIPTION uses a broad dependency set and may need version bounds in a future cleanup; this is not changed in this phase.

## Machine-specific dependency assumptions
- The .Rprofile activates renv from a relative path and only when renv/activate.R exists, so a checkout without a restored renv/ directory still starts R normally.
