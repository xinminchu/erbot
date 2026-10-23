# Repository Architecture

## Directory responsibilities
- R/: canonical package functions and reusable logic only.
- tests/: unit and regression tests.
- data/: project data organized by provenance and immutability; raw data remain immutable.
- data-raw/: scripts used to acquire or construct data.
- experiments/: canonical experiment registry, configs, and entry-point scripts.
- scripts/: thin executable wrappers that call into R/.
- results/: generated and reproducible outputs that are not publication evidence until validated.
- paper/: paper sources and manuscript assets.
- docs/: audit, architecture, research, and dataset documentation.
- archive/: historical and untrusted material retained for traceability only.

## Allowed dependencies between directories
- scripts/ may call R/.
- experiments/ may call R/ and reference configs under experiments/.
- paper/ may consume validated results from results/.
- docs/ may reference all canonical directories but should not be treated as executable code.

## Generated versus source files
- Source files live in R/, tests/, scripts/, docs/, paper/, and package metadata at the repository root.
- Generated files live under results/ and should be reproducible from source data and configurations.

## Prohibited practices
- No hard-coded absolute paths to R:, D:, OneDrive, or user-profile paths.
- No setwd() in canonical source.
- No ad hoc experiments in R/.
- No manually edited results in results/.

## Naming rules
- Use stable IDs for experiments, such as MV2X2X2-EXP-0001.
- Prefer descriptive directory names and avoid ambiguous suffixes.

## Data flow
- Raw inputs are read from data/ or data-raw/.
- Canonical package functions in R/ transform data and generate outputs.
- Reproducible outputs are written to results/.
