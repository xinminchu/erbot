# Reproducibility Standard

- Every experiment must declare a random seed.
- Raw data must remain immutable and be stored under data/ or data-raw/.
- Dataset checksums should be recorded when available.
- Dependencies must be declared in DESCRIPTION or an explicit environment manifest.
- Experiments must be driven by stable configuration files rather than manual edits.
- Metrics must be written in machine-readable formats (for example CSV or JSON).
- Provenance metadata must be captured for each output artifact.
- Results in results/ must be regenerated rather than hand-edited.
- Session information and Git commit identification should be recorded with every validated run.
- Deterministic behavior should be preferred where technically feasible.
