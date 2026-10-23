# Multiview 2x2x2 Restart Plan

## Formal research objective
- Re-establish a rigorous, reproducible, and auditable research program for multiview entity resolution that is grounded in the current repository code and data assets rather than legacy claims.

## Precise interpretation of the three binary dimensions
- The three binary dimensions of the multiview 2x2x2 framework are fixed by user decision:
  1. Clustering approach: pairwise-threshold clustering vs graph/community-detection clustering.
  2. Fusion order: early fusion (fusion-first) vs late fusion (clustering-first).
  3. ER regime: deduplication vs record linkage.
- The four method cells over clustering approach x fusion order are PE, PL, GE, and GL (P = pairwise-threshold, G = graph/community detection; E = early/fusion-first, L = late/clustering-first). Each method cell is evaluated under both ER regimes.

## Candidate datasets
- CORA, affiliation strings, D10K, and NCVR-based data assets.

## Experimental units
- Record-level datasets and their derived pair or cluster labels.

## Split strategies
- The repository currently has no canonical split strategy; this must be defined before implementation.

## Leakage controls
- Source identifiers and truth columns must be excluded from a feature pipeline when used for learning or evaluation.

## Evaluation metrics
- ARI, B-cubed F-score, pair completeness, and reduction ratio are present in the code and docs.

## Baselines
- Baselines should be limited to package-supported methods initially and should be recorded in the experiment registry.

## Ablations
- The initial restart plan should focus on minimal ablations that are easy to validate and reproduce.

## Reproducibility requirements
- Each experiment must record its seed, data checksum, configuration, code commit, environment, and outputs in a machine-readable registry.

## Minimum tests
- Path-integrity tests, parsing tests for canonical R files, and a small smoke-test for core package entry points.

## Staged implementation plan
1. Establish the canonical repository layout and audit documentation.
2. Define the experiment schema and registry.
3. Implement minimum reproducible scaffolding for one representative experiment.
4. Validate the workflow end to end on a small sample.
5. Only then expand to broader multiview benchmarks.
