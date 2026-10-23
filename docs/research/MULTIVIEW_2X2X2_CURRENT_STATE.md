# Multiview 2x2x2 Current State

## 1. Confirmed implemented functionality
- The repository contains package functions for loading, diagnosing, blocking, similarity, clustering, evaluation, and some ablation/reporting flows.
- The package includes functions and documentation for entity-resolution workflows such as clustering, blocking, and evaluation.

## 2. Partially implemented functionality
- There is evidence of experiment-style scripts and test scaffolding around NCVR linkage and redesign experiments, but the repository does not yet present a canonical multiview 2x2x2 framework.

## 3. Historical claims not supported by current code
- The repository still includes paper materials and historical result files, but they are not trusted as authoritative without fresh validation.

## 4. Existing datasets
- CORA, affiliation strings, D10K, and NCVR-based data assets are present in the repository data tree or bundled inst/extdata.

## 5. Existing tests
- tests/testthat/ contains several ER-specific tests and repository-validation tests.

## 6. Existing experiment scripts
- The repository contains legacy experiment runners and test entry points under tests/ and R/.

## 7. Existing paper material
- The paper/ directory contains multiple previous paper versions and generated outputs, which should be treated as historical material until revalidated.

## 8. Missing components
- At the start of Phase 0, no canonical multiview 2x2x2 experiment registry, canonical experiment configuration schema, or validated restart plan was present in the current canonical snapshot. "Not present in the current canonical snapshot" means the asset is not part of the tracked, trusted repository state; it does not assert that such an asset never existed elsewhere (for example in historical or personal reference material, which is excluded from research evidence).
- Phase 0 adds scaffolding for the registry (experiments/registry/experiment_registry.csv) and a restart plan (docs/research/MULTIVIEW_2X2X2_RESTART_PLAN.md); both remain unvalidated scientifically.

## 9. Contradictions
- Historical paper results and current code are not yet aligned under a single governance model.

## 10. Fixed framework decisions
- The three binary dimensions of the 2x2x2 program are fixed by user decision:
  1. Clustering approach: pairwise-threshold clustering vs graph/community-detection clustering.
  2. Fusion order: early fusion (fusion-first) vs late fusion (clustering-first).
  3. ER regime: deduplication vs record linkage.
- The four method cells over clustering approach x fusion order are PE, PL, GE, and GL; each is evaluated under both ER regimes. See docs/research/MULTIVIEW_2X2X2_RESTART_PLAN.md.
