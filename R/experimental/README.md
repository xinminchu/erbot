# Experimental: neural ER modules (NOT part of the built package)

R only collates `.R` files directly under `R/`; files in this subdirectory
are **ignored by `R CMD build` / `R CMD INSTALL`**. That is deliberate.

These four files are torch-based entity-resolution experiments
(`RecordEncoder`, `contrastive_loss`, `stability_penalty`,
`train_one_epoch`, `run_training`, plus graph helpers in
`43-graph_utils.R`). They are quarantined here because:

1. **Not end-to-end differentiable.** `train_one_epoch()` backpropagates
   only through the contrastive loss; the graph-clustering step breaks the
   gradient (see its own docs: graph and stability losses are "treated as
   external regularizers"). `stability_penalty()` is pure R, not
   torch-differentiable at all.
2. **Disconnected from the pipeline.** Nothing in the core nine-stage
   pipeline (`er_run()` and friends) calls any of these functions.
3. **Heavy optional dependencies** (`torch`, `RSpectra`) that the core
   package no longer suggests.

Kept in-repo (not deleted) so the experiments and their git history stay
available. To revive one: move it back to `R/`, restore its `export()` lines
in `NAMESPACE`, and re-add the dependency to `DESCRIPTION`.

Quarantined 2026-10-06 (Phase 3 forging).
