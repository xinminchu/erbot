# ERBOT Blocking Experiments — Progress Brief: E1 / E2 Cora Prototype Results

**Date:** October 20, 2026
**Status:** Both prototypes from the October 16 pre-registered design have now been run on Cora (entity-disjoint 70/30 split; hyperparameters tuned on training entities only; test set reported once; kill lines declared before the runs). Headline: **neither kill line was triggered — E2 is a large effect, E1 a weak positive with two stated caveats.**

## E2 · Marginal-allocation blocking ("water-filling") — main result

- Test split (604 records): fixed-k baseline PC = 0.5865 (20,898 pairs); marginal allocation PC = **0.9092** (20,575 pairs, i.e. 1.5% *under* budget — already inside the ±2% alignment tolerance set for the paper version).
- **dPC = +0.3227.** Both calibration variants agree exactly: (A) labeled Platt calibration; (B) unlabeled two-component mixture calibration (separation 0.4891) — no gold labels are needed to obtain the same allocation.
- Mechanism check: under marginal allocation, 56% of records receive *fewer* pairs than the fixed k (baseline: 0%) — the budget measurably flows from easy records to hard ones, as the design intends, rather than winning on total volume.

## E1 · Collective blocking — weak positive, two caveats

- Test split (729 records): baseline PC = 0.7050 (24,267 pairs); treatment PC = 0.7298 (21,164 pairs); **dPC = +0.0248**, above the kill line.
- Caveat 1 (budget not aligned): the treatment used 12.8% fewer pairs than the baseline — the prototype's alignment only trims overspend and never tops up. Winning on a smaller budget is directionally encouraging, but the paper version must be re-run at ±2% alignment; the corrected script (v2, two-sided alignment to exactly B pairs) is ready.
- Caveat 2 (identical end-to-end metrics): the two arms' end-to-end clustering metrics were identical on all 14 measures. We audited the evaluation code line by line (the clustering does use the edges and weights; evaluation is aligned by record id) and found no bug. The most plausible explanation is that both candidate graphs share the same strong-edge skeleton and Leiden at resolution 1 converges to the same partition. v2 prints explicit diagnostics (pair-set Jaccard, cluster counts, a partition-identity check) so the re-run settles this with data.

## Next steps: full protocol (scripts ready)

1. **Permutation tests** (new in v2): community-label permutation for E1 (community sizes preserved, identity destroyed) and allocation-profile permutation for E2 (budget distribution preserved, assignment destroyed), 199 repetitions each, reporting empirical p-values — testing whether community structure and allocation profile themselves carry the information.
2. **Aligned re-run**: E1 on Cora at exact budget alignment, removing Caveat 1.
3. **New datasets**: D10K and MusicBrainz 20K (loaders for both already exist in the package), to test whether the effects replicate across datasets.

Runs execute on the Windows workstation (R environment verified); Cora-scale prototypes take minutes, the permutation tests and the two larger datasets are on the order of hours at most. Full raw output will accompany the next report.

## Provenance

- Scripts: `experiments/exp-e1-collective-blocking.R` (seed 20261012), `experiments/exp-e2-waterfill.R` (seed 20261016); v2: `exp-e1-collective-blocking-v2.R`, `exp-e2-waterfill-v2.R`.
- Raw outputs and all figures: `results/e1-e2-cora-2026-10-20.md`.
- Pre-registered design and the three-direction framing: `reports/blocking-innovations-2026-10-16.md`.
