# Experiments

Experiment IDs follow a stable convention:

- MV2X2X2-EXP-0001, MV2X2X2-EXP-0002, ...

The ID is a stable identifier for the experiment configuration and should not encode mutable conclusions.

## Blocking experiments (October 2026)

The scripts behind the ERBOT blocking results reported at
<https://www.sandx.io/experiments> (the "Reproduce these runs in R"
section there has the full step-by-step):

| Script | What it runs |
|---|---|
| `exp-e1-collective-blocking-v2.R` | E1 collective (community) blocking + community-label permutation test |
| `exp-e2-waterfill-v2.R` | E2 marginal allocation ("water-filling", variants A/B) + allocation-profile permutation test |
| `exp-related-baselines-v1.R` | Related-methods comparison on the same protocol: global top-B, adaptive-k, meta-blocking (CBS/JS/WNP), sorted neighborhood, canopy. Also runs without the package installed (standalone mode). |
| `build-musicbrainz20k.R` | One-time builder for the MusicBrainz-20K whole-cluster sample |
| `exp-e1-collective-blocking.R`, `exp-e2-waterfill.R` | v1 scripts, kept for the record |
| `audit-d10k-leakage-v1.R` | D10K provenance / leakage audit |
| `RUN-v2-2026-10-20.md` | Original v2 run instructions |

Usage: `Rscript experiments/<script>.R <dataset>` with dataset one of
`cora` (default), `d10k`, `musicbrainz20k`, run from the package root.
Run outputs are archived under `results/`; the advisor-facing reports
under `reports/`.

Repository note (2026-10-22): this repository was unified to the ERBOT
working-tree state — the code that produced the archived results.
`xinminchu/erbotv2` is retained as an archived (read-only) snapshot.
The D10K and MusicBrainz-20K data files are not tracked here; see
`data/README.md`. `docs/datasets/DATASET_CATALOG.md` is intentionally
kept out of this public repository (it names a private source
collection); the public dataset documentation is at
<https://www.sandx.io/datasets>.
