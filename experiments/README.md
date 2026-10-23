# Blocking experiments (E1 / E2 / related methods)

The experiment scripts behind the ERBOT blocking results reported at
<https://www.sandx.io/experiments> ("Reproduce these runs in R" section
there has the full step-by-step: environment, data placement, commands,
and the archived check numbers).

| Script | What it runs |
|---|---|
| `exp-e1-collective-blocking-v2.R` | E1 collective (community) blocking + community-label permutation test |
| `exp-e2-waterfill-v2.R` | E2 marginal allocation ("water-filling", variants A/B) + allocation-profile permutation test |
| `exp-related-baselines-v1.R` | Related-methods comparison on the same protocol: global top-B, adaptive-k, meta-blocking (CBS/JS/WNP), sorted neighborhood, canopy. Also runs without the package installed (standalone mode). |
| `build-musicbrainz20k.R` | One-time builder for the MusicBrainz-20K whole-cluster sample |

Usage: `Rscript experiments/<script>.R <dataset>` with dataset one of
`cora` (default), `d10k`, `musicbrainz20k`, run from the package root.

Data: Cora ships in `inst/extdata/`. The D10K and MusicBrainz-20K data
files are not redistributed in this repository; their sources and
construction are documented on the Datasets page at
<https://www.sandx.io/datasets>.

State note (2026-10-22): these scripts were run on 2026-10-20–22 against
the ERBOT working tree; this repository's package code may lag that
state.
