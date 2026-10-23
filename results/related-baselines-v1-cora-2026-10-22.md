# Related-methods comparison v1 — Cora (2026-10-22, Mas VM run)

Script: experiments/exp-related-baselines-v1.R (standalone mode; harness
sections copied from exp-e2-waterfill-v2.R v2.2; loaders/metric replicated
from package sources R/01-load.R, R/09-evaluate.R, R/03-block.R).
Self-check vs archived v2: baseline 20,898 pairs / PC 0.5865, marg-alloc
20,575 pairs / PC 0.9092 (A and B) — EXACT match. Same split (seed
20261016, entity-disjoint 70/30; test rows 604), same pool (top-200),
B_test = 20,898.

| Method | Pairs | PC@B (aligned) | Native PC |
|---|---|---|---|
| M0 fixed-k baseline | 20,898 | 0.5865 | — |
| M1 global top-B (pool ranked by cosine) | 20,898 | **0.9227** | = |
| M6 marg-alloc A (Platt) | 20,575 | 0.9092 | = |
| M6 marg-alloc B (mixture) | 20,575 | 0.9092 | = |
| M3 adaptive-k (alpha*=0.70) | 20,445 native | 0.9173 | 0.8990 |
| M4 sorted neighborhood (win*=40) | 22,776 native | 0.5279 | 0.5279 |
| M2 meta-blocking CBS aligned@B | 20,898 | 0.5135 | graph only 15,009 edges |
| M2 meta-blocking JS aligned@B | 20,898 | 0.5135 | (identical set: graph < B) |
| M2 meta-blocking WNP native | 4,373 | — | 0.1790 |
| M5 canopy (T2*=0.9373) | 4,517 native | 0.9227* | 0.2007 |

*canopy aligned = its high-cosine native pairs are a subset of global
top-B; the aligned number is the top-up, not canopy itself.

Key readings (Cora only, descriptive, no permutation run for baselines):
1. Global top-B BEATS waterfill by +0.0135. Calibration (Platt/mixture)
   is a monotone re-ranking of cosine within record; the waterfill floor
   (1 pair per record) and profile structure cost a little vs pure global
   ranking. The v2 permutation result (profile shape matters, +0.15 over
   shuffled profiles) is untouched — global top-B is a different,
   profile-free allocation.
2. Adaptive-k (published family) also edges out waterfill when aligned.
3. Meta-blocking's token graph on Cora holds only 15,009 edges (< B) and
   they are low-yield: even budget-aligned it reaches only 0.5135, below
   the fixed-k baseline. Cora's sparse bibliographic tokens are a poor
   fit for token blocking; D10K/Music graphs will be far denser.
4. Sorted neighborhood (0.5279) also loses to fixed-k on Cora.

Status: D10K + MusicBrainz-20K runs pending dataset sourcing (public
copies must verify against project fingerprints: D10K 8,705 pairs /
6,000 entities; Music sample 20,000 / 10,294 / 16,934). Music raw file
is ~2.3 GB; may run on Sam's Windows where the built sample already
exists.
