# E1/E2 full-protocol v2 — musicbrainz20k (20k whole-cluster sample of the
# 19.375M-record MusicBrainz A01 file; gold = CID; sample seed 20261020:
# 20,000 records / 10,294 entities / 16,934 matched pairs / 5,168 singletons)

## E1 PREVIEW run (2026-10-20 22:36) — CONTAMINATED KEY, NOT report data

Ran on the first build (record key = raw string `id`, which has 180 duplicate
values across sources). Symptoms visible in the output: "dropping 88
unlabeled rows" (n = 19,912, entities 10,241). Label noise ~1%; all numbers
below are indicative only and will be superseded by the clean rerun keyed
by TID.

- Tuning (train): k* = 50 at PC 0.9885–0.9922; treatment (res* = 1) 0.9925.
- Test (budget aligned exactly, 221,604 pairs): BASELINE PC = 0.9958,
  TREATMENT PC = 0.9942 → **dPC = −0.0016, kill line fired** ("idea dead on
  this dataset"); permutation p = 1.0000 (sd 0.0000), same degenerate
  pool-determined pattern as elsewhere.
- End-to-end: singleton collapse in er_cluster again (clusters = 5933 =
  test rows, ARI = 0, both arms identical; pair-set Jaccard 0.377).

### Preview reading

Music behaves like d10k, not like cora: rich structured fields
(title/artist/album/year) + near-copy duplicates let per-record kNN nearly
saturate on the test split (0.9958), leaving ~0.4% headroom — nothing for
collective blocking to win, and the −0.0016 gap is inside this build's
label-noise band. Cross-dataset pattern for the Friday report: the
budget-allocation effect is large exactly where the baseline cannot
saturate (cora) and vanishes at the ceiling (d10k, music) — the mechanism
matters in the hard, low-signal regime.

Clean reruns (TID-keyed build) to follow for both E1 and E2; expect
dropping = 0.

## E1 CLEAN run (2026-10-20 22:58) — report data

TID-keyed build confirmed: no dropping line, n = 20,000, entities = 10,294.

- Tuning (train): k* = 50 (PC 0.9899 / 0.9913 / 0.9935); treatment
  (res* = 1) train PC = 0.9935.
- Test (budget aligned exactly, 226,013 pairs): BASELINE PC = **0.9948**,
  TREATMENT PC = **0.9923** → **dPC = −0.0025, kill line fired**.
  Permutation p = 1.0000 (null = observed, sd 0.0000).
- The clean gap is slightly larger than the preview suggested (−0.0016)
  and is NOT label noise this time: at identical budget the treatment
  misses ≈ 13 more true pairs than plain kNN (test true pairs ≈ 5.1k).
  At near-saturation any budget reallocation can only lose pairs the
  per-record top-k would have kept; the community machinery's alignment
  trim does exactly that here.
- End-to-end: singleton collapse in both arms again (clusters = 6046 =
  test rows, ARI = 0; Jaccard 0.376). Together with d10k this is now a
  robust limitation note: er_cluster returns singletons on the d10k and
  music candidate graphs (it did not on cora: 571 clusters).

## E2 CLEAN run (2026-10-21 08:39, Sam's Windows) — report data

n = 20,000, entities = 10,294; train 14,057 / test 5,943 (no dropping,
TID-keyed build as for E1 clean).

- Tuning (train): k* = 50 (PC 0.9893 / 0.9914 / 0.9932). Train pool
  2,811,400 pairs, positives 23,692 (0.84%). Platt coef: intercept
  −16.489, slope 29.197. Mixture: w = 0.985/0.015, mu = 0.215/0.599,
  separation 0.3845.
- Test: BASELINE (fixed k) pairs = 223,406, PC = **0.9954**, RR = 0.9873.
  - Variant A (labeled Platt): pairs = 222,814 (−0.27%), PC = 0.9944 →
    **dPC = −0.0010, kill line fired** ("dead on this dataset").
  - Variant B (unlabeled mixture): pairs = 223,341 (−0.03%), PC = 0.9928 →
    **dPC = −0.0026, kill line fired**.
- Permutation tests (R = 199):
  - A: null dPC mean = −0.0060 (sd 0.0012), observed −0.0010,
    **empirical p = 0.0050** (floor at R = 199).
  - B: null dPC mean = −0.0022 (sd 0.0004), observed −0.0026, p = 0.9300.

### E2 reading

Both variants are dead by the pre-registered kill lines — with the
baseline at PC 0.9954 there is ~0.5% headroom and, as in E1 here and
everything on d10k, reallocation can only swap away pairs fixed-k
already holds. One nuance worth one sentence in the report: variant A's
observed dPC (−0.0010) is significantly *less bad* than its random-profile
null (−0.0060, p = 0.005) — the learned allocation profile does carry
signal versus random reallocation; it simply cannot beat fixed-k at the
ceiling. The ordering learned > random holds on both cora (null +0.170
vs observed +0.3227) and music (null −0.0060 vs observed −0.0010); what
flips sign across datasets is the headroom, not the allocation quality.
Variant B shows no such signal here (observed inside its null, p = 0.93):
the unlabeled mixture profile is not a usable allocator on this dataset.

## v2 3×2 matrix — CLOSED (2026-10-21)

| dataset | baseline PC | E1 dPC (perm p) | E2 dPC A / B (perm p) | reading |
|---|---|---|---|---|
| cora | 0.7050 (E1) / 0.5865 (E2) | +0.2835 (p = 1.0 → community attribution rejected) | +0.3227 / +0.3227 (p = 0.005 → allocation established) | effect lives here: baseline cannot saturate |
| d10k | 1.0000 | −0.0008 (dead) | +0.0000 / −0.0004 (dead; A = equal PC at −1.3% pairs) | ceiling: scope boundary |
| musicbrainz20k | 0.9948 (E1) / 0.9954 (E2) | −0.0025 (dead) | −0.0010 (dead; beats random null, p = 0.005) / −0.0026 (dead, p = 0.93) | near-ceiling: same boundary |

Unified conclusion (pre-registered kill lines throughout): the
marginal-allocation effect is real but scope-bound — it appears exactly
where the fixed-k baseline cannot saturate and vanishes at the ceiling.
The deliverable for the advisor is this scope statement plus the cora
effect size, not a universal improvement claim.
