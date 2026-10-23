# Scale Track Design — E1 / E2 at North Carolina Scale (5M–8M+ records)

**Date:** October 20, 2026 · **Status:** design proposal, for advisor discussion
**Companions:** `blocking-innovations-2026-10-16.md` (three directions, pre-registered prototypes), `nc-dataset-card-2026-10-20.md` (benchmark facts), `advisor-brief-e1e2-cora-2026-10-20.md` (Cora results).

## 0. Why a separate scale track

The Cora prototypes established correctness of the two ideas under exact evaluation: E2 (marginal allocation) dPC = +0.3227 at aligned budget; E1 (collective blocking) dPC = +0.0248 with caveats. But blocking is a *scaling* technology: its claims (reduction ratio, candidate quality per unit budget, wall-clock) only become meaningful when the no-blocking alternative is infeasible. At NCVR scale (~8.3M records) all-pairs is ~3.4×10¹³ pairs — the regime where the advisor's scaling direction (record-level vs pair-level blocking, method innovation rather than comparison) actually lives. The v2 full-protocol scripts remain the correctness track (D10K, MusicBrainz 20K); this document designs the scale track on NCVR.

## 1. What carries over, what changes

**Unchanged (the science):**
- The framing: blocking = budget allocation. E1 is a *record-level* method (which records generate candidates together); E2 is a *pair-level* method (which candidate pairs get the budget). At scale the two compose: E1 generates, E2 schedules.
- Exact blocking evaluation: PC and RR are computed against gold clusters (NCID), whose pair count is known without all-pairs matching. Blocking claims stay exact at 8M.
- Pre-registration discipline: splits, budgets and kill lines declared before runs, at every ladder step.

**Changed (the engineering and the estimands):**
- No global top-k anywhere: E2's candidate pool can no longer come from all-pairs cosine. Pools are *produced by the blocking pass itself* and allocation happens within and across blocks.
- No end-to-end no-blocking baseline: replaced by (i) exact small-scale anchors (Cora/D10K), (ii) progressive recall-vs-budget curves at scale (the evaluation style of the progressive-scheduling literature), (iii) the gold-free audit (Direction 3) as a cross-check on NCID gold noise.
- Runtime and memory become first-class outcomes, reported alongside PC/RR at every scale step.

## 2. Engineering rules (non-negotiable for all scale code)

1. **Sparse-only.** No dense similarity matrices at any n. (The prototypes' chunked dense cosine is the first thing that dies: a single 500×8.3M dense block is ~33 GB.)
2. **data.table for all record/pair wrangling** (grouping, joins, ranking); Matrix (sparse) only where linear algebra is unavoidable, bucket-locally.
3. **Bucket-local computation.** Similarities are computed inside blocks/buckets (small), never globally. Pair lists are the universal currency: (idx1, idx2, score) triplets in data.table.
4. **Deterministic + checkpointed.** Every stage writes its pair list / statistics to disk with the seed and git hash; a killed run resumes from the last stage.
5. **Memory ceiling declared per run** (target: full NCVR on a single workstation, ≤ 32 GB; the exact workstation RAM is to be confirmed — design assumes 32 GB, degrades gracefully to snapshot subsets at 16 GB).

## 3. E1 at scale — collective blocking without the prototype's bottlenecks

Prototype bottlenecks: per-record R loops for MinHash signatures, `combn()` inside communities, dense top-200 insurance pool.

Scale design:
- **Signatures:** shingle hashing vectorised with integer arithmetic in data.table (hash each unique shingle once, join back — the prototype already hashes uniques; the loop moves from records to a keyed join). MinHash via grouped minima per (record, hash-function) using data.table aggregation, not matrix scans.
- **LSH banding by key grouping:** band key = concatenated signature slice; candidate pairs arise only inside band buckets (data.table self-join on band key). 
- **Giant-bucket policy (pre-registered):** buckets above a size cap are *not* enumerated pairwise. They are handled by (i) sorted-neighborhood within the bucket on a secondary key, and (ii) the exclusion direction's conflict predicates to drop provably-dead pairs, recycling the saved budget — this is where Direction 2 plugs into the scale track. Cap and policy fixed before runs; the number of capped buckets is reported (on NCVR, common-name buckets are the known stress case).
- **Communities:** the candidate graph's connected components are computed first (union-find over the pair list). Leiden runs *inside* components only, and components above a size threshold fall back to label propagation. The prototype's finding — partitions insensitive to low-weight edge differences — is rechecked at the 100k ladder step with the v2 diagnostics.

## 4. E2 at scale — two-level marginal allocation

- **Pools:** per-record candidate pools = the union of (a) E1 community pairs, (b) sorted-neighborhood pairs (the package's existing SN blocker, window 40, is the NCVR default), (c) LSH band pairs. All as pair lists.
- **Scoring:** pair similarity computed bucket-locally; calibration (Platt / mixture) fit on a *sample* of pairs (≤ 200k) drawn from training entities — at NC scale, "training entities" = a cluster-preserving subsample (see §5), never the full file.
- **Allocation:** Level 1 allocates the global budget B across components/blocks in proportion to their estimated marginal recall mass (block size × mean calibrated match probability); Level 2 runs the prototype's water-filling inside each block with its sub-budget. The global-top-B equivalence proof of the prototype becomes a two-level approximation; the approximation gap is itself measured at ladder steps where exact global allocation is still computable (≤ 1M records) — a designed, reportable quantity, not a hidden shortcut.
- **Progressive output:** the allocator emits pairs in marginal-value order, so a single run yields the full progressive recall-vs-budget curve instead of one (B, PC) point.

## 5. Evaluation harness

- **Ladder:** NCVR subsamples of 100k → 500k → 1M → full (~8.3M), drawn by **sampling gold clusters (NCIDs), never individual records** — record-level sampling would destroy the duplicate structure being measured. The same ladder, same seeds, for every method.
- **Metrics per step:** PC, RR, candidate count, wall time, peak memory, PC-vs-budget curve (progressive), and the capped-bucket count from §3.
- **Gold noise handling:** NCID gold is near-perfect, not perfect (dataset card §3). At the 500k step, run the Direction-3 audit (near-miss stratified sample, human-checked) to estimate the gold error rate; report PC both raw and audit-adjusted if the rate is material.
- **Anchors:** D10K full protocol (v2 scripts) must pass first — the scale track does not start reporting NC numbers until the same code paths reproduce the Cora/D10K results at the ladder's bottom.

## 6. Milestones (each ends with a written result note; kill/continue decided per milestone)

- **S0 — Data (this week).** NCSBE snapshots downloaded on the workstation; snapshot-stacking constructor (Christen recipe) + NCID gold builder (`er_load_gold("ncvr")`); data card verified against the actual files (record counts, NCID cluster distribution). *Kill criterion:* if stacked-snapshot duplicates < 50k pairs, fall back to requesting Christen's NCVR before proceeding.
- **S1 — Scale core.** Pair-list infrastructure: SN + LSH pair generation and the exact PC/RR evaluator running at 1M within the memory ceiling. *Continue criterion:* 1M end-to-end (pairs + evaluation) < 1 hour on the workstation.
- **S2 — E1-scale at 100k/500k.** Community generation per §3; compare against SN baseline at equal budgets; recheck the partition-identity diagnostic. *Kill line (pre-registered):* E1-scale PC ≤ SN PC at equal budget on both ladder steps ⇒ collective blocking stays a small-data result; we say so.
- **S3 — E2-scale at 100k/500k/1M.** Two-level allocation vs fixed-k within identical pools; permutation test at 100k (feasible), progressive curves at all steps. *Kill line:* dPC ≤ 0 at 500k ⇒ the Cora effect does not survive scale; report honestly.
- **S4 — Full NCVR run.** Best configuration from S2/S3 at full scale; scaling curves across the ladder; audit cross-check. Output: results note + figures for the paper draft.

## 7. Risks and open questions

- **Workstation memory/CPU unknown** — the design's 32 GB assumption must be confirmed; if 16 GB, S4 runs on a snapshot subset (still >5M records) and says so.
- **R's ceiling:** data.table handles 8M rows comfortably; the risk is pair explosion in giant buckets, mitigated by the §3 cap policy — but if capped buckets hold a large share of true pairs, PC will show it and the design must be revisited, not tuned silently.
- **Christen NCVR availability** (if the S0 fallback triggers): author-request timeline is outside our control; the stacking recipe makes us independent of it.
- **Privacy hygiene:** raw NC files stay on the workstation, never in git or cloud drives; only code, aggregate statistics and pair-level results (ids hashed) leave the machine.

## 8. What we ask of the advisor

Feedback on two design choices: (1) the two-level allocation approximation for E2 (exactness traded for scale, gap measured at ≤1M); (2) the giant-bucket policy (cap + sorted-neighborhood + exclusion recycling) as the point where the exclusion direction enters the scale track.
