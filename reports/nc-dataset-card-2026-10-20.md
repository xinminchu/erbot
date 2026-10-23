# Dataset Card — North Carolina Voter Registration (NCVR) for Large-Scale Blocking / ER

**Date:** October 20, 2026 · **Purpose:** candidate "ultra-large" benchmark for the ERBOT scaling track (blocking and scalable entity resolution). All facts below were verified against primary sources on 2026-10-20; items that could not be verified are marked as such.

## 1. What "the NC dataset" actually is — three distinct versions

Confusion between versions is the main trap. They are different objects:

**Version A — Raw NCSBE files (the source).** The North Carolina State Board of Elections publishes the full voter registration file, free, updated weekly (Sunday evenings), as tab-delimited text, statewide or per county (100 counties), plus election snapshots going back 15+ years (download host: dl.ncsbe.gov). A single snapshot is one row per registration — by itself it is **not** a duplicate-detection benchmark (see §4).

**Version B — Christen's combined temporal NCVR (the research standard).** Peter Christen (ANU) downloaded the NCSBE file every two months from October 2011 and merged 17 snapshots (report dated 29 June 2014), keeping, per voter, every record that is not an exact duplicate of an earlier one. Result: **nearly 8.3 million records of over 8 million individual voters; ~98% of voters have one record, ~145,000 have two, ~3,500 have three to six.** That is ≈ **155,000 true duplicate pairs**. The duplicates are *temporal variants of the same person* — name/address changes and data-entry corrections over time — which is precisely the hard, realistic kind of duplicate.

**Version C — Generated-at-scale NC (the stress test).** Wingerath et al. (EDBT 2021, industrial paper) used historical NC voter data to *generate* a test dataset of **more than 120 million records with 640 million duplicate pairs**, with a largely error-free underlying gold standard. This is the option if we ever need >10M scale; it is a generator output, not raw registrations.

## 2. Fields (public file)

Per NCSBE's file layout and data pages: `ncid` (statewide voter id), county + `voter_reg_num`, first/middle/last name + suffix, residential and mailing address, city, ZIP, birth year / age (exact **birth date, SSN and driver's licence number are confidential by law and are NOT in the public file**), race, ethnicity, gender, party, registration date, voter status and status reason, precinct and district assignments. Files are linkable to the separate voter-history files via `ncid`.

Implication for ER features: no DOB and no SSN4 — matching must work from names, addresses and birth *year* only. This makes the benchmark harder and more honest than toy data.

## 3. Gold standard — the reason this dataset is valuable

- The **NCID follows the voter across counties** and uniquely identifies the individual; the literature uses it as the object id / gold cluster key. Gold pair completeness and reduction ratio are therefore *exactly* computable at any scale: the denominator Σ C(cluster size, 2) comes from the gold, not from running all-pairs matching (which at 8.3M records would be ~3.4×10¹³ pairs and is impossible — and unnecessary for evaluating *blocking*).
- Christen's own caveat, and ours: identity via the registration number is **"not 100% correct"** — a small share of voters in every snapshot carry more than one NCID, so gold clusters are near-perfect, not mathematically perfect. Any paper using NCVR must state this, and it is exactly what our gold-free audit direction (Direction 3 of the Oct 16 report) can cross-check on samples.

## 4. Construction caveats that shape the experiment design

1. **A single snapshot has (almost) no duplicates.** The task exists only in the temporal construction (Version B) or by stacking snapshots ourselves. Recommended: reproduce Christen's recipe from today's NCSBE snapshots (e.g., the January-1 snapshots across years), which also gives a *current* dataset instead of a 2014 one — or request Christen's original NCVR (he distributes it on request; availability/timeline unverified).
2. **Duplicates are longitudinal, not coincidental.** The same person's records differ by moves, name changes and corrections — cross-snapshot pairs. Blocking keys must survive address changes; pure address-based keys will systematically miss the movers. This is a feature (realistic difficulty), but it must be described as such.
3. **Exact duplicates were removed in Christen's construction.** Evaluation should follow the same rule when stacking snapshots, otherwise PC is inflated by trivial pairs.

## 5. Access and licensing

- Raw files: free public download from NCSBE (no account); the dataset is available **for noncommercial purposes** (per the EDBT description of the NC register's terms). Research use is the intended case; no redistribution of raw personal records in our repo — we publish code, aggregate results and derived statistics only. (Prudence note: the files contain real names/addresses of millions of people. Keep raw data on the local workstation, out of git.)
- Christen NCVR: available from the author on request (per his technical report). Timeline unverified.
- Precedent: NC voter data is an accepted ER benchmark — e.g., the 2025 *Algorithms* paper on blocking for LLM-era record linkage evaluates on NC voter data among its datasets (see our literature report, 2026-10-12).

## 6. Fit with the existing erbot package (code-audited 2026-10-20)

Already present:
- `ncvr_read(root, which = c("5", "10", "all"))` — reads NCVR CSV partitions (`*_nump_5.csv` / `*_nump_10.csv`) with data.table.
- `ncvr_guess_fields(df)` — maps both real NCVR columns and synthetic-partition naming to ER fields.
- `run_erbot_dataset("ncvr")` — end-to-end run with sorted-neighborhood blocking (window 40); truth must be supplied by the caller.

Missing (to build in the scale track):
- No `er_load("ncvr")` / `er_load_gold("ncvr")`: the NCID→gold-cluster builder does not exist yet.
- No snapshot-stacking constructor (Christen recipe) producing records + gold from raw NCSBE files.
- E1/E2 are not wired to NCVR at all, and their prototype implementations do not scale (see the scale-track design doc).

## 7. Recommendation

Adopt NCVR as the scale benchmark, in Christen's temporal construction, with NCID gold and the stated caveat. Build order: (1) snapshot-stacking constructor + NCID gold builder; (2) cluster-preserving subsample ladder (100k → 500k → 1M → full) so every method is debugged small and measured large; (3) scale implementations of E1/E2 per the companion design doc. D10K and MusicBrainz 20K remain the correctness anchors where exact no-blocking comparisons are still possible.
