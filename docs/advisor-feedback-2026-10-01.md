# ERBOT advisor feedback tracker — 2026-10-01

Source meeting: 2026-09-25, recorded in `~/workspace/erbot-mentor-2026-09-25.md`.
Meeting to prepare for: 2026-10-02.

This tracker is deliberately conservative. "Implemented" means code or a document
exists in this worktree. It does **not** mean the code has been run successfully
in Sam's Windows / VS Code R environment. This VM has no `R` / `Rscript`, so R
execution still has to be verified by Sam.

## Status summary

| # | Advisor request | Status on 2026-10-01 | What changed tonight | Still open |
|---|---|---|---|---|
| 1 | Clean the code: run any scenario quickly; errors should explain the reason and the fix | Partially implemented in `erbotv2`; the v3 Scenario 1/3 experiment harness is not in this repo | Added scenario presets, one-command runner, and friendly error wrapper (`R/21-scenarios.R`, `scripts/run_scenario.R`) | Sam's local Scenario 1/3 harness (`er_modular.R`, `main_multidataset.R`, `er_datasets.R`, if those are the current files) still has to be merged or pointed to; Sam must render/run once in VS Code |
| 2 | Understand why each success/failure happened | Evidence exists in the v3 report; not yet packaged as reusable explanation cards | Added mechanism cards (`docs/mechanism-cards-2026-10-01.md`) with phenomenon, numbers, mechanism, how to say it, and open questions | Sam should rehearse the cards in his own words; cards do not replace re-running the underlying experiments |
| 3 | Raise the positive ratio: stratified sampling by label, or append positives until a target ratio is reached | Design decision, not yet an accepted protocol | Added a pre-registered design and implementation (`R/22-positive_ratio.R`): entity-disjoint stratified three-way split, Fit-only positive-pair augmentation, natural-prevalence Validation/Test | Advisor must confirm whether label-aware split design is acceptable, and which target/Fit prevalence is wanted |
| 4a | Add D10K synthetic; compute a similarity matrix ourselves | Blocked by missing raw structured fields in this worktree | Added `er_parse_d10k_aggregate()` and `er_d10k_similarity_spec()` so the Aggregate Value text can be parsed into field-like columns when the raw file is available | The current loader keeps only `Id` and `Aggregate Value`; the local worktree does not contain `data/10Kfull.csv`. Real D10K verification needs the raw file |
| 4b | Add MusicBrainz 20K | Source and raw header verified from Leipzig (comma-delimited; header `TID,CID,CTID,SourceID,id,number,title,length,artist,album,year,language`) | Added `er_build_musicbrainz()` in `R/14-datasets.R`; updated the dataset catalog entry | Download the Leipzig file locally and run the builder once in Sam's R environment |
| 5 | Choose a paper direction: scaling/blocking innovation vs deep learning / embedding / LLM | Open decision for the 2026-10-02 meeting | Added one-page brief (`docs/paper-direction-brief-2026-10-01.md`) with a recommendation | Sam and advisor decide tomorrow |

## Item 1 — Clean the code

### Advisor's underlying problem

The advisor wants to ask for a situation — a dataset, a scenario, a failure case —
and see the result quickly. A raw stack trace is not enough; the failure needs a
reason and a next action.

### What already existed in `erbotv2`

- `er_run()` in `R/er_run.R` is already a nine-stage single entry point:
  load → diagnose → block → similarity → weights → combine → cluster → merge →
  evaluate/report.
- It prints stage-level progress when `verbose = TRUE`.
- `R/12-cv_protocols.R` already has `er_split()`, `er_protocol_a()`, and
  `er_protocol_b()`.

### The important gap

The v3 report's Scenario 1 screening and Scenario 3
Fit → Validation → Test harness are **not** in this repository. The current repo
has Protocol A/B, but not the report's admissibility checks, three-stage
positive-pair discipline, or the exact Scenario 1/3 experiment code. That code
appears to live only in Sam's local Windows / VS Code workspace. Tonight's work
therefore cleans the interface around `erbotv2`; it does not pretend to have
rewritten Sam's local experiment harness.

### Implemented tonight

- `R/21-scenarios.R`
  - `er_scenario_presets()`: named presets — `smoke`, `screening`, `full`,
    `unsupervised_quick`.
  - `er_run_scenario()`: resolves a preset, runs `er_run()`, and converts an
    error into a structured `erbot_scenario_error` with `stage`, `reason`, and
    `next_step`.
  - `er_explain_error()`: maps common failures (missing file, missing package,
    no similarity fields, label leakage, pair-budget explosion, all-NA sweep)
    to a human-readable cause and repair step.
- `scripts/run_scenario.R`
  - One-command entry point:
    `Rscript scripts/run_scenario.R --scenario smoke --data cora`.
- `reports/advisor_meeting_2026-10-02.qmd`
  - Quarto notebook for the meeting: renders to HTML, exposes parameters, and
    can re-run a guarded demo chunk when Sam enables it.

### Not done

- Sam's local Scenario 1/3 code has not been merged.
- No R execution has been verified in this VM (no R runtime here).

## Item 2 — Understand why success / fail

### Advisor's underlying problem

A table of ARI values is not an explanation. Each important success or failure
needs a mechanism: what structure in the data/design produced that number?

### Evidence already in the v3 report

The report already contains the raw evidence for the main mechanism cards:

- QDA inadmissibility disappears in 20/20 cells when two-way interactions are
  disabled, so the default refusal is a design-matrix artifact, not a property
  of the datasets.
- Logistic regression still fails after the ablation on most datasets, with
  fitted probabilities numerically 0 or 1 — the signature of complete
  separation; Firth/penalized logistic is the relevant repair path, not a bug
  fix.
- CORA seed 6666 has the highest validation ARI (0.946320) and the lowest test
  ARI (0.414960), with pair recall 0.996336, precision 0.290998, only 155 missed
  true pairs, and 102,684 false-positive co-clustered pairs. The mechanism is
  over-merging through transitive chaining.
- Validation often does not identify a unique model: exact ties survive after
  collapsing threshold duplicates (up to 60 distinct models / 12
  classifier–clusterer pairs in NCVR seed 5555).
- Restaurant test partitions contain only 9–13 positive pairs, so one pair
  decision moves the metrics visibly.

### Implemented tonight

- `docs/mechanism-cards-2026-10-01.md`: one card per mechanism, each with
  phenomenon, numeric evidence, mechanism, why the surface explanation is
  wrong, a sentence Sam can say to the advisor, and what remains unresolved.

### Not done

- The cards summarize evidence already in the report; they are not a substitute
  for Sam being able to explain the mechanisms without reading them.

## Item 3 — Raise the positive ratio

### Advisor's underlying problem

ER candidate pairs are extremely imbalanced. With only a handful of positive
pairs in Fit or Test, estimators may be inadmissible and test metrics become
coarse and split-sensitive.

### Tension with the existing discipline

The v3 design deliberately split records without looking at gold labels, and it
refused to redraw a seed when a split failed its positive-pair floors. That
discipline prevents seed-hacking. Any label-aware design therefore has to be
pre-registered: fixed seed, fixed quotas, no redraw after seeing results.

### Design implemented tonight

`R/22-positive_ratio.R` implements the design as code, but it is a proposal for
the advisor, not yet the canonical protocol:

- `er_stratified_three_way_split()`
  - Splits **entities**, not records or pairs, into Fit / Validation / Test.
  - Uses gold cluster labels only to balance entity-size strata and positive-pair
    quotas across partitions.
  - Uses one fixed seed and preserves the caller's RNG state.
  - If the requested positive-pair quotas are infeasible because entities are
    indivisible, it errors with the achieved counts instead of redrawing.
- `er_augment_fit_pairs()`
  - Operates on the **Fit partition only**.
  - Resamples positive Fit pairs (with replacement) until a target Fit positive
    ratio is reached.
  - Never touches Validation or Test.
  - Returns pair weights so duplicated positives can be down-weighted; the
    augmented view is explicitly labelled `simulated_prevalence = TRUE`.
- `er_positive_ratio_report()`
  - Reports natural prevalence per partition and, if augmentation was used, the
    simulated Fit prevalence separately. The two are never mixed.

### Decisions the advisor must confirm

1. Is a label-aware, pre-registered split design acceptable, given the previous
   "split does not look at gold" discipline?
2. Which target matters: minimum positive pairs per partition, or a Fit positive
   ratio? The code supports quotas first and ratio second.
3. Should augmented Fit pairs be duplicated with weights, or should positives
   be appended as extra candidate pairs only inside Fit?

## Item 4a — D10K similarity matrix

### Current state

- `er_load("d10k")` in `R/01-load.R` reads only `Id` and `Aggregate Value`, and
  renames the latter to `text`.
- That is enough for a single text similarity, but not for a field-wise
  similarity matrix.
- This worktree does not contain `data/10Kfull.csv`, so the real file cannot be
  inspected or run here.

### Implemented tonight

- `er_parse_d10k_aggregate()` and `er_d10k_similarity()` in
  `R/23-d10k_similarity.R`: parses the D10K `Aggregate Value` string
  into field-like columns when structure is present, builds an
  `er_similarity()` spec, and returns the combined sparse similarity matrix.
- `er_d10k_similarity_spec()`: returns an `er_similarity()` field-spec list
  (JW for short text, Jaccard/BoW for longer text, year/numeric/categorical
  where detected). Missing values stay `NA`; nothing is imputed at the
  similarity layer.

### Still blocked

- Needs the real `10Kfull.csv` (or the exact D10K source file) to confirm the
  Aggregate Value format and to run the spec end-to-end.

## Item 4b — MusicBrainz 20K

### Source verification

Official source: Leipzig University Database Group,
"Benchmark datasets for entity resolution". The landing page lists Music Brainz
20K as 19,375 entities, 16,250 matches, 10,000 clusters, 5 sources, attributes
artist / title / album / year / length, generated from real MusicBrainz records
with the DAPO generator at a high corruption rate. The official readme defines
`TID` (unique record id), `CID` (cluster id = ground truth), `CTID` (within-cluster
id), `SourceID` (one of five deduplicated sources), `Id` (original source id,
not guaranteed unique, can be ignored), `number`, `length`, `artist`, `year`,
and `language`.

The raw file's header and delimiter were verified on 2026-10-01: comma-delimited,
header `TID,CID,CTID,SourceID,id,number,title,length,artist,album,year,language`.
Values are messy (e.g. length can be `1m 58sec`, year can be `75` or `null`), so
the builder still relies on `data.table::fread()` type handling and validates the
required columns after normalisation instead of assuming clean types.

### Implemented tonight

- `er_build_musicbrainz()` in `R/14-datasets.R`:
  - Reads the Leipzig `.dapo` file.
  - Normalises column names.
  - Requires `tid` and `cid` after normalisation; errors with a friendly reason
    if they are absent.
  - Writes `data/musicbrainz20k.csv` and `data/musicbrainz20k_gold.csv`.
  - `CID` becomes gold `cluster_id` only; it is not a model feature.
  - Model fields are carried through as ordinary columns (title, artist, album,
    year, length, language, number, source).
- `er_load("musicbrainz20k")` and `er_load_gold("musicbrainz20k")` support.
- Catalog entry updated in `docs/datasets/DATASET_CATALOG.md`.

### Not done

- The builder has not been run on the real file in this VM (no R runtime, and
  the file is not present locally).

## Item 5 — Paper direction

See `docs/paper-direction-brief-2026-10-01.md`.

Short version: the brief recommends **Scaling / blocking** as the primary
direction, because it continues the existing candidate-space work, needs less
new machinery, and sits closer to the advisor's expertise. Deep learning /
embedding / LLM remains a credible second direction, but it carries higher
setup cost and reviewing risk, and the advisor has said it is open territory
for him as well. The decision belongs to Sam and the advisor tomorrow.
