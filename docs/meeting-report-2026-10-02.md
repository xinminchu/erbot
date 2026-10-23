# ERBOT progress report for the 2026-10-02 advisor meeting

Prepared 2026-10-01. Status words are used strictly:

- **Done** = the code/document exists in this worktree.
- **Not yet verified** = it has not been run in Sam's Windows / VS Code R
  environment. This preparation environment has no R runtime, so nothing below
  is claimed as "runs" until Sam renders/runs it once.
- **Not done** = explicitly still open.

## The five requests, one by one

### 1. Clean the code — *implemented, not yet verified*

What existed: `er_run()` already runs the nine-stage pipeline in one call, with
stage messages. What was missing: a named way to say "run *this* situation",
and errors that explain themselves.

Added:

- `R/21-scenarios.R` — `er_scenario_presets()` (`smoke`, `screening`, `full`,
  `unsupervised_quick`), `er_run_scenario()`, and `er_explain_error()`. A
  failure now reports the stage, the likely reason, and the next repair step
  (missing file, missing package, label leakage, pair-budget explosion,
  all-NA sweep, infeasible quota), not just a stack trace.
- `scripts/run_scenario.R` — one command:
  `Rscript scripts/run_scenario.R --scenario smoke --data cora`.
- `reports/advisor_meeting_2026-10-02.qmd` — the Quarto notebook version of
  this report: renderable to HTML, parameterised, and able to re-run a guarded
  demo chunk live in the meeting.

**Honest gap.** The v3 report's Scenario 1 / Scenario 3 harness is *not* in
this repository (the repo has Protocol A/B in `R/12-cv_protocols.R`, not the
report's admissibility/three-stage harness). That code lives in Sam's local
VS Code workspace. Tonight's work cleans the erbotv2 interface; merging the
local harness is still **not done** and needs Sam to point at or hand over
those files.

### 2. Understand why success / fail — *done as documents*

`docs/mechanism-cards-2026-10-01.md` — eight cards, each with phenomenon,
numeric evidence, mechanism, why the surface reading is wrong, one sentence to
say aloud, and what remains unresolved:

1. QDA's refusal is an interaction-expansion artifact (restored 20/20 without
   interactions), not a property of the datasets.
2. Logistic regression instead hits complete separation; Firth is the repair
   path, and the two refusals must not be filed under one label.
3. CORA seed 6666: best validation ARI (0.946320), worst test ARI (0.414960);
   recall 0.996, precision 0.291, 155 FN vs 102,684 FP — over-merging by
   transitive chaining, which ARI alone does not diagnose.
4. Validation ties: up to 60 distinct models / 12 classifier–clusterer pairs
   tie at the optimum (NCVR seed 5555); the recorded winner is a tie-breaking
   artifact. Saturation (few validation positives) and quantisation (KNN/tree
   probability grids) are the two causes.
5. Sparse positives: Restaurant test partitions hold 9–13 positive pairs; one
   pair decision moves the metrics. A measurement-precision caveat, not a
   universal instability law.
6. Affiliation fails in both directions at once (recall 0.17–0.25, precision
   0.32–0.63) because a single comparison feature is too weak at any threshold.
7. The candidate universe is part of the task definition (Restaurant:
   372,816 all-pairs → 176,423 cross-source pairs); blocking is a separate
   computational approximation, and gold-selected blocking recall is a
   selection criterion, not generalisation evidence.
8. Scenario 1 (what is estimable) vs Scenario 3 (Fit → Validation select →
   Test once): credibility is a chain of three disciplines, not one "Test was
   untouched" claim. Scenario 2 was never run.

### 3. Raise the positive ratio — *designed and implemented as a proposal; needs the advisor's sign-off*

`R/22-positive_ratio.R` implements the design without breaking the split
discipline:

- `er_stratified_three_way_split()` splits **entities** into Fit/Validation/
  Test with pre-registered fractions and positive-pair quotas, one fixed seed,
  caller RNG preserved. If indivisible entities make a quota infeasible, it
  errors with the achieved counts — it never redraws a seed.
- `er_augment_fit_pairs()` resamples positive pairs **inside Fit only** to a
  target Fit prevalence, marks the result simulated prevalence, and reports
  natural vs simulated prevalence separately, forever.
- Validation and Test always keep natural prevalence. Gold labels design the
  split; they never enter similarity features (BUG-22 guard intact).

**Decision for the advisor:** accept a label-aware, pre-registered split
design? Prefer quotas (minimum positive pairs) or a Fit prevalence target?
Duplicate weighted positives, or append positives as extra Fit candidate pairs
only?

### 4. More datasets — *implemented, not yet verified on the real files*

**D10K.** `R/23-d10k_similarity.R`: `er_parse_d10k_aggregate()` parses the
Aggregate Value column into field-like columns where structure exists
(key=value or a consistent separator) and otherwise keeps one text field;
`er_d10k_similarity()` blocks, computes per-field similarities, and returns the
combined sparse similarity matrix `S`. Missing fields stay NA and are handled
by the NA-aware combiner. *Blocker:* `data/10Kfull.csv` is not in this
worktree, so the parser has not been run on the real Aggregate Value format.

**MusicBrainz 20K.** Source verified from the Leipzig Database Group page:
19,375 records, 16,250 matches, 10,000 clusters, 5 sources, high-corruption
DAPO duplicates. Raw header verified 2026-10-01: comma-delimited,
`TID,CID,CTID,SourceID,id,number,title,length,artist,album,year,language`
(values are messy — length can be `1m 58sec`, year can be `75` or `null`).
`er_build_musicbrainz()` (in `R/14-datasets.R`) writes
`data/musicbrainz20k.csv` (model fields only: title, artist, album, year,
length, language, number, source) and `data/musicbrainz20k_gold.csv`
(`CID` → `cluster_id`, gold only, never a feature). `er_load("musicbrainz20k")`
and `er_load_gold("musicbrainz20k")` are wired; the catalog is updated.
*Remaining:* the builder has not yet run on the real file in an R environment.

### 5. Paper direction — *brief written; decision tomorrow*

`docs/paper-direction-brief-2026-10-01.md` recommends **scaling / blocking** as
the primary direction: strongest continuity with the candidate-space work,
least new machinery, closest to the advisor's expertise — with the
record-level vs pair-level blocking distinction as the precise ownable
question. It keeps deep learning warm with one cheap probe (an embedding view
on Affiliation under the existing protocol). The decision is for the meeting.

## What Sam needs to do in VS Code (about 10 minutes)

1. Open the `erbot-work` folder. In the integrated terminal:
   `quarto --version` and `quarto check` (confirms the CLI that renders).
2. Run the new tests:
   `Rscript tests/run_advisor_feedback_tests.R`
3. Render the meeting notebook:
   `quarto render reports/advisor_meeting_2026-10-02.qmd`
   and open the HTML. To re-run a scenario live in the meeting, set the
   `run_demo` parameter to `true` (and `dataset` if needed) and re-render.
4. Optional smoke run:
   `Rscript scripts/run_scenario.R --scenario smoke --data cora`

## If the advisor asks "show me a failure"

Run the smoke scenario on a dataset with the truth accidentally left in the
features, or point at mechanism card 1/2: the error explanation and the
QDA/logistic distinction are the two fastest demonstrations that failures now
come with reasons.

## Speaking outline (5 minutes)

1. "I went through your five points one by one; here is the status of each,
   including what is not done." (tracker)
2. "Failures now explain themselves: scenario presets plus errors with stage,
   reason, and fix." (Item 1; name the local-harness gap honestly.)
3. "Three mechanisms I can now explain: QDA was our design matrix, logistic is
   separation and needs Firth, and CORA 6666 is over-merging that ARI alone
   misses." (Cards 1–3.)
4. "For positives, I am proposing a pre-registered design rather than a quiet
   fix: entity-level stratified split with quotas, augmentation in Fit only,
   natural Test prevalence always. I need your sign-off because it changes the
   split discipline." (Item 3.)
5. "Datasets: D10K similarity layer is written but needs the real file;
   MusicBrainz builder is written from the Leipzig source, gold kept out of
   the features." (Item 4.)
6. "Direction: I recommend scaling/blocking, with the record-level vs
   pair-level distinction as the question; deep learning stays as a cheap
   Affiliation probe. Your call." (Item 5.)

## Full status table

| Item | Done | Not yet verified | Not done |
|---|---|---|---|
| 1. Clean code | Presets, one-command runner, friendly errors, Quarto notebook | Any R execution (tests, smoke run, render) | Merge Sam's local Scenario 1/3 harness into erbotv2 |
| 2. Mechanisms | 8 mechanism cards from v3 evidence | Sam's own-word rehearsal | Firth as the in-pipeline separation-safe path |
| 3. Positive ratio | Stratified split + Fit-only augmentation code | Advisor sign-off; any run on real data | Choosing quotas vs Fit prevalence target as the canonical rule |
| 4a. D10K | Parser + similarity-matrix code | Run on real 10Kfull.csv | Confirm real Aggregate Value format |
| 4b. MusicBrainz | Builder, loader, gold loader, catalog; raw header/delimiter verified | Run builder on real .dapo file in R | Confirm messy year/length values behave under er_diagnose() |
| 5. Direction | One-page brief with recommendation | — | The decision itself (tomorrow) |
