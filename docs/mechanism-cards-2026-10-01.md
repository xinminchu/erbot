# ERBOT mechanism cards — 2026-10-01

Purpose: answer the advisor's second request — *understand why that success or
fail* — in a form Sam can explain without reading a table aloud.

Each card has the same shape: phenomenon → numeric evidence → mechanism → why
the surface explanation is wrong → one sentence Sam can say → what is still
unresolved. Numbers are from the v3 report
(`ERBOT_Multi_Dataset_Research_Report_v3_2026-09-17.pdf`).

---

## Card 1 — QDA was refused because the design matrix was too wide, not because QDA "does not work" on these datasets

**Phenomenon.** Under the default configuration, QDA (and logistic regression)
are admissible on exactly the two datasets whose design matrix has at most
three columns, and refused on all four with ten or more.

**Evidence.** With two-way interactions on, comparison features expand as
p → p + C(p, 2): Affiliation 1 → 1, CORA 2 → 3, Restaurant/DBLP 4 → 10,
NCVR 5 → 15, FEBRL1 9 → 45. FEBRL1 has only 37–47 minority-class (positive)
pairs in Fit. When interactions are switched off and nothing else changes,
QDA is restored in **20/20** dataset-by-seed cells.

**Mechanism.** QDA estimates a separate covariance matrix per class. If the
number of predictors is close to or above the minority-class count, that
covariance is rank-deficient and the estimator is not identifiable. The
interaction expansion manufactured predictors faster than the split supplied
positive pairs. The refusal rule correctly detected non-estimability — of a
design the framework itself had created.

**Why the surface explanation is wrong.** "QDA is not applicable to FEBRL1" is
false. The same data, same splits, same seeds admit QDA once the expansion is
off. The inadmissibility was a property of the configuration, labelled as a
property of the data.

**Say it in one sentence.** "QDA's refusal was an artifact of our two-way
interaction expansion colliding with a small positive class; with the expansion
off, QDA fits in all twenty cells, so admissibility here is a design property,
not a dataset property."

**Still unresolved.** Restoring admissibility does not make QDA good: on NCVR
the ablation reaches only 0.9158 mean test ARI vs 0.9886 for the full search.
Admissibility and accuracy are separate questions.

---

## Card 2 — Logistic regression fails for a different reason: complete separation, and Firth is the repair path

**Phenomenon.** In the no-interaction ablation, logistic regression is restored
only on DBLP (4/5 seeds). Elsewhere it still fails — but the failure signature
changes from rank deficiency to non-convergence with fitted probabilities
numerically 0 or 1.

**Evidence.** The v3 ablation table records the residual logistic failure as
"did not converge" on Restaurant, FEBRL1, and NCVR, and "aliased coefficients"
on DBLP seed 6666.

**Mechanism.** Fitted probabilities of exactly 0/1 are the fingerprint of
complete (or quasi-complete) separation: within the Fit partition, the
comparison features separate matches from non-matches so well that the
maximum-likelihood estimate diverges — coefficients run off to infinity and
the optimiser never converges. This is a genuine property of the data/split,
not of the design matrix: narrowing the design does not remove a separating
hyperplane.

**Why the surface explanation is wrong.** Two different failures were being
filed under one label, "logistic not estimable". Rank deficiency is fixable by
changing the design. Separation is not; it needs a penalised or Bayesian
estimator (Firth logistic regression is the standard small-sample repair).

**Say it in one sentence.** "Logistic regression and QDA were refused for
different reasons: QDA was blocked by our own feature expansion, while logistic
hits complete separation, which is real structure in the data and needs a
penalised fit such as Firth rather than a design tweak."

**Still unresolved.** Firth has not yet been run inside the pipeline as the
separation-safe path; the current code can only say "refused", not "refused
under ML, here is the Firth result".

---

## Card 3 — CORA seed 6666: the best validation score produced the worst test partition, through over-merging

**Phenomenon.** CORA seed 6666 has the highest validation ARI of the five CORA
seeds and the lowest test ARI.

**Evidence.** Validation ARI 0.946320 (best of five); test ARI 0.414960 (worst
of five). Test pair recall 0.996336, pairwise precision 0.290998. Only 155 true
test pairs are missed, but 102,684 false-positive co-clustered pairs are
created. The largest predicted entity has 496 records; the largest true entity
has 199.

**Mechanism.** This is not a model that "missed matches". It found almost every
true pair (recall ≈ 1) and then chained several true entities into one
component through spurious links plus transitive closure. Once a few false
bridges exist, connected-component/threshold clustering merges whole entities,
and each merged entity contributes a quadratic number of false pairs — hence
102,684 FP against 155 FN.

**Why the surface explanation is wrong.** "ARI dropped, so the model got
worse at matching" misses the asymmetry. A single aggregate (even pairwise F1)
cannot distinguish over-merging from fragmentation; the pair decomposition
(TP 42,145 / FP 102,684 / FN 155) and the largest-entity comparison can.

**Say it in one sentence.** "Seed 6666 did not fail by missing matches — it
failed by over-merging: near-perfect recall, precision under 0.3, and one
496-record predicted entity where the truth has at most 199, so a few false
bridge links chained whole entities together."

**Still unresolved.** Seed 6666 is the only CORA run using both XGBoost and
threshold clustering, so classifier, clusterer, and split effects cannot be
separated from one cell. ARI alone also under-penalises this failure shape;
entity-size diagnostics should accompany it.

---

## Card 4 — Validation often does not identify a single model; the "winner" is a tie-breaking artifact

**Phenomenon.** On Restaurant, FEBRL1, and NCVR the validation objective is
exactly flat over many configurations. Four Restaurant seeds, all FEBRL1 seeds,
and all NCVR seeds reach validation ARI 1.0.

**Evidence.** After collapsing pure threshold duplicates, the
validation-optimal equivalence set still contains up to 60 distinct models and
12 distinct classifier–clusterer pairs (NCVR seed 5555). NCVR records
KNN + threshold clustering in all five seeds even though KNN, weighted KNN,
and NNET each attain the optimum in all five.

**Mechanism.** Two causes flatten the criterion. (1) *Saturation*: with only
11–18 validation positive pairs (Restaurant) or 39–47 (FEBRL1), ARI reaches its
ceiling easily and cannot discriminate further. (2) *Quantisation*: an
unweighted KNN with k neighbours emits probabilities on a grid of spacing 1/k,
and a shallow decision tree emits near-degenerate leaf probabilities, so a wide
range of clustering thresholds induces the *identical* partition. The
first-maximum rule then converts enumeration order into a "selected model".

**Why the surface explanation is wrong.** The recorded winner is not "the best
model"; it is the first member of an equivalence set under a deterministic
tie-break. Reporting model frequencies across seeds as evidence of superiority
overstates what validation established.

**Say it in one sentence.** "On three datasets validation saturates: many
genuinely different models tie at ARI 1.0, so the recorded winner reflects our
tie-breaking order, not demonstrated superiority — the equivalence set is the
result, not the single winner."

**Still unresolved.** A finer selection criterion (or deduplicating the grid by
induced partition before counting ties) has not been implemented. Affiliation
is the cautionary negative case: a unique optimum in every seed, and the
lowest test ARI in the study — identifiability and accuracy are separate.

---

## Card 5 — Sparse positives make test metrics coarse and split-sensitive (Restaurant)

**Phenomenon.** Restaurant's test ARI ranges from 0.761831 to 0.947354 across
five seeds, with descriptive SD 0.081447 — roughly eight to ten times the
spread on DBLP, FEBRL1, or NCVR.

**Evidence.** Restaurant has 110 gold positive links in its full cross-source
candidate universe, but record-level splitting leaves only **9–13 positive
pairs in each test partition**.

**Mechanism.** With ~10 positives, a single pair decision moves recall by
about 1/10 and visibly moves precision, F1, and ARI. The metric is not noisy
because the model is unstable; it is coarse because the denominator is tiny.
Positive-pair counts per partition are part of the statistical design and
should be reported next to every score.

**Why the surface explanation is wrong.** "Restaurant results vary a lot, so
the method is unreliable on Restaurant" confuses measurement precision with
model quality. But the caveat is also not universal: CORA has hundreds of
positives and the largest spread anyway (Card 3), so sparsity is neither
necessary nor sufficient for instability — it is a measurement-precision
warning, not a causal law.

**Say it in one sentence.** "Restaurant's test splits contain only nine to
thirteen positive pairs, so one pair decision moves the score; that spread is
mostly measurement coarseness, and it is exactly what the positive-ratio design
(Item 3) is meant to fix — on the Fit side, without touching natural Test
prevalence."

**Still unresolved.** Item 3's label-aware split design needs the advisor's
sign-off (see tracker), because it changes the previous "split never looks at
gold" discipline.

---

## Card 6 — Affiliation fails in both directions at once: fragmentation *and* false merging

**Phenomenon.** Affiliation has the lowest test ARI in the study (mean 0.2835),
with pairwise recall only 0.166–0.250 and pairwise precision 0.324–0.632.

**Evidence.** Across five seeds the run predicts 448–530 entities against
270–275 true entities (fragmentation), yet in seeds 3333 and 4444 false
positives exceed true positives (651 TP vs 1,246 FP; 680 TP vs 1,421 FP) —
about two thirds of predicted links are wrong (false merging).

**Mechanism.** Affiliation has a single comparison feature. One weak signal
cannot separate the classes at any threshold: a low threshold merges unrelated
strings, a high threshold splits true entities, and every intermediate setting
does some of both. The failure is a signal-strength problem, not a
threshold-tuning problem.

**Why the surface explanation is wrong.** It is tempting to classify failures
as "over-merging" (CORA) vs "under-merging" (Affiliation) as if they were
opposites. Affiliation does both simultaneously; there is no clean inverse of
the CORA failure anywhere in the study. Only the joint pattern of precision,
recall, entity counts, and entity sizes shows this.

**Say it in one sentence.** "Affiliation fails two-sidedly — it splits true
entities and merges unrelated ones at the same time — because a single
comparison feature is too weak to separate the classes at any threshold."

**Still unresolved.** Whether adding comparison features (or an embedding
view) rescues Affiliation is an empirical question; it is also one bridge to
the deep-learning paper direction.

---

## Card 7 — The candidate universe is part of the task definition; blocking is a different operation

**Phenomenon.** Restaurant originally generated all 372,816 unordered pairs
among 864 records. The corrected Fodors–Zagat linkage universe is the
cross-source product 533 × 331 = 176,423 pairs; the 196,393 removed pairs are
within-source pairs, every one a known negative, and all 110 gold positives
remain.

**Evidence.** After the correction, Restaurant was rerun from Scenario 1 and
its Scenario 3 model space re-derived; the corrected run is the only
Restaurant result used in the main tables. Separately, the report notes a case
where a blocking strategy chosen by scoring against gold achieves recall 1.0
by construction.

**Mechanism.** A cross-source restriction is a *semantic* definition of which
pairs are eligible; blocking is a *computational* approximation with a
measurable recall trade-off. Merging the two under one label ("candidate
generation") hides which numbers are task definitions and which are
approximations. Likewise, blocking recall obtained by selecting the strategy
against gold is a selection criterion, not generalisation evidence.

**Why the surface explanation is wrong.** "We reduced pairs by 52.68%, so
blocking worked" would be a category error: the Restaurant reduction changed
the task being measured, not the efficiency of measuring it.

**Say it in one sentence.** "The Restaurant correction was not a blocking
improvement — it fixed the task definition to cross-source pairs only;
blocking is a separate, computational choice whose recall must be reported
together with how the strategy was selected."

**Still unresolved.** This distinction is the seed of the scaling paper
direction: record-level blocking and pair-level blocking are different
objects, and the advisor has already flagged that they must not be conflated.

---

## Card 8 — Scenario 1 and Scenario 3 answer different questions; mixing them inflates confidence

**Phenomenon.** Scenario 1 screens the full model space on one split (seed
2222) and supplies rule-based applicability exclusions to Scenario 3, which
then runs Fit → Validation select → Test evaluated once, over five fixed
seeds. Scenario 2 (fitting-fraction sweep) was defined but never run.

**Evidence.** Records are drawn once, without reference to entity labels
(source-aware stratification for cross-source tasks); no seed search is
performed; the framework errors rather than redraw when a split fails its
minimum-pair floors. Pairs whose records fall in different partitions are
dropped entirely (34.1–66.5% of candidates are never evaluated). Seed 2222 is
used by both Scenario 1 and the first Scenario 3 cell — a recorded caveat.

**Mechanism.** Scenario 1 answers "what is estimable at all, and roughly what
does the model space look like?" Scenario 3 answers "with selection confined
to Validation and Test touched once, what held-out score does the selected
configuration get?" If screening information (validation scores, grid-boundary
behaviour) quietly prunes the Scenario 3 space, Scenario 3's estimate inherits
some of Scenario 1's optimism even though Test is never touched.

**Why the surface explanation is wrong.** "Test was evaluated once, therefore
the estimate is unbiased" is too strong. Honesty here is a chain: split
discipline, selection discipline, and reporting discipline (ties, positive
counts, candidate universe) each guard a different leak.

**Say it in one sentence.** "Scenario 1 tells us what is estimable, Scenario 3
tells us what the selected configuration scores on untouched Test — and the
credibility of the second depends on the first not quietly leaking selection
information into it."

**Still unresolved.** Scenario 2 was never run, so no claim exists about
sensitivity to the Fit/Validation/Test allocation. Sam's local Scenario 1/3
harness is also not yet merged into erbotv2 (see tracker Item 1).
