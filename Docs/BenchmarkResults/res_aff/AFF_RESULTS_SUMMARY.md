# ERBOT Results Summary — Affiliation Strings Dataset

**Generated:** 2026-03-22
**Pipeline version:** ERBOT v0.2.0
**Run time:** ~5.6 hours (20,280s)

---

## 1. Dataset Overview

| Property | Value |
|---|---|
| Dataset | Affiliation Strings |
| Records | 2,260 |
| Fields | 2 (id, affiliation) |
| Task | Deduplication (single-source) |
| Ground truth | 32,816 matched pairs (from `affiliationstrings_mapping.csv`) |

Affiliation strings is a benchmark of research paper author affiliations — free-text strings like
"Dept. of Computer Science, MIT, Cambridge, MA" that refer to real-world institutions. The same
institution appears under many surface forms: abbreviations, missing department names, country
variants, spelling differences, and language variations. The dataset has only one content field
(`affiliation`), making it a hard single-field deduplication task.

---

## 2. Pipeline Configuration

| Stage | Setting | Value |
|---|---|---|
| Blocking | Method | Prefix blocking on `affiliation`, length 3 |
| Blocking | Candidate pairs | 164,719 |
| Blocking | Reduction ratio | 0.935 (93.5% of all pairs eliminated) |
| Similarity | Fields computed | 1 (affiliation only) |
| Weights | Method | Equal (weight = 1.0 for affiliation) |
| Clustering | Methods run | 9 |
| Merge | Strategy | Best |

**Blocking note:** 164,719 pairs from 2,260 records (2,547,670 total possible). Prefix-3 on
`affiliation` eliminates 93.5% of pairs. However, affiliation strings are highly variable in their
opening tokens — "University of X", "Dept. of Y, University of X", "X University", and
"X Univ." all refer to the same institution but have different 3-character prefixes. This means
blocking likely discards many true matches before evaluation even begins, directly limiting recall
for all methods.

---

## 3. Performance Results

All 9 clustering methods evaluated against the gold standard:

| Method | ARI | NMI | VI | B³-P | B³-R | B³-F | Homog. | Complet. | V-meas. | PairF-P | PairF-R | PairF-F |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **threshold_cc** | **0.076** | 0.688 | 4.24 | **0.395** | 0.550 | **0.460** | 0.606 | 0.796 | 0.688 | — | — | — |
| **louvain** | **0.076** | 0.688 | 4.24 | 0.394 | 0.550 | 0.459 | 0.606 | 0.796 | 0.688 | — | — | — |
| **label_prop** | **0.076** | 0.688 | 4.24 | 0.394 | 0.550 | 0.459 | 0.606 | 0.796 | 0.688 | — | — | — |
| leiden | 0.000 | **0.819** | **3.42** | 1.000 | 0.146 | 0.255 | 1.000 | 0.693 | **0.819** | — | — | — |
| svm | 0.035 | **0.818** | **3.40** | **0.977** | 0.159 | 0.273 | **0.990** | 0.697 | **0.818** | — | — | — |
| hclust_avg | 0.053 | 0.516 | 5.84 | 0.184 | 0.530 | 0.272 | 0.404 | 0.716 | 0.516 | — | — | — |
| pam | 0.053 | 0.516 | 5.84 | 0.184 | 0.530 | 0.272 | 0.404 | 0.716 | 0.516 | — | — | — |
| hclust_ward | 0.042 | 0.516 | 5.89 | 0.185 | 0.519 | 0.273 | 0.406 | 0.706 | 0.516 | — | — | — |
| gc | 0.000 | 0.000 | 7.72 | 0.007 | 1.000 | 0.014 | 0.000 | 1.000 | 0.000 | — | — | — |
| **final** | **0.076** | 0.688 | 4.24 | **0.395** | 0.550 | **0.460** | 0.606 | 0.796 | 0.688 | — | — | — |

*Metrics: ARI = Adjusted Rand Index; NMI = Normalized Mutual Information; VI = Variation of
Information (lower is better); B³ = B-cubed; V-meas. = V-measure.*

*Note: PairF columns omitted from table for space; full values in `er_performance.csv`.*

---

## 4. Final Cluster Assignment

The `final` strategy correctly selected **threshold_cc** (highest ARI = 0.076).

| Stat | Value |
|---|---|
| Total clusters | 290 |
| Records | 2,260 |
| Mean cluster size | 7.8 |

290 clusters from 2,260 records is a reasonable partition for an affiliation dataset where each
real institution may appear under dozens of surface forms.

---

## 5. Method-by-Method Interpretation

### Threshold-CC / Louvain / Label Propagation (Best — ARI = 0.076)
All three produce nearly identical results (as with CORA), again indicating the similarity graph
has dominant structure that overwhelms algorithm differences. B³-F of 0.460 reflects a reasonable
balance: precision 0.395 (some false merges) and recall 0.550 (misses about half of true matches).
These graph-based methods are the practical ceiling given equal-weight single-field similarity.

### SVM (ARI = 0.035 — paradoxically worse than threshold-CC)
SVM has near-perfect precision (0.977) and near-perfect homogeneity (0.990) but extremely low
recall (0.159). It is so conservative that it only merges pairs it is almost certain about,
producing many small, pure clusters — but missing the vast majority of true duplicates. On a
single-field dataset with high surface-form variability, SVM cannot generalise beyond the
high-confidence pairs in its training signal, making it less useful than the simpler threshold
approach. High NMI (0.818) reflects the purity of its clusters, not their coverage.

### Leiden (Degenerate — ARI = 0.000)
Perfect homogeneity (1.0) and highest NMI (0.819) but near-zero recall (0.146). Like SVM,
Leiden over-partitioned — the resolution parameter is too high for this dataset, creating hundreds
of tiny, pure sub-clusters instead of merging variants of the same institution. The VI score
(3.42) is the best of the non-SVM methods despite ARI = 0, reflecting high per-cluster purity.
Needs a lower resolution parameter.

### GC — Graph Coloring (Degenerate — ARI = 0.000)
Perfect recall (1.0) but near-zero precision (0.007), collapsing all 2,260 records into
effectively one cluster. GC continues to fail at the default threshold — the gc bug
(dimension mismatch error) also caused it to fall back to a degenerate solution.

### PAM / hclust_avg / hclust_ward (ARI ≈ 0.042–0.053)
All three are below threshold-CC. They find some structure but the similarity matrix from a
single noisy text field is insufficiently informative to guide centroidal or hierarchical methods
toward the true partition.

---

## 6. Key Findings

1. **Overall performance is low.** Best ARI is 0.076 — far below CORA (0.746). This is
   expected: affiliation deduplication is intrinsically harder because (a) only one field is
   available, (b) surface-form variability is extreme (abbreviations, reorderings, language
   variants), and (c) prefix-3 blocking likely discards many true matches before evaluation.

2. **Blocking is the primary bottleneck.** Prefix-3 on `affiliation` is poorly suited to this
   dataset. Affiliation strings start with highly variable tokens ("University of", "Dept.", city
   names, acronyms). Many true duplicates will have non-matching 3-character prefixes and are
   silently dropped at Stage 3. This single issue likely accounts for most of the low recall
   across all methods.

3. **SVM precision–recall tradeoff is extreme.** Precision = 0.977, recall = 0.159. SVM
   learns to be extremely conservative on this noisy single-field data — it finds the easy
   matches but ignores the hard ones. For high-precision applications (where false merges are
   costly) SVM is useful; for high-recall applications it is not.

4. **Graph methods are the practical best.** Threshold-CC, Louvain, and Label Propagation
   achieve the best balanced B³-F (0.460) and are robust, consistent, and fast relative to SVM.

5. **Equal weights are not the issue here.** With only one field, weight learning cannot help.
   The limiting factors are blocking and similarity function choice.

6. **The `final` merge correctly picked threshold_cc.** The merge bug fix is working as
   expected across datasets.

---

## 7. Recommendations for Next Steps

| Priority | Action |
|---|---|
| 🔴 High | Replace prefix-3 blocking with SN blocking (sorted neighbourhood, window=40–60) on `affiliation` — far better suited to variable-length strings with no consistent prefix |
| 🔴 High | Use token-set ratio or Jaccard on token sets as the similarity function instead of default string similarity — affiliation strings are bag-of-words problems, not edit-distance problems |
| 🟡 Medium | Tune Leiden resolution (try 0.05–0.2) to reduce over-splitting |
| 🟡 Medium | Fix GC dimension mismatch error — gc falls back to a degenerate solution silently |
| 🟡 Medium | Measure pair completeness at Stage 3 to quantify how many true matches blocking discards |
| 🟢 Low | Try TF-IDF + cosine similarity with IDF trained on the full affiliation corpus — common tokens like "University", "Department" should be down-weighted |
| 🟢 Low | Try record linkage with an external affiliation authority list (ROR, OpenAlex institutions) as a lookup layer before ER |

---

## 8. Comparison with CORA

| Metric | CORA | Affiliation | Interpretation |
|---|---|---|---|
| Records | 1,879 | 2,260 | Similar scale |
| Fields | 15 (usable) | 1 | Affiliation severely disadvantaged |
| Best ARI | 0.746 (hclust_avg) | 0.076 (threshold_cc) | ~10× harder |
| Best B³-F | 0.738 | 0.460 | Large gap |
| Best method | hclust_avg | threshold_cc | Different winners |
| Final correct? | Yes | Yes | Merge fix working |
| Blocking RR | 92.2% | 93.5% | Similar reduction |
| Runtime | ~63 min | ~338 min | SVM bottleneck |

The performance gap is almost entirely explained by the field count: CORA has 15 discriminative
fields that jointly identify duplicates; affiliation has one highly variable text field with no
supporting structure.

---

## 9. Output Files

| File | Description |
|---|---|
| `er_performance.csv` | Full metric table (10 methods × 13 metrics) |
| `er_predictions.csv` | Final cluster assignments (2,260 records × 2 columns) |
| `affiliation_data_profile.pdf` | Dataset profile: field types, distributions, missingness |
| `plot_method_comparison.pdf` | Grouped bar chart: ARI, B³-F, NMI per method |
| `plot_performance_heatmap.pdf` | Colour heatmap: all metrics × all methods |
| `plot_cluster_sizes.pdf` | Cluster size distribution histogram |
| `plot_precision_recall.pdf` | Precision vs Recall scatter (B-cubed and Pair-F) |
| `AFF_RESULTS_SUMMARY.md` | This document |
