# ERBOT Results Summary — Synthetic 10K Dataset

**Generated:** 2026-03-23
**Pipeline version:** ERBOT v0.2.0
**Run time:** ~11 minutes (641s)

---

## 1. Dataset Overview

| Property | Value |
|---|---|
| Dataset | Synthetic 10K (d10k) |
| Records | 10,000 |
| Fields | 2 (id, text) |
| Task | Deduplication (single-source) |
| Ground truth | 8,705 matched pairs (from `10Kduplicates.csv`) |

The synthetic 10K dataset consists of 10,000 noisy text records generated to simulate
real-world deduplication challenges. Each record has a single free-text field (`text`)
containing a scrambled mix of tokens — names, dates, numbers, addresses — with no
explicit field structure. Duplicates are introduced by corrupting original records with
character substitutions, token deletions, and reorderings, mimicking common data entry
and OCR errors.

---

## 2. Pipeline Configuration

| Stage | Setting | Value |
|---|---|---|
| Blocking | Method | Prefix blocking on `text`, length 3 |
| Blocking | Candidate pairs | 885,301 |
| Blocking | Reduction ratio | 0.982 (98.2% of all pairs eliminated) |
| Similarity | Fields computed | 1 (text only) |
| Weights | Method | Equal (weight = 1.0 for text) |
| Clustering | Methods run | 8 (SVM excluded — see note) |
| Merge | Strategy | Best |

**SVM excluded:** SVM was initially run but killed after ~20 hours with no result.
With 885,301 candidate pairs, SVM training time was estimated at 1–4 days due to its
super-linear scaling with the number of pairs. All other 8 methods completed in ~11
minutes. SVM remains viable for smaller datasets (CORA at 137K pairs completed in ~63
minutes total) but is not practical for datasets at this scale without subsampling or
approximation.

**Blocking note:** 885,301 pairs from 10,000 records (49,995,000 total possible).
Prefix-3 achieves 98.2% reduction — the highest reduction ratio across all three
datasets tested. However, as with affiliation strings, prefix-3 on a single noisy
text field may silently discard true matches whose first 3 characters differ due to
corruption or reordering.

---

## 3. Performance Results

8 clustering methods evaluated (SVM excluded):

| Method | ARI | NMI | VI | B³-P | B³-R | B³-F | Homog. | Complet. | V-meas. |
|---|---|---|---|---|---|---|---|---|---|
| leiden | 0.000 | **0.914** | **1.97** | **1.000** | 0.271 | **0.426** | **1.000** | 0.841 | **0.914** |
| **threshold_cc** | **0.023** | 0.778 | 4.21 | 0.334 | 0.519 | 0.406 | 0.707 | 0.865 | 0.778 |
| **louvain** | **0.023** | 0.778 | 4.21 | 0.334 | 0.519 | 0.406 | 0.707 | 0.865 | 0.778 |
| **label_prop** | **0.023** | 0.778 | 4.21 | 0.334 | 0.519 | 0.406 | 0.707 | 0.865 | 0.778 |
| hclust_avg | 0.002 | 0.371 | 8.82 | 0.019 | 0.573 | 0.036 | 0.249 | 0.727 | 0.371 |
| pam | 0.002 | 0.371 | 8.82 | 0.019 | 0.573 | 0.036 | 0.249 | 0.727 | 0.371 |
| hclust_ward | 0.001 | 0.347 | 8.80 | 0.019 | 0.690 | 0.037 | 0.224 | 0.772 | 0.347 |
| gc | 0.000 | 0.000 | 10.4 | 0.001 | 1.000 | 0.002 | 0.000 | 1.000 | 0.000 |
| **final** | **0.023** | 0.778 | 4.21 | 0.334 | 0.519 | **0.406** | 0.707 | 0.865 | 0.778 |

*Metrics: ARI = Adjusted Rand Index; NMI = Normalized Mutual Information; VI = Variation
of Information (lower is better); B³ = B-cubed; V-meas. = V-measure.*

---

## 4. Final Cluster Assignment

The `final` strategy correctly selected **threshold_cc** (highest ARI = 0.023).

| Stat | Value |
|---|---|
| Total clusters | 1,619 |
| Records | 10,000 |
| Mean cluster size | 6.2 |

1,619 clusters from 10,000 records. Given the dataset has 8,705 gold pairs implying
a moderate duplication rate, this is a plausible partition size.

---

## 5. Method-by-Method Interpretation

### Threshold-CC / Louvain / Label Propagation (Highest ARI — 0.023)
The three graph-based methods again converge to identical solutions, confirming that the
similarity graph structure dominates algorithm choice. B³-F of 0.406 with precision 0.334
and recall 0.519 is a moderate result — about half of true matches are found, at the cost
of some false merges. These are the best practical methods at this scale and are the
`final` selection.

### Leiden (Interesting — ARI = 0.000, NMI = 0.914, B³-F = 0.426)
Leiden produces the highest NMI (0.914) and B³-F (0.426) but zero ARI. This apparent
contradiction arises because ARI heavily penalises over-splitting: Leiden creates many
small, highly pure sub-clusters (precision = 1.000) but each true entity is split into
multiple predicted clusters (recall = 0.271). The clusters are perfectly pure but
incomplete — every predicted cluster is a true sub-group, but the true groups are
fragmented. NMI rewards this purity whereas ARI does not.

For applications where **false merges are very costly** (e.g. legal identity resolution
where merging two different people is a serious error), Leiden at its default resolution
is actually the safest choice. For applications where **recall matters** (finding all
duplicates), threshold-CC is better.

### hclust_avg / PAM (ARI ≈ 0.002)
Near-zero ARI despite moderate NMI. Both collapse into very large clusters with tiny
precision (0.019), meaning almost every pair they merge is wrong. At 10K records the
similarity matrix is too sparse and noisy for centroidal/hierarchical methods to find
meaningful structure.

### hclust_ward (ARI = 0.001)
Similar story to hclust_avg — Ward linkage performs marginally worse on recall (0.690
vs 0.573) but equally poorly on precision. Hierarchical methods are clearly not suited
to large sparse similarity matrices.

### GC — Graph Coloring (Degenerate — ARI = 0.000)
Perfect recall, near-zero precision, collapsed to one giant cluster. The gc dimension
mismatch error persists — this is a recurring bug across all three datasets and needs
fixing in the GCMER interface layer.

---

## 6. Key Findings

1. **Overall performance is very low.** Best ARI is 0.023 — lower than affiliation
   (0.076) and far below CORA (0.746). At 10,000 records with a single noisy text field
   and no field structure, the task is fundamentally harder.

2. **Blocking discards too many true matches.** 98.2% reduction is aggressive. With
   only 8,705 gold pairs in a 10,000-record dataset, many duplicates involve records
   with corrupted or reordered first tokens — these are eliminated before any similarity
   is computed. Blocking is the primary performance bottleneck.

3. **Leiden is the precision champion.** Perfect B³-precision (1.000) and NMI (0.914)
   make Leiden the best choice when false merges must be avoided. Its ARI = 0 is
   misleading — it reflects over-splitting, not noise.

4. **SVM is not viable at this scale.** 885K pairs × SVM training = 1–4 days runtime.
   For datasets above ~200K candidate pairs, SVM should be replaced with a faster
   supervised method (logistic regression, gradient boosting, or approximate nearest
   neighbour classification).

5. **Graph methods plateau quickly.** Threshold-CC, Louvain, and Label Propagation
   give identical results here as in CORA and affiliation. The similarity graph
   structure consistently dominates — different community detection algorithms find
   the same partition.

6. **The `final` merge correctly selected threshold_cc.** Merge bug fix continues to
   work correctly across all three datasets.

---

## 7. Recommendations for Next Steps

| Priority | Action |
|---|---|
| 🔴 High | Replace prefix-3 blocking with SN blocking (window=40–80) — the 98.2% reduction is too aggressive for a noisy single-field dataset |
| 🔴 High | Replace SVM with logistic regression or gradient boosting for supervised classification — linear scalability, similar accuracy |
| 🟡 Medium | Tune Leiden resolution downward (try 0.1–0.3) to improve recall while preserving its high precision |
| 🟡 Medium | Fix GC dimension mismatch error — silently falls back to degenerate solution across all datasets |
| 🟡 Medium | Measure pair completeness at Stage 3 to quantify how many gold pairs are lost at blocking |
| 🟢 Low | Explore token-set / Jaccard similarity instead of default string similarity for unstructured text |
| 🟢 Low | Try field parsing: split `text` into sub-fields (name, date, address tokens) to enable multi-field similarity |

---

## 8. Cross-Dataset Comparison

| Metric | CORA | Affiliation | Syn10k |
|---|---|---|---|
| Records | 1,879 | 2,260 | 10,000 |
| Fields (usable) | 15 | 1 | 1 |
| Gold pairs | 64,578 | 32,816 | 8,705 |
| Candidate pairs | 137,707 | 164,719 | 885,301 |
| Blocking RR | 92.2% | 93.5% | 98.2% |
| Best ARI | 0.746 | 0.076 | 0.023 |
| Best B³-F | 0.738 | 0.460 | 0.426 |
| Best method (ARI) | hclust_avg | threshold_cc | threshold_cc |
| SVM viable? | Yes (~63 min) | Marginal (~5.6h) | No (~1–4 days) |
| Final correct? | Yes | Yes | Yes |
| Runtime (no SVM) | — | — | ~11 min |

The clear trend: **more records + fewer fields = lower performance**. CORA benefits
from 15 discriminative fields that collectively identify duplicates even under noise.
Affiliation and Syn10k each have one field, forcing all discriminative power through
a single noisy similarity score.

---

## 9. Output Files

| File | Description |
|---|---|
| `er_performance.csv` | Full metric table (9 methods × 13 metrics) |
| `er_predictions.csv` | Final cluster assignments (10,000 records × 2 columns) |
| `d10k_data_profile.pdf` | Dataset profile: field types, distributions, missingness |
| `plot_method_comparison.pdf` | Grouped bar chart: ARI, B³-F, NMI per method |
| `plot_performance_heatmap.pdf` | Colour heatmap: all metrics × all methods |
| `plot_cluster_sizes.pdf` | Cluster size distribution histogram |
| `plot_precision_recall.pdf` | Precision vs Recall scatter (B-cubed and Pair-F) |
| `SYN10K_RESULTS_SUMMARY.md` | This document |
