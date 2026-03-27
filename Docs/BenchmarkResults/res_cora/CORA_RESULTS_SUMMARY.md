# ERBOT Results Summary — CORA Bibliographic Dataset

**Generated:** 2026-03-21
**Pipeline version:** ERBOT v0.2.0
**Run time:** ~59 minutes

---

## 1. Dataset Overview

| Property | Value |
|---|---|
| Dataset | CORA Bibliographic Bibliography |
| Records | 1,879 |
| Fields | 16 (id, title, booktitle, authors, address, date, year, editor, journal, volume, pages, publisher, institution, type, tech, note) |
| Task | Deduplication (single-source) |
| Ground truth | 64,578 matched pairs (from `cora_gold.csv`) |

CORA is a standard entity resolution benchmark consisting of research paper citations with substantial noise: missing fields, inconsistent formatting, OCR errors, and partial references. The same real paper may appear multiple times with different field values across entries.

---

## 2. Pipeline Configuration

| Stage | Setting | Value |
|---|---|---|
| Blocking | Method | Prefix blocking on `title`, length 3 |
| Blocking | Candidate pairs | 137,707 |
| Blocking | Reduction ratio | 0.922 (92.2% of all pairs eliminated) |
| Similarity | Fields computed | 15 (all non-ID fields) |
| Weights | Method | Equal (1/15 per field) |
| Clustering | Methods run | 9 |
| Merge | Strategy | Best (consensus) |

**Blocking note:** 137,707 pairs from 1,879 records (1,764,381 total possible). The prefix-3 blocking on `title` achieves 92.2% reduction while retaining candidate pairs. Pair completeness (fraction of true matches retained) was not recorded in this run due to truth format — worth measuring in future runs.

---

## 3. Performance Results

All 9 clustering methods evaluated against the gold standard:

| Method | ARI | NMI | VI | B³-P | B³-R | B³-F | Homog. | Complet. | V-meas. | PairF-P | PairF-R | PairF-F |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **svm** | **0.543** | **0.850** | **1.62** | **0.635** | 0.815 | **0.714** | **0.811** | 0.891 | **0.850** | **0.438** | 0.802 | **0.566** |
| threshold_cc | 0.506 | 0.811 | 1.92 | 0.520 | 0.858 | 0.648 | 0.733 | 0.909 | 0.811 | 0.394 | 0.818 | 0.532 |
| louvain | 0.506 | 0.811 | 1.92 | 0.520 | 0.858 | 0.648 | 0.733 | 0.909 | 0.811 | 0.394 | 0.818 | 0.532 |
| label_prop | 0.506 | 0.811 | 1.92 | 0.520 | 0.858 | 0.648 | 0.733 | 0.909 | 0.811 | 0.394 | 0.818 | 0.532 |
| leiden | 0.000 | 0.686 | 5.18 | 1.000 | 0.065 | 0.122 | 1.000 | 0.522 | 0.686 | 0.000 | 0.000 | 0.000 |
| pam | 0.161 | 0.395 | 4.74 | 0.123 | 0.743 | 0.211 | 0.274 | 0.708 | 0.395 | 0.127 | 0.779 | 0.218 |
| hclust_ward | 0.109 | 0.437 | 4.23 | 0.201 | 0.909 | 0.329 | 0.291 | 0.880 | 0.437 | 0.096 | 0.845 | 0.172 |
| hclust_avg | 0.067 | 0.276 | 4.91 | 0.083 | 0.919 | 0.153 | 0.166 | 0.820 | 0.276 | 0.073 | 0.895 | 0.135 |
| gc | 0.000 | 0.000 | 5.65 | 0.040 | 1.000 | 0.077 | 0.000 | 1.000 | 0.000 | 0.039 | 1.000 | 0.076 |
| **final** | 0.067 | 0.276 | 4.91 | 0.083 | 0.919 | 0.153 | 0.166 | 0.820 | 0.276 | 0.073 | 0.895 | 0.135 |

*Metrics: ARI = Adjusted Rand Index; NMI = Normalized Mutual Information; VI = Variation of Information (lower is better); B³ = B-cubed; V-meas. = V-measure; PairF = Pairwise F-score.*

---

## 4. Final Cluster Assignment

The `final` strategy (consensus/best merge) selected **hclust_avg** — which is the worst-performing method. This is a known bug in the merge selection logic that needs fixing.

| Cluster | Records |
|---|---|
| 1 | 932 |
| 2 | 913 |
| 3 | 11 |
| 4 | 15 |
| 5 | 8 |

- **Total clusters:** 5
- **Singletons:** 0
- **Mean cluster size:** 375.8

The final assignment is severely under-partitioned — 1,879 records collapsed into just 5 clusters, with two mega-clusters of ~900 records each. This indicates the merge strategy is merging far too aggressively.

---

## 5. Method-by-Method Interpretation

### SVM (Best — ARI=0.543)
The strongest result overall. SVM learns a decision boundary on the combined similarity score to classify pairs as match/non-match, then applies connected components. The higher precision (0.635 vs 0.520 for graph methods) indicates it is more conservative about linking records — it avoids false merges better than threshold-based methods. Best choice when a labeled training set is available.

### Threshold-CC / Louvain / Label Propagation (Identical — ARI=0.506)
All three converge to exactly the same solution, meaning the graph structure is so dominant that different community detection algorithms find the same partition. This is expected when the similarity graph has clear cluster structure at the chosen threshold (0.5). A good sign for robustness. Their higher recall (0.858) vs SVM (0.815) means they catch more true matches, but at the cost of more false merges (precision 0.520 vs 0.635).

### Leiden (Degenerate — ARI=0.000)
Perfect homogeneity (1.0) but near-zero completeness (0.065) and zero ARI. Leiden over-partitioned — it split each true entity into many tiny sub-clusters. This typically happens when the resolution parameter is too high or when the graph is sparse and Leiden finds many small communities. Needs resolution parameter tuning.

### GC — Graph Coloring (Degenerate — ARI=0.000)
Perfect recall and completeness (1.0) but near-zero precision. GC merged everything into effectively one or two giant clusters. Graph coloring requires careful threshold tuning; at the default 0.5 threshold it collapsed too aggressively.

### PAM (ARI=0.161)
Partitioning Around Medoids requires specifying k (number of clusters). Without ground-truth tuning of k, performance is limited. The relatively balanced precision/recall (0.123/0.743) suggests it finds some structure but not at the right granularity.

### Hierarchical (hclust_ward ARI=0.109, hclust_avg ARI=0.067)
Both perform poorly — hierarchical clustering on sparse similarity matrices tends to chain (average-link) or over-merge (ward). High recall but very low precision indicates they merge too many records. Not recommended for large bibliographic ER tasks.

---

## 6. Key Findings

1. **Best method is SVM** (ARI=0.543). For CORA, a supervised approach that learns the match/non-match boundary outperforms unsupervised graph methods by ~0.04 ARI points.

2. **Graph-based methods are consistent.** Threshold-CC, Louvain, and Label Propagation all agree — this is a positive signal that the similarity graph has stable structure and results are not method-dependent.

3. **Equal weights limit performance.** All 15 fields contributed equally (weight=0.067 each). Fields like `title` and `authors` are far more discriminative than `note` or `tech`. Supervised weight learning (ARI-based) would likely push SVM and graph methods above 0.6 ARI.

4. **Final merge logic is broken.** The `best` merge strategy selected `hclust_avg` (worst performer) instead of `svm` or `threshold_cc`. The consensus/best selection criteria need fixing — this is the top priority bug.

5. **Leiden and GC need tuning.** Both degenerated at default parameters. Leiden needs a lower resolution parameter; GC needs threshold calibration.

6. **Blocking is effective.** 92.2% reduction ratio with prefix-3 on title is good for a bibliographic dataset where title prefixes are usually consistent across duplicates.

---

## 7. Recommendations for Next Steps

| Priority | Action |
|---|---|
| 🔴 High | Fix `final` merge/best-selection logic — it should pick the method with highest ARI or B³-F, not fall back to hclust_avg |
| 🔴 High | Switch from equal weights to supervised ARI-based weights — likely to gain +0.05–0.10 ARI |
| 🟡 Medium | Tune Leiden resolution parameter (try 0.1–0.5) to prevent over-splitting |
| 🟡 Medium | Tune GC threshold (try 0.6–0.8) to prevent over-merging |
| 🟡 Medium | Add pair completeness to blocking stats (requires truth in id/cluster format at Stage 3) |
| 🟢 Low | Try SN blocking (window=40) instead of prefix-3 — may improve recall at cost of more pairs |
| 🟢 Low | Run with field-specific similarity methods (jaro-winkler for names, exact for year) rather than auto-detected |

---

## 8. Output Files

| File | Description |
|---|---|
| `er_performance.csv` | Full metric table (10 methods × 13 metrics) |
| `er_predictions.csv` | Final cluster assignments (1,879 records × 2 columns) |
| `cora_data_profile.pdf` | Dataset profile: field types, distributions, missingness |
| `plot_method_comparison.pdf` | Grouped bar chart: ARI, B³-F, NMI per method |
| `plot_performance_heatmap.pdf` | Color heatmap: all metrics × all methods |
| `plot_cluster_sizes.pdf` | Cluster size distribution histogram |
| `plot_precision_recall.pdf` | Precision vs Recall scatter (B-cubed and Pair-F) |
| `CORA_RESULTS_SUMMARY.md` | This document |
