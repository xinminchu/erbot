# ERBOT v2 Technical Reference Guide

**Entity Resolution Bundle for Optimal Tuning**
Author: Xinmin Chu | Advisor: Prof. David Degras-Valabregue
Last updated: 2026-03-20 | Package version: 0.2.0

---

## Table of Contents

1. [What Is Entity Resolution?](#1-what-is-entity-resolution)
2. [The ERBOT Pipeline at a Glance](#2-the-erbot-pipeline-at-a-glance)
3. [Foundational Theory](#3-foundational-theory)
   - 3.1 Fellegi-Sunter Model
   - 3.2 Missing Data Mechanisms (MCAR / MAR / MNAR)
   - 3.3 Evaluation Splits and the Labeling Problem
   - 3.4 Missingness Patterns and Their Effect on the Pipeline
4. [Stage 1 – Data Loading](#4-stage-1--data-loading)
5. [Stage 2 – Diagnosis](#5-stage-2--diagnosis)
6. [Stage 3 – Blocking](#6-stage-3--blocking)
7. [Stage 4 – Similarity Computation](#7-stage-4--similarity-computation)
8. [Stage 5 – Weight Learning](#8-stage-5--weight-learning)
9. [Stage 6 – Field Combination: Two Approaches](#9-stage-6--field-combination-two-approaches)
10. [Stage 7 – Clustering](#10-stage-7--clustering)
    - 10.1 Centroidal Methods (HC, PAM)
    - 10.2 Graph-Community Methods (Louvain, Leiden, Label Propagation, Threshold-CC)
    - 10.3 Graph Coloring (GCMER)
    - 10.4 Supervised Pairwise Classifiers (SVM, GBM)
    - 10.5 k-Tuning Strategies
11. [Stage 8 – Post-Processing and Merging](#11-stage-8--post-processing-and-merging)
12. [Stage 9 – Evaluation](#12-stage-9--evaluation)
13. [Neural Entity Resolution Module](#13-neural-entity-resolution-module)
14. [Dataset Profile Report](#14-dataset-profile-report)
15. [Data Structures](#15-data-structures)
16. [Complete API Reference](#16-complete-api-reference)
17. [Worked Examples](#17-worked-examples)
18. [Common Questions and Misconceptions](#18-common-questions-and-misconceptions)
19. [Glossary](#19-glossary)

---

## 1. What Is Entity Resolution?

**Entity resolution (ER)** — also called *record linkage*, *deduplication*, or *entity matching* — is the problem of deciding which records in one or more databases refer to the same real-world entity.

**Example**: These two records likely refer to the same person, even though no field matches exactly:

| Field | Record A        | Record B  |
|-------|-----------------|-----------|
| name  | Alice M. Smith  | A. Smith  |
| city  | New York, NY    | N.Y.      |
| year  | 1985            | NA        |

ER arises everywhere:
- **Healthcare**: Merging patient records across hospitals (different spellings, missing DOB)
- **Bibliography**: Deduplicating citation databases (CORA, DBLP)
- **Government**: Voter registration cleansing (NC Voter dataset)
- **Business**: Product catalog matching across retailers

### Deduplication vs. Record Linkage

| Mode               | Datasets | Question                                  |
|--------------------|----------|-------------------------------------------|
| **Deduplication**  | One      | Which records are the same entity?        |
| **Record linkage** | Two+     | Which record in A matches which in B?     |

ERBOT handles both. Mode is auto-detected from the data (presence of a `source_id` column).

### The Three Classic Stages of ER

1. **Blocking**: Reduce the O(n²) comparison space to a manageable candidate set.
2. **Field comparison / Similarity**: For each candidate pair, compute a pairwise similarity score per field, producing a feature vector per pair.
3. **Classification / Clustering**: Group matched records into entity clusters.

ERBOT extends this with **weight learning** (which fields matter most?) and **adaptive combination** (how to aggregate similarity scores when some fields are missing?).

---

## 2. The ERBOT Pipeline at a Glance

```
er_run(data, ...)
   │
   ▼
Stage 1: er_load()            ─── Read CSV / data.frame / benchmark keyword
   │
   ▼
Stage 2: er_diagnose()        ─── Field types, missingness, blocking recommendations
   │                               → er_data_profile() for visual PDF report
   ▼
Stage 3: er_block()           ─── Candidate pair generation
                                   (none / standard / prefix / sorted-neighborhood)
   │
   ▼
Stage 4: er_similarity()      ─── Per-field similarities → named list of NA-aware vectors
                                   (jw / lv / jaccard / bow / categorical / numeric / year)
   │
   ▼
Stage 5: er_weights()         ─── Field weight learning
                                   (auto / equal / variance / bimodal / fellegi_sunter / ari)
   │
   ▼
Stage 6a: er_combine()        ─── Approach 1: NA-aware adaptive weighted average → single S
       OR
Stage 6b: er_field_ensemble() ─── Approach 2: cluster each field separately → consensus labels
   │
   ▼  (Approach 1 only continues to Stage 7; Approach 2 skips to Stage 8)
Stage 7: er_cluster_all()     ─── Clustering on sparse similarity matrix S
                                   (hclust_avg / hclust_ward / pam /
                                    threshold_cc / louvain / leiden / label_prop /
                                    gc / svm / gbm)
   │
   ▼
Stage 8: er_merge()           ─── Post-processing
                                   (transitivity / consensus / best / none)
   │
   ▼
Stage 9: er_evaluate()        ─── ARI, NMI, VI, B³ F, V-measure, Pairwise F, silhouette
```

**Key design principle**: Each stage is independently callable. The master function `er_run()` calls them in sequence. You can also run just `er_block()` + `er_similarity()` without clustering.

---

## 3. Foundational Theory

### 3.1 Fellegi-Sunter Model

**Reference**: Fellegi and Sunter (1969), "A theory for record linkage," *JASA* 64(328).

The classical probabilistic model for record linkage. For a candidate pair (A, B), define the **comparison vector** γ = (γ₁, ..., γ_K) where each γ_k encodes field-k agreement. Modern implementations use continuous similarities in [0, 1].

Define:
- **M** = event that (A, B) are a true match
- **U** = event that (A, B) are a non-match

The **likelihood ratio** is:

```
R(A,B) = P(γ | M) / P(γ | U)
```

Under independence across fields:

```
R(A,B) = ∏_k  m_k(γ_k) / u_k(γ_k)
```

where m_k = P(γ_k | match) and u_k = P(γ_k | non-match).

The **log likelihood ratio** (log Bayes factor) is:

```
log R = Σ_k  log[ m_k(γ_k) / u_k(γ_k) ]
```

**Decision rule**:
- R > T_upper → declare match
- R < T_lower → declare non-match
- Otherwise → uncertain (manual review)

**What ERBOT uses**: `er_weights(method = "fellegi_sunter")` estimates m_k and u_k via a 2-component Beta EM mixture, then sets w_k ∝ log(m_k / u_k). High discriminative power (m_k >> u_k) → high weight.

### 3.2 Missing Data Mechanisms (MCAR / MAR / MNAR)

**Reference**: Rubin (1976), *Biometrika*; Little & Rubin (2002).

#### MCAR – Missing Completely At Random

Missingness is independent of all data (observed or not). Listwise deletion gives unbiased estimates. Testable by chi-squared test between the missingness indicator and all other variables.

#### MAR – Missing At Random

Missingness depends on *observed* data only. Valid imputation is possible using observed covariates.

**Example**: Records from Source B never have a `venue` field, but source is observed.

#### MNAR – Missing Not At Random

Missingness depends on the *missing value itself*. No standard imputation fully corrects for MNAR bias.

**Example**: Very old papers are more likely to be missing `year` precisely because old records are harder to find.

**Why MNAR matters in ER**: Missingness is often *entity-correlated* — all records from county X are missing middle name, not randomly. This creates problems for imputation-based pipelines.

**ERBOT's response**: NA propagates through similarity computation and is handled at Stage 6 by ignoring missing fields per pair. No imputation.

### 3.3 Evaluation Splits and the Labeling Problem

Evaluating an ER system requires ground-truth entity labels — knowing which records actually refer to the same entity. In practice, such labels are scarce and expensive to obtain, because producing them requires manual expert review of record pairs. This is the core challenge of ER evaluation. Widely used benchmarks such as CORA and NC Voter Registration data provide pre-labeled ground truth, which is why they are used repeatedly across the ER literature.

**Record-disjoint splits**: The standard practice divides records randomly into training and test sets. Both sets contain records from the *same* entities, so the model can implicitly learn entity-specific patterns (a specific author's naming conventions, a company's address format) rather than generalizable matching rules. Evaluation results can be overly optimistic by 5–15% ARI as a result.

**Entity-disjoint splits**: The conceptually cleaner approach is to split *entities* rather than records — all records from test entities are withheld from training entirely, mimicking a deployment scenario where the model encounters previously unseen entities. However, this approach requires pre-assigned entity identifiers to define the split — which is precisely what ER is trying to discover in the first place. To evaluate ER in an entity-disjoint manner, you need to already know the answer. This circular dependency makes entity-disjoint splits impractical in most real ER settings, and it is not the standard in production pipelines.

**Practical implication for ERBOT**: Most of ERBOT's core pipeline (Stages 1–9) is unsupervised — it requires no labeled training data. Ground-truth labels, when available, are used only as an external post-hoc quality check via `er_evaluate()`, not as input to the algorithm. The **both-null artifact** (sim("","") = 1 for two missing values) is a validity concern regardless of split strategy: ERBOT returns NA for missing field similarities so that `er_combine()` ignores those fields entirely rather than injecting false match signal.

### 3.4 Missingness Patterns and Their Effect on the Pipeline

The `missingness` column in `er_diagnose()` — fraction of records with NA or empty string per field — is the primary actionable signal about data quality. It directly controls three pipeline decisions:

**1. Blocking candidate selection (Stage 3)**

A field is excluded as a blocking key if its miss rate exceeds 0.3 (30%). A blocking key with high missingness silently drops records with no key value into an uncontrolled catch-all block, destroying RR/PC balance. Fields passing the missingness threshold are flagged `blocking_candidate = TRUE`.

**2. Per-pair adaptive combination (Stage 6)**

`er_combine()` drops any field that is NA for a specific pair and re-normalizes weights over only the observed fields for that pair. The `missingness` rate of a field predicts how often it will be absent from the weighted average — a field missing 60% of the time contributes to fewer than half of all pair scores.

**3. Systematic vs. random missingness**

The danger is not the overall miss rate, but its *pattern*:

| Pattern | Example | Risk |
|---------|---------|------|
| **Random** (scattered NAs) | 5% of all records missing `year` | Low — affects pairs independently |
| **Systematic** (source-level) | All records from Source B lack `venue` | Higher — all pairs within that source trigger the both-null artifact if not handled |

Systematic missingness is common in multi-source ER (MAR: missingness depends on source, which is observed). ERBOT handles both patterns correctly by returning NA at the similarity level and absorbing it in `er_combine()` rather than imputing.

---

## 4. Stage 1 – Data Loading

### `er_load(input, ...)`

Accepts:
- A `data.frame` or `data.table` directly
- A file path (CSV, TSV, pipe-delimited `|`, semicolon-delimited, Excel `.xlsx`/`.xls`)
- A benchmark keyword: `"cora"`, `"affiliation"`, `"d10k"`

Delimiter is auto-detected by `er_read_pipe_or_fix()`, which tries `|`, `\t`, `;`, `,` in that order based on column count.

`er_load_input()` is an alias for `er_load()` (backward compatibility).

### `ncvr_read(path, n_records)`

Reads NC Voter Registration data (pipe-delimited, 67 fields). Field auto-detection via `ncvr_guess_fields()`.

**NCVoters dataset specifics**:
- Full file: ~9 million records; ERBOT uses samples of 5k, 10k, or larger
- Typical sample strategy: draw `n_records` rows, stratified to keep entity groups intact
- 10k sample contains ~10,000 unique entities with ~1.4–3 records each
- Ground truth: entity ID column
- High missingness in middle name (~40%), moderate in street address (~15%)
- Recommended blocking: sorted neighborhood on normalized last name, window = 3

---

## 5. Stage 2 – Diagnosis

### `er_diagnose(d, text_cols, entity_col, ...)`

Analyzes every column and returns a `fields` tibble:

| Column              | Meaning                                           |
|---------------------|---------------------------------------------------|
| `name`              | Column name                                       |
| `type`              | Detected type (year / numeric / categorical / text_short / text_long) |
| `missingness`       | Fraction of records with NA or ""                 |
| `cardinality_ratio` | Unique values / total records                     |
| `avg_tokens`        | Mean number of whitespace-separated tokens        |
| `sim_method`        | Recommended similarity type                       |
| `blocking_candidate`| Whether this field is suitable for blocking       |

Also returns: `id_col`, `source_col`, `mode` (dedup/link), `n`, `estimated_pairs`, `blocking_needed`, `recommended_block_method`, `recommended_block_key`.

**Field type detection rules**:

| Detected Type | Condition                                               | Default Similarity |
|---------------|---------------------------------------------------------|--------------------|
| `year`        | Column name matches year regex AND values are 4-digit integers | `year`      |
| `numeric`     | All non-NA values numeric, cardinality > 20             | `numeric`          |
| `categorical` | Non-numeric, cardinality_ratio < 0.05                  | `categorical`      |
| `text_short`  | avg_tokens ≤ 3                                          | `jw`               |
| `text_long`   | avg_tokens > 3                                          | `jaccard`          |

**Blocking candidate criterion**: miss_rate < 0.3 AND cardinality_ratio > 0.01 AND type ∈ {text_short, categorical, year}.

---

## 6. Stage 3 – Blocking

**Problem**: With n records, full pairwise comparison requires n(n-1)/2 pairs. For n = 10,000 that is ~50 million pairs. Blocking restricts comparison to likely matches only.

### Quality Metrics

**Reduction Ratio (RR)**:
```
RR = 1 - |candidate pairs| / C(n, 2)
```
Higher is better. Good blocking achieves RR > 0.99.

**Pair Completeness (PC)**:
```
PC = |true matches in candidate set| / |all true matches|
```
Should be close to 1.0 (blocking recall). High RR and high PC simultaneously is the goal.

### Method 1: `none`

All C(n,2) pairs. Feasible only for n ≤ 5,000 (~12.5 million pairs max). Used for CORA (1,879 records).

### Method 2: `standard`

Exact match on a blocking key (e.g., first 5 characters of last name). Records sharing the same key are compared.

Records with a missing blocking key are placed into a single "missing" block and compared among themselves — preventing silent dropout.

### Method 3: `prefix`

Like `standard` but applies NFKC Unicode normalization + lowercase before taking the first `p` characters.

**Why NFKC?** Unicode has multiple byte-level representations of the same character. NFKC (Normalization Form KC) decomposes and recomposes canonically so "Müller" and "Mu\u0308ller" become identical before prefix extraction.

**Prefix length**: Configurable via `key_len` (default 5). Shorter prefix → larger blocks (higher PC, lower RR). Longer prefix → smaller blocks (higher RR, lower PC).

**How the prefix is obtained**: `substr(stringi::stri_trans_nfkc(tolower(trimws(key))), 1, key_len)`.

### Method 4: `sn` (Sorted Neighborhood)

**Algorithm**:
1. Sort all records by a normalized blocking key
2. Slide a window of width w over the sorted list
3. All pairs within the same window are candidate pairs

**Complexity**: O(n × w) pairs. With w = 3 and n = 10,000 → ~30,000 pairs vs 50 million full.

**Why it works**: Records for the same entity have similar keys and therefore sort near each other. Near-misses like "Smith" vs "Smyth" appear close in alphabetical order.

**Window size w**: Default 3. Larger w → better recall but more pairs. Typical range: 3–10.

**NA handling**: Records with missing blocking key are placed at the end of the sorted list and compared within a separate window.

**What `_dt` means**: The internal implementation uses `data.table` (hence the `_dt` suffix in internal helpers) for efficient in-place sorting and index-based pair enumeration. `data.table` sorts n = 100,000 rows in milliseconds using radix sort, versus seconds for base R `order()` on large character vectors.

### Method 5: `auto`

Selects one of the four strategies above using the recommendation from `er_diagnose()`.

### `er_block_stats(pairs, truth, n)`

Computes RR and PC given candidate pairs, ground-truth labels, and total n.

---

## 7. Stage 4 – Similarity Computation

### `er_similarity(d, pairs, spec, diag)`

For each candidate pair (i, j) in `pairs`, computes a per-field similarity score.

**Critical design**: Returns **NA** when either record is missing a field value.

| What to return | Effect |
|----------------|--------|
| sim = 1 (both-null default) | False "definite match" signal — WRONG |
| sim = 0 | False "definite non-match" — also wrong |
| sim = 0.5 | Arbitrary neutral; still injects fake data |
| **NA** | Truthful: "no information." Handle explicitly downstream. |

**Returns**: Named list with one numeric vector per field, each of length = number of candidate pairs. Values in [0,1] or NA.

### Similarity Type 1: `"jw"` – Jaro-Winkler

**Best for**: Names, short free-text (avg tokens ≤ 3)

Jaro similarity:
```
jaro(s, t) = (1/3)(m/|s| + m/|t| + (m - t_half)/m)   if m > 0, else 0
```
where m = matching characters (within window ⌊max(|s|,|t|)/2⌋ - 1), t_half = transpositions/2.

Jaro-Winkler adds a prefix bonus:
```
jw(s, t) = jaro(s,t) + p × ℓ × (1 - jaro(s,t))
```
where ℓ = common prefix length (≤ 4), p = 0.1.

**Examples**:
- jw("Alice Smith", "Alice Smyth") ≈ 0.967
- jw("Bob Jones", "Robert J.") ≈ 0.816

**Implementation**: `stringdist::stringsim(method = "jw", p = 0.1)`

### Similarity Type 2: `"lv"` – Normalized Levenshtein

**Best for**: Strings with insertions/deletions/substitutions

```
sim_lv(s, t) = 1 - lev_distance(s, t) / max(|s|, |t|)
```

Levenshtein distance = minimum edit operations (insert, delete, substitute) to transform s → t.

**Example**: lev("kitten", "sitting") = 3 → sim_lv ≈ 0.571

### Similarity Type 3: `"jaccard"` – Token Jaccard

**Best for**: Medium-length text where word presence matters more than order

```
jaccard(s, t) = |tokens(s) ∩ tokens(t)| / |tokens(s) ∪ tokens(t)|
```

**Example**:
- s = "University of Massachusetts Boston" → {university, of, massachusetts, boston}
- t = "Univ Massachusetts Boston" → {univ, massachusetts, boston}
- jaccard = 2/5 = 0.40

### Similarity Type 4: `"bow"` – IDF-Weighted Bag-of-Words Cosine Similarity

**Best for**: Long free-text fields (abstracts, full addresses) where common words should be down-weighted

**Why BoW over Jaccard for long text?** Jaccard treats all words equally. "the", "a", "of" appear in every document and contribute nothing. IDF down-weights common words, so rare discriminative words (like "xylophone" or a specific place name) drive the similarity.

**Two-step process**:

**Step 1** — Build IDF weights once from the entire column (`.bow_idf(corpus)`):

```
IDF(t) = log( (N + 1) / (df(t) + 1) ) + 1
```

where:
- N = total number of non-NA values in the column
- df(t) = number of values containing term t
- The +1 smoothing prevents division by zero for terms appearing in every document

**Step 2** — For each pair, compute weighted cosine similarity (`.sim_bow(a, b, idf)`):

1. Tokenize a and b (lowercase, remove punctuation, split on whitespace)
2. Compute TF vectors: tf_a(t) = count of term t in a, tf_b(t) similarly
3. Weight by IDF: w_a(t) = tf_a(t) × IDF(t), similarly w_b
4. Cosine similarity: (w_a · w_b) / (||w_a|| × ||w_b||)

**Efficiency**: IDF is computed once per column (O(n)), not per pair (O(pairs²)). For unknown terms at query time, IDF defaults to log(2) ≈ 0.693.

### Similarity Type 5: `"categorical"` – Exact Match

**Best for**: Standardized codes, state abbreviations, gender, country codes

```
sim_cat(s, t) = 1   if norm_str(s) == norm_str(t)
              = 0   otherwise
              = NA  if either is missing
```

No partial credit — either they match or they don't.

### Similarity Type 6: `"numeric"` – Exponential Decay

**Best for**: Continuous numeric fields where closeness matters

```
sim_num(a, b) = exp( -|a - b| / (tau × range) )
```

where `range` = max(col) - min(col), `tau` = scale parameter (default 1.0).

At distance = 0 → sim = 1. At distance = tau × range → sim = exp(-1) ≈ 0.37.

### Similarity Type 7: `"year"` – Tolerance Window

**Best for**: Publication year, birth year (off-by-one errors are common)

```
sim_year(a, b) = 1   if |a - b| ≤ year_tol
               = 0   otherwise
               = NA  if either is missing
```

Default tolerance = 1 year (a paper published December 2019 may be cited as 2020).

### `er_pairs_to_sparse(pairs, sim_vec, n, na_fill = 0)`

Converts a combined similarity vector into the symmetric sparse n×n matrix S (dgCMatrix) needed by clustering. NA values replaced by `na_fill` (default 0). Diagonal set to 1.

**Memory**: For n = 10,000 with 200,000 pairs: ~5 MB sparse vs ~800 MB dense.

---

## 8. Stage 5 – Weight Learning

### `er_weights(sim_list, method, pairs, truth, id_vec)`

Assigns weight w_k to each field k, where Σ_k w_k = 1. These weights reflect each field's *relative discriminative power*, not its absolute importance.

### Method: `"equal"`

w_k = 1/K for all k. Baseline — use when no information distinguishes field quality.

### Method: `"variance"`

```
w_k ∝ Var(S_k)
```

Fields with higher variance across pairs are more discriminative. A field where all pairs score 0.5 carries no information; one where scores span 0.1 to 0.9 is highly informative.

**Limitation**: Variance is high even when the distribution is concentrated near 1 (e.g., most pairs are very similar on this field — not because they match but because the field has little variance in content).

### Method: `"bimodal"`

**Sarle's bimodality coefficient (BC)**:
```
BC = (γ₁² + 1) / (γ₂ + 3(n-1)²/((n-2)(n-3)))
```

where γ₁ = skewness, γ₂ = excess kurtosis.

Weight formula: `w_k ∝ max(0, BC_k - 0.5)`

**Why bimodality?** A perfect discriminating field has a bimodal similarity distribution: one peak near 1 (true match pairs) and one near 0 (non-match pairs). BC > 0.555 indicates bimodality.

### Method: `"fellegi_sunter"`

Fits a 2-component Beta mixture to each field's similarity distribution:

```
S_k ~ π_m × Beta(α_m, β_m)  +  (1 - π_m) × Beta(α_u, β_u)
```

**ERBOT parametrization**: Beta(α, β) via mean μ: α = max(0.1, μ×9), β = max(0.1, (1-μ)×9).

**EM algorithm** (5 iterations):

*Initialization*: Split at median; m_k = mean upper half; u_k = mean lower half; π_m = 0.1.

*E-step*:
```
r_m(i) = π_m × f_Beta(S_k(i); m_k) / [π_m × f_Beta(S_k(i); m_k) + (1-π_m) × f_Beta(S_k(i); u_k)]
```

*M-step*:
```
π_m ← mean(r_m)
m_k ← Σ r_m(i) × S_k(i) / Σ r_m(i)
u_k ← Σ (1 - r_m(i)) × S_k(i) / Σ (1 - r_m(i))
```

*Weight*:
```
w_k = max(0, log(m_k / u_k))
```

**Interpretation**: log(m_k / u_k) is the log Bayes factor. If m_k = 0.9 and u_k = 0.1: w_k = log(9) ≈ 2.2. If m_k ≈ u_k (field not discriminative): w_k ≈ 0.

**Why 5 EM iterations?** The 2-component Beta model converges quickly from a reasonable initialization. Five iterations is efficient for large datasets while giving results indistinguishable from full convergence.

**Why not ridge or lasso?** Ridge and lasso are regularization methods for regression — they minimize a loss over a labeled dataset. `er_weights()` is unsupervised (no labels needed for fellegi_sunter, variance, bimodal). When labels are available, the `"ari"` method directly measures each field's clustering quality. Ridge would add unnecessary complexity without a labeled loss to optimize.

### Method: `"ari"` (Supervised)

**Requires ground truth**. For each field k:
1. Threshold S_k at the median → build graph → find connected components → cluster labels C_k
2. Compute ARI(C_k, truth)
3. Weight: w_k ∝ ARI_k

Fields whose individual clustering aligns well with true entities get higher weight. Falls back to variance if `aricode` is unavailable.

### Method: `"auto"`

Uses `"ari"` if truth is provided, `"fellegi_sunter"` otherwise. Default and recommended.

---

## 9. Stage 6 – Field Combination: Two Approaches

When a dataset has multiple fields, there are two general strategies:

**Approach 1** (default): Compute per-field similarities → combine into one score S(i,j) → cluster once.

**Approach 2** (available): Cluster each field's similarity matrix independently → merge label vectors via consensus.

### Approach 1: `er_combine(sim_list, weights, na_fill)`

The **NA-aware adaptive weighted average**:

```
         Σ_{k ∈ K(i,j)}  w_k × S_k(i,j)
S(i,j) = ─────────────────────────────────
         Σ_{k ∈ K(i,j)}  w_k
```

where K(i,j) = {k : S_k(i,j) ≠ NA} is the set of **observed** fields for pair (i,j).

When all fields are NA: S(i,j) = `na_fill` (default 0).

**Why better than imputation**:

| Approach             | Effect on missing field             |
|----------------------|-------------------------------------|
| both-null → 1        | False "definite match" signal       |
| Mean imputation      | Dilutes real signal from observed fields |
| Zero imputation      | Penalizes pair for missing data     |
| **Adaptive combination** | Ignores missing field; re-normalizes weights over observed fields |

**Key property**: Equivalent to a weighted average over only the fields actually observed. If name is available and year is missing, S = S_name. We don't pretend to know anything about year.

**Example**:
```
weights = {name: 0.6, year: 0.4}
pair (1,2): S_name = 0.9, S_year = NA  →  S = 0.9 × 0.6 / 0.6 = 0.9
pair (3,4): S_name = 0.9, S_year = 0.8 →  S = (0.9×0.6 + 0.8×0.4) / 1.0 = 0.86
```

Pair (1,2) correctly gets a higher combined score: name matches perfectly, and we don't penalize for missing year.

### Approach 2: `er_field_ensemble(sim_list, pairs, n, cluster_method, merge_alpha, threshold, min_pairs)`

**Per-field clustering then consensus merge**.

**Algorithm**:
1. For each field k with at least `min_pairs` non-NA similarities:
   - Build sparse S_k from pairs using `er_pairs_to_sparse()`
   - Run `er_cluster(S_k, method = cluster_method, threshold = threshold)` → label vector ℓ_k
2. Collect all {ℓ_k} into a list
3. Return `er_consensus(cluster_list, n, alpha = merge_alpha)`

**When to prefer Approach 2**:
- Fields are very heterogeneous in scale or type (e.g., a 4-character state code vs a 500-word abstract)
- You suspect fields carry complementary rather than redundant signals
- As a robustness check against Approach 1

**`merge_alpha` parameter**: Fraction of fields that must agree to co-cluster a pair.
- 0.5 (default): majority vote — pair is co-clustered if more than half of fields agree
- 1.0: intersection — all fields must agree (conservative, high precision)
- 0.0 would be union — any single field agreement co-clusters (liberal, high recall)

Fields with fewer than `min_pairs` (default 10) non-NA similarities are silently skipped.

**Usage comparison**:
```r
sim <- er_similarity(df, pairs)

# Approach 1 (default pipeline)
wt     <- er_weights(sim)
S      <- er_pairs_to_sparse(pairs, er_combine(sim, wt), n)
labs_1 <- er_cluster(S, "louvain")

# Approach 2 (per-field ensemble)
labs_2 <- er_field_ensemble(sim, pairs, n,
                             cluster_method = "threshold_cc",
                             merge_alpha    = 0.5)
```

---

## 10. Stage 7 – Clustering

The combined similarity S (output of `er_combine()`) is converted to a sparse n×n matrix via `er_pairs_to_sparse()`. Clustering algorithms operate on this matrix.

`er_cluster(S, method, ...)` — single method.
`er_cluster_all(S, methods, ...)` — all (or selected) methods; returns named list of label vectors.

Cluster count k is chosen automatically by silhouette (unsupervised) or ARI (supervised) when not specified.

**Full method list**:

| Type        | Method           | Notes |
|-------------|------------------|-------|
| Graph       | `threshold_cc`   | Connected components at fixed threshold |
| Graph       | `louvain`        | Modularity optimization (igraph) |
| Graph       | `leiden`         | Leiden community detection (igraph) |
| Graph       | `label_prop`     | Label propagation (igraph) |
| Graph       | `gc`             | Graph coloring (GCMER) |
| Centroidal  | `hclust_avg`     | Average linkage, **cosine distance** |
| Centroidal  | `hclust_ward`    | Ward.D2 linkage, **Euclidean distance** |
| Centroidal  | `pam`            | Partitioning around medoids (cluster pkg) |
| Supervised  | `svm`            | Pairwise RBF-SVM; requires `truth_vec` and `e1071` |
| Supervised  | `gbm`            | Pairwise XGBoost; requires `truth_vec` and `xgboost` |

### 10.1 Centroidal Methods (HC, PAM)

These methods need a feature matrix X, not just pairwise similarities.

**Feature extraction from S via truncated SVD**:
```
S ≈ U Σ Vᵀ   (rank-d approximation)
X = U × Σ    (d-dimensional embedding)
```

Uses `irlba::irlba(S, nv = svd_dim)` (default svd_dim = 50) — efficient for large sparse matrices.

#### Hierarchical Clustering

**Agglomerative / bottom-up**: Start with n singleton clusters; repeatedly merge the two closest clusters until k remain.

**Linkage criterion**:

| Linkage   | Distance(A, B)                       | ERBOT method   | Distance metric used |
|-----------|--------------------------------------|----------------|----------------------|
| `average` | Mean of all pairwise distances A↔B   | `hclust_avg`   | **Cosine** distance  |
| `ward.D2` | Increase in total within-cluster SS  | `hclust_ward`  | **Euclidean** distance |

**Why different distances?** Ward.D2 is geometrically valid only in Euclidean space — it merges by minimizing the increase in within-cluster sum of squares, which requires the concept of a centroid, which requires Euclidean geometry. Cosine distance does not define a valid centroid. Using cosine with Ward.D2 would produce mathematically inconsistent merges.

Average linkage does not require a centroid — it only averages pairwise distances — so it works correctly with cosine distance.

**Cosine distance**:
```
cosine_dist(u, v) = 1 - (u · v) / (||u|| × ||v||)
```

**Euclidean distance** (for Ward.D2): `stats::dist(X)` — standard Euclidean on the SVD embedding.

**Cutting the tree**: `stats::cutree(hc, k = k)` cuts the dendrogram at k clusters.

#### PAM – Partition Around Medoids

1. Select k records as *medoids* (most central actual records)
2. Assign every record to its nearest medoid
3. For each cluster, try swapping the medoid with every non-medoid; accept if it reduces total distance
4. Repeat until no improvement

**Medoid vs centroid**: A centroid is the mean of the cluster (may not correspond to any actual record). A medoid is an actual record minimizing sum of distances to all other cluster members. More robust to outliers, uses the distance matrix directly (no embedding required).

**Implementation**: `cluster::pam(cosine_dist_matrix, k)`.

### 10.2 Graph-Community Methods

These operate directly on the weighted similarity graph G = (V, E, W) where nodes are records, edges are candidate pairs with positive similarity, and weights are similarity scores.

#### Threshold Connected Components (`"threshold_cc"`)

1. Keep edges with S(i,j) ≥ threshold (default 0.5)
2. Find connected components of the resulting graph

**Strength**: Simple, interpretable.
**Weakness**: Transitivity chaining — if A~B (0.6) and B~C (0.6), then A and C are co-clustered even if sim(A,C) = 0.1.

#### Louvain Algorithm (`"louvain"`)

Maximizes **modularity Q**:
```
Q = (1/2m) Σ_{i,j} [A_ij - k_i k_j / (2m)] × δ(c_i, c_j)
```

where A_ij = edge weight, k_i = weighted degree of node i, m = total edge weight, δ = 1 if same community.

**Two phases** (repeated until convergence):
1. Move each node to the neighbor community that maximizes ΔQ
2. Compress communities into super-nodes and repeat

**Resolution parameter γ**: Default 1. Higher → more, smaller communities. Lower → fewer, larger communities.

**Weakness**: Non-deterministic; can produce disconnected communities.

#### Leiden Algorithm (`"leiden"`)

Refinement of Louvain (Traag et al., 2019) that adds a refinement step ensuring all communities are internally connected. Requires `igraph::cluster_leiden()` (falls back to Louvain if `leidenbase` unavailable).

**When to prefer Leiden**: When cluster connectedness is critical and reproducibility matters.

#### Label Propagation (`"label_prop"`)

Each node adopts the label most common among its neighbors (weighted). Repeat until stable. Very fast O(m) per iteration, but non-deterministic.

### 10.3 Graph Coloring (`"gc"`)

**Uses the GCMER package** (Degras-Valabregue & Chu, 2025).

**Idea**: Transform ER into a graph *coloring* problem on a *conflict graph*.

- **Conflict graph**: edge (i,j) exists if S(i,j) < threshold (records i and j are NOT the same entity)
- **Graph coloring**: assign colors such that no adjacent nodes share a color
- **Key insight**: A proper coloring of the conflict graph assigns entity labels — records in the same color class are never in conflict, so they could all be the same entity

**CPM score** (Constant Potts Model): quality metric for graph clustering:
```
CPM = Σ_c [ e_c - γ × n_c(n_c-1)/2 ]
```
where e_c = edges within cluster c, n_c = cluster size, γ = resolution. Higher CPM → better cluster structure.

**GCMER algorithms**:

| Method   | Type           | Description                                  |
|----------|----------------|----------------------------------------------|
| `DSatur` | Heuristic      | Color nodes in order of saturation (# distinct colors in neighborhood) |
| `MCS`    | Heuristic      | Maximum Cardinality Search ordering; fast    |
| `LMXRLF` | Heuristic     | Largest Minimum eXcluded; greedy with restart |
| `Tabucol`| Metaheuristic  | Tabu search with tabu list to avoid cycling  |
| `RLF`    | Heuristic      | Recursive Largest First; good for sparse graphs |

ERBOT uses `"rlf"` by default. Threshold τ can be swept; best τ selected by silhouette or ARI.

### 10.4 Supervised Pairwise Classifiers (SVM, GBM)

Both methods treat ER as a **pairwise binary classification** problem: for each candidate pair (i,j), predict match (1) or non-match (0) using the feature difference vector.

**Feature construction** (`.pair_features()`):
```
feature(i,j) = | X[i,] - X[j,] |   (element-wise absolute difference)
```
where X is the SVD embedding of S.

**Label construction** (`.pair_labels()`):
```
label(i,j) = 1 if truth_vec[i] == truth_vec[j], else 0
```

**Why train on pairs, not records?** ER is fundamentally a pairwise decision problem. The model must predict similarity between two records, not classify a single record.

**Class imbalance**: In ER, the vast majority of candidate pairs are non-matches. For n = 1,000 records with 10 entities of 100 members each: ~4,500 positive pairs vs ~44,550 negative pairs — roughly 1:10 ratio. In practice the ratio is often 1:100 or worse. Both methods correct for this:

- **SVM** (`e1071::svm(probability = TRUE)`): `class.weights = c("0" = 1, "1" = n_neg/n_pos)` — penalizes misclassification of the rare positive class proportionally more
- **GBM** (`xgboost::xgboost(objective = "binary:logistic")`): `scale_pos_weight = n_neg/n_pos` — same idea, implemented natively in XGBoost

**From probabilities to clusters**:
1. Predict match probability p(i,j) for all candidate pairs
2. Build new similarity matrix S_new from these probabilities
3. Apply `threshold_cc` to produce final cluster labels

**Fallback**: If labeled pairs are insufficient (< 10 positive or negative examples), both methods fall back to Louvain with a warning.

**Require**: `truth_vec` must be provided; methods are silently skipped in `er_cluster_all()` when absent.

### 10.5 k-Tuning Strategies

For centroidal methods (HC, PAM), cluster count k must be specified or auto-tuned.

**k_grid**: Default {5, 10, 15, 20, 30, 50} or user-specified.

**Supervised tuning** (with truth): Select k maximizing ARI(labels, truth).

**Unsupervised tuning**: Select k maximizing average silhouette width:

```
s(i) = (b(i) - a(i)) / max(a(i), b(i))
```

where a(i) = mean intra-cluster distance, b(i) = mean distance to nearest other cluster.

- s(i) ≈ 1: Well-placed record
- s(i) ≈ 0: On cluster boundary
- s(i) < 0: Possibly in wrong cluster

---

## 11. Stage 8 – Post-Processing and Merging

### `er_merge(cluster_list, S, method, threshold, alpha, absorb_small, m_min, truth_vec)`

Takes the named list of label vectors from `er_cluster_all()` and produces one final clustering.

**Strategies**:

| Strategy        | Description |
|-----------------|-------------|
| `"transitivity"` | Connected components of the similarity graph at `threshold` |
| `"consensus"`    | Majority-vote co-membership across all methods |
| `"best"`         | Single best method by ARI (if truth given) or first method |
| `"none"`         | Return first method's labels unchanged |

### 11.1 Transitivity Enforcement (`er_transitivity`)

Builds a graph from pairs with S(i,j) ≥ threshold, returns connected components.

**Mathematical property**: After enforcement, if A and B are co-clustered AND B and C are co-clustered AND sim(A,C) ≥ τ, then A and C are also co-clustered. This is the *transitive closure* of the threshold relation.

### 11.2 Consensus Clustering (`er_consensus`)

For each pair (i,j), count how many methods M place i and j in the same cluster. If this count ≥ ⌈α × M⌉, declare the pair as co-clustered. Find connected components.

**Note**: `er_consensus()` operates over *methods* (all run on the same combined similarity matrix). `er_field_ensemble()` operates over *fields* (each with its own similarity matrix). These are complementary and can be combined.

**Example** (4 methods, α = 0.5):
- Louvain: {A,B,C}, {D,E}
- Leiden: {A,B}, {C,D,E}
- HC-avg: {A,B,C}, {D,E}
- GC: {A,B,C,D}, {E}

Pair (A,B): 4/4 agree → YES. Pair (C,D): 1/4 → NO. Result: {A,B,C}, {D}, {E}.

### 11.3 Small-Cluster Absorption (`er_absorb_small`)

Clusters of size < m_min (default 2) are merged into the large cluster with the highest mean similarity to their members.

---

## 12. Stage 9 – Evaluation

### `er_evaluate(pred, truth, id_vec)`

Truth labels loaded via `er_truth_from_any()` from vector, file, or data frame. Pair lists converted to cluster labels by `er_pairs_to_clusters()`.

### 12.1 Adjusted Rand Index (ARI)

**Reference**: Hubert & Arabie (1985), *Journal of Classification*.

```
ARI = (RI - E[RI]) / (max(RI) - E[RI])
```

Corrects the Rand Index for the expected value under random clustering. ARI = 1.0 → perfect; ARI ≈ 0 → random; ARI < 0 → worse than random.

**Primary computation**: `GCMER::adj_rand(pred, truth)`. Backups: `aricode::ARI()`, `mclust::adjustedRandIndex()`.

**Limitation**: Sensitive to cluster size imbalance. Large clusters dominate.

### 12.2 Normalized Mutual Information (NMI)

**Implemented via `GCMER::mutual_info(pred, truth)`**, which returns a named vector including:

- `"MI"`: Raw Mutual Information
- `"G"`: Geometric mean NMI
- `"FJ"`: Harmonic mean NMI (Fred & Jain) — **ERBOT uses this**
- `"VI"`: Variation of Information (see below)

**Harmonic mean NMI** (Fred & Jain):
```
NMI_FJ = 2 × I(C; K) / (H(C) + H(K))
```

where I(C; K) = mutual information, H(C) = entropy of predicted clusters, H(K) = entropy of true classes.

**Interpretation**: NMI = 1 → perfect agreement; NMI = 0 → no shared information.

**When to prefer NMI over ARI**: NMI is less sensitive to the number of clusters and to cluster size imbalance. When comparing clusterings with very different numbers of clusters, NMI gives a more stable comparison.

### 12.3 Variation of Information (VI)

```
VI(C, K) = H(C | K) + H(K | C)
```

= sum of the conditional entropies in each direction.

**Properties**:
- VI = 0: Perfect agreement
- VI increases as clusterings diverge
- VI is a *metric* on the space of clusterings (satisfies triangle inequality)
- Unbounded above (unlike ARI and NMI which are in [-1,1] and [0,1])

**When useful**: VI is directly interpretable as "information lost" when going from one clustering to another. Useful when you want a distance measure between clusterings, not just a normalized score.

### 12.4 B-Cubed (B³) Precision, Recall, F-Score

**Reference**: Bagga & Baldwin (1998).

For record i:
```
Precision(i) = |cluster(i) ∩ entity(i)| / |cluster(i)|
Recall(i)    = |cluster(i) ∩ entity(i)| / |entity(i)|
```

Overall:
```
B³-P = mean_i Precision(i)
B³-R = mean_i Recall(i)
B³-F = 2 × B³-P × B³-R / (B³-P + B³-R)
```

**Why better than pairwise for ER**: Pairwise metrics weight large clusters heavily (a cluster of 100 contributes C(100,2) = 4,950 pairs; a size-2 cluster contributes 1). B-cubed gives equal weight to every record regardless of cluster size.

### 12.5 V-Measure (Homogeneity and Completeness)

**Reference**: Rosenberg & Hirschberg (2007).

```
Homogeneity H  = 1 - H(C|K) / H(C)   [each cluster is pure]
Completeness Co = 1 - H(K|C) / H(K)   [each entity is in one cluster]
V-measure = 2 × H × Co / (H + Co)
```

**Intuition**:
- High H, low Co: Many small, pure clusters — every record is its own cluster (H=1, Co=0)
- Low H, high Co: Few large clusters containing all true matches but also false positives

### 12.6 Pairwise F-Score

Classical pair-level precision/recall:
```
PairP = |correctly co-clustered pairs| / |predicted co-clustered pairs|
PairR = |correctly co-clustered pairs| / |truly co-occurring pairs|
PairF = 2 × PairP × PairR / (PairP + PairR)
```

**Limitation**: Dominated by large clusters. Used mainly for historical comparison.

### 12.7 Unsupervised Quality (`er_unsupervised_quality`)

When no ground truth is available:
- **Within-cluster mean similarity**: Average S(i,j) for same-cluster pairs (higher is better)
- **Between-cluster mean similarity**: Average S(i,j) for different-cluster pairs (lower is better)
- **Similarity gap**: within - between (higher is better)
- **Average silhouette width**: See Section 10.5

---

## 13. Neural Entity Resolution Module

Optional neural module for learning entity embeddings. Files: `42-neural_encoder.R` through `45-training_loop.R`. Requires `torch`.

### 13.1 RecordEncoder Architecture

```
Input: n × p feature matrix X

Layer 1: Linear(p → hidden_dim)  +  ReLU
Layer 2: Dropout(rate = 0.2)
Layer 3: Linear(hidden_dim → emb_dim)
Output:  L2-normalize(z)  →  n × emb_dim  (unit-norm embeddings)
```

Default: hidden_dim = 128, emb_dim = 64.

**L2 normalization**: Forces embeddings onto the unit sphere. Cosine similarity of two unit vectors = their dot product. This is required for the contrastive loss to work correctly.

**Implementation**: `torch::nn_module()`.

### 13.2 Contrastive Loss

For each positive pair (anchor i, positive j):

```
L_contrastive = -log[ exp(z_i · z_j / τ) / Σ_{k ≠ i} exp(z_i · z_k / τ) ]
```

where τ = temperature (default 0.1).

**Temperature τ**: Borrowed from the Boltzmann distribution in statistical physics. Low τ → very sharp softmax, model focuses on the hardest negatives. High τ → softer distribution, all negatives treated more equally. τ = 0.1 is standard for contrastive metric learning.

**ERBOT adds a negative-pair penalty**:
```
total = L_contrastive + λ × mean_{(i,j) negative} max(0, z_i · z_j - margin)
```

This explicitly penalizes negatives with similarity above `margin` (default 0.1).

**Negative ratio**: The number of negatives per positive in each training batch. Higher negative ratio → harder training, better discrimination, but slower. In practice, using all in-batch negatives (as InfoNCE does) is efficient. ERBOT uses all non-positive pairs in the batch as negatives.

### 13.3 Graph Smoothness Regularization

**Graph Laplacian**: L = D - W (D = diagonal degree matrix, W = weighted adjacency).

**Smoothness loss**:
```
L_smooth = Tr(Zᵀ L Z) = Σ_{(i,j) ∈ E} W_ij × ||z_i - z_j||²
```

Penalizes connected record pairs from being far apart in embedding space. If two records are similar (high edge weight), their embeddings should be close.

### 13.4 Stability Penalty

For original graph G and perturbed graph G^δ (Gaussian noise on edge weights + random dropout):

```
penalty = |rank(P) - rank(P^δ)| + Σ_{m=1}^{top_m} |λ_m(PLP) - λ_m(P^δ L^δ P^δ)|
```

where P = HHᵀ is the projection onto the cluster indicator subspace.

- **Term 1**: Rank difference → change in number of clusters under perturbation
- **Term 2**: Spectral difference → change in cluster boundary structure

**Current implementation**: Stability penalty is computed but does not backpropagate gradients (discrete clustering is not differentiable). Only the contrastive loss drives the gradient update.

### 13.5 Training Loop

**Single epoch** (`train_one_epoch()`):
1. Forward pass: z = encoder(X)
2. Build kNN graph G from z
3. Perturb G → G^δ; cluster both → P, L, P^δ, L^δ
4. Compute: total = α × L_contrastive + β × L_smooth + γ × stability
5. Backpropagate contrastive loss only; Adam optimizer step

**Full training** (`run_training()`): Multi-epoch with lr = 1e-3; returns encoder + history.

---

## 14. Dataset Profile Report

### `er_data_profile(data, diag, output_file, title, top_k_cats, hist_bins, max_fields)`

Generates a self-contained multi-page PDF describing the dataset's structure, field characteristics, and recommendations. Can be called after Stage 1 (loading) and optionally Stage 2 (diagnosis).

**Pages produced**:

1. **Cover page**: Title, timestamp, overview table (records, columns, mode, estimated pairs, ID/source columns, blocking key/method), field-type bar chart.

2. **One page per field** (up to `max_fields` = 30): Three panels per page:
   - *Field info panel*: type, missingness %, cardinality ratio, avg tokens, recommended similarity method, blocking candidacy
   - *Distribution chart*: histogram for numeric/year; word-count histogram for text; top-k bar chart for categorical
   - *Missingness bar*: color-coded horizontal bar showing % missing vs present

3. **Recommendations page**: Plain-text table — blocking strategy (key, method, needed?), per-field similarity methods, blocking candidacy.

**Usage**:
```r
df   <- er_load("data/authors.csv")
diag <- er_diagnose(df)
er_data_profile(df, diag,
                output_file = "reports/authors_profile.pdf",
                title       = "Authors Dataset — Profile")
```

Can also be called with only `data` (no diagnosis) for a quick structural overview before running the full pipeline.

---

## 15. Data Structures

### Input Record Table

Standard `data.frame`:
```
id    entity_id  name             city      year
1     1          "Alice Smith"    "London"  2019
2     1          "A. Smyth"       NA        2019
3     2          "Bob Jones"      "Paris"   NA
```

Required: record identifier column (default `"id"`).
Optional: entity identifier column (default `"entity_id"`) for evaluation.

### Candidate Pairs

`tibble(idx1, idx2)` — 1-based integer row indices, always idx1 < idx2:
```
idx1  idx2
1     2
1     3
2     3
```

These are row *positions*, not record IDs. This makes array indexing O(1).

### Similarity List

Named list, one numeric vector per field:
```r
list(
  name = c(0.967, NA, 0.500, ...),
  city = c(0.931, 0.500, NA, ...),
  year = c(1.000, 1.000, NA, ...)
)
```

Each vector length = number of candidate pairs.

### Weights Vector

Named numeric vector summing to 1:
```r
c(name = 0.558, city = 0.291, year = 0.151)
```

### Sparse Similarity Matrix

`Matrix::dgCMatrix` — n×n symmetric sparse matrix. Diagonal = 1. Off-diagonal: only positive-similarity candidate pairs stored. All other entries implicitly 0.

For n = 10,000, 200,000 pairs: ~5 MB sparse vs ~800 MB dense.

### Cluster Labels

Integer vector of length n. Arbitrary integer labels; records sharing the same label are in the same cluster.

### Performance Table

Tibble with columns: `method`, `ARI`, `NMI`, `VI`, `Bcubed_P`, `Bcubed_R`, `Bcubed_F`, `Homogeneity`, `Completeness`, `Vmeasure`, `Pair_P`, `Pair_R`, `Pair_F`.

---

## 16. Complete API Reference

### Main Entry Points

| Function | Description |
|----------|-------------|
| `er_run(data, ...)` | Full 9-stage pipeline |
| `er_main(data, ...)` | User-friendly facade |
| `er_unified_pipeline(...)` | Stage composition with tuning |
| `er_tune(data, fields, methods, ...)` | Grid-search benchmarking on embedding-based methods |

### Stage Functions

| Function | Stage | Key Parameters |
|----------|-------|----------------|
| `er_load(input)` | 1 | `input`: path / data.frame / keyword |
| `er_diagnose(d, text_cols)` | 2 | Returns `fields` tibble: type, missingness, cardinality\_ratio, sim\_method, blocking\_candidate |
| `er_block(d, method, key_col, window)` | 3 | `method` ∈ {none, standard, prefix, sn, auto} |
| `er_similarity(d, pairs, spec, diag)` | 4 | `spec`: named list of similarity types per field |
| `er_weights(sim_list, method, pairs, truth)` | 5 | `method` ∈ {auto, equal, variance, bimodal, fellegi_sunter, ari} |
| `er_combine(sim_list, weights, na_fill)` | 6a | NA-aware weighted average |
| `er_field_ensemble(sim_list, pairs, n, cluster_method, merge_alpha)` | 6b | Per-field clustering then consensus |
| `er_cluster(S, method, k, threshold, resolution)` | 7 | `method` ∈ {hclust_avg, hclust_ward, pam, threshold_cc, louvain, leiden, label_prop, gc, svm, gbm} |
| `er_cluster_all(S, methods, ...)` | 7 | `methods = "all"` runs all |
| `er_merge(cluster_list, S, method, alpha)` | 8 | `method` ∈ {transitivity, consensus, best, none} |
| `er_evaluate(pred, truth, id_vec)` | 9 | Returns tibble of all metrics |

### Combination and Ensemble

| Function | Description |
|----------|-------------|
| `er_combine(sim_list, weights, na_fill)` | Approach 1: NA-aware weighted average |
| `er_field_ensemble(sim_list, pairs, n, ...)` | Approach 2: per-field cluster then consensus |
| `er_weights(sim_list, method, ...)` | Weight learning (6 strategies) |

### Post-Processing

| Function | Description |
|----------|-------------|
| `er_merge(cluster_list, S, method, ...)` | Main post-processing entry point |
| `er_transitivity(S, threshold)` | Transitive closure of threshold graph |
| `er_consensus(cluster_list, n, alpha)` | Majority-vote co-membership consensus |
| `er_absorb_small(labels, S, m_min)` | Absorb small clusters into nearest large |

### Evaluation

| Function | Description |
|----------|-------------|
| `er_evaluate(pred, truth, id_vec)` | ARI, NMI, VI, B³, V-measure, Pairwise F |
| `er_unsupervised_quality(labels, S)` | Within/between sim, gap, silhouette |
| `er_truth_from_any(truth)` | Parse truth from vector / file / data.frame |
| `er_pairs_to_clusters(pairs, labels)` | Convert pair decisions to cluster labels |
| `er_block_stats(pairs, truth, n)` | Reduction ratio + pair completeness |

### Utility Functions

| Function | Description |
|----------|-------------|
| `er_pairs_to_sparse(pairs, S_vec, n)` | Build sparse n×n matrix |
| `er_cosine_dist(X)` | Cosine distance matrix |
| `er_silhouette_avg(labels, dist_mat)` | Average silhouette width |
| `er_data_profile(data, diag, output_file, ...)` | Dataset profile PDF |
| `er_save_report_pdf(res, file, ...)` | Pipeline result PDF report |
| `er_write_performance(x, file, digits)` | Save performance table to CSV/TXT |
| `normalize_weights(w)` | Normalize to sum to 1 |
| `make_timestamp_filename(prefix, ext)` | Timestamped filename |
| `%||%` | Fallback operator: `a %||% b` returns a if not NULL, else b |

### Reporting

| Function | Description |
|----------|-------------|
| `er_save_report_pdf(res, file, ...)` | Multi-page PDF (params, performance, curves) |
| `er_autoplot_tuning(res, metric)` | Draw 1–4 tuning panels |
| `er_draw_table(df, title)` | Render data.frame as grid table |
| `er_plot_curve(df, x_col, y_col, ...)` | Tuning curve line plot |
| `er_params_df(res)` | Extract parameters from pipeline result |
| `er_gc_top_table(res, top_n)` | Top GC threshold table |
| `er_data_profile(data, diag, ...)` | Dataset profile PDF |

### Neural Functions

| Function | Description |
|----------|-------------|
| `RecordEncoder(input_dim, hidden_dim, emb_dim, dropout)` | torch nn_module |
| `contrastive_loss(z, pos_pairs, tau, lambda, margin)` | Contrastive loss |
| `build_knn_graph(z, k)` | k-NN graph from embeddings |
| `perturb_graph(g, noise_sd, dropout_rate)` | Gaussian + dropout perturbation |
| `graph_laplacian_matrix(g)` | L = D - W |
| `graph_smoothness_loss(z, g)` | Tr(ZᵀLZ) |
| `projection_from_membership(labels, n)` | P = HHᵀ |
| `stability_penalty(P, L, P_delta, L_delta, top_m)` | K-theory surrogate |
| `train_one_epoch(encoder, X, pos_pairs, G_orig, ...)` | Single training epoch |
| `run_training(X, pos_pairs, n_epochs, ...)` | Full training loop |

---

## 17. Worked Examples

### Example 1: Minimal Deduplication (6 records, full pipeline)

```r
library(erbot)

d <- data.frame(
  id        = 1:6,
  entity_id = c(1,1,2,2,3,3),
  name      = c("Alice Smith", "Alice Smyth", "Bob Jones",
                "B. Jones",   "Carol White", "Carol Wht"),
  city      = c("London", "Londen", "Paris", NA, "Rome", "Roma")
)

# Stage 2: diagnose
diag <- er_diagnose(d)
er_data_profile(d, diag, output_file = "profile.pdf")  # optional

# Stage 3: block (small data → none)
pairs <- er_block(d, method = "none")   # all 15 pairs

# Stage 4: similarities
sims <- er_similarity(d, pairs,
                      spec = list(name = "jw", city = "jw"))

# Stage 5: weights
w <- er_weights(sims, method = "fellegi_sunter")

# Stage 6a: combine (Approach 1)
S <- er_pairs_to_sparse(pairs, er_combine(sims, w), n = 6)

# Stage 7: cluster all methods
all_labs <- er_cluster_all(S)

# Stage 8: consensus post-processing
final <- er_merge(all_labs, S, method = "consensus", alpha = 0.5)

# Stage 9: evaluate
truth <- setNames(d$entity_id, d$id)
ev    <- er_evaluate(final, truth, id_vec = as.character(d$id))
print(ev)  # ARI, NMI, VI, B³, V-measure, Pairwise F
```

### Example 2: Approach 2 (field ensemble) vs Approach 1

```r
sim <- er_similarity(d, pairs, spec = list(name="jw", city="jw", year="year"))

# Approach 1
wt     <- er_weights(sim, method = "fellegi_sunter")
S      <- er_pairs_to_sparse(pairs, er_combine(sim, wt), n = nrow(d))
labs_1 <- er_cluster(S, "louvain")

# Approach 2
labs_2 <- er_field_ensemble(sim, pairs, n = nrow(d),
                             cluster_method = "threshold_cc",
                             merge_alpha    = 0.5)

# Compare
ev1 <- er_evaluate(labs_1, truth, id_vec = as.character(d$id))
ev2 <- er_evaluate(labs_2, truth, id_vec = as.character(d$id))
```

### Example 3: Large-Scale with Sorted Neighborhood Blocking

```r
d     <- er_load("d10k")
pairs <- er_block(d, method = "sn", key_col = "name", window = 3L)
cat("Pairs:", nrow(pairs), "vs full:", nrow(d)*(nrow(d)-1)/2, "\n")

sims <- er_similarity(d, pairs,
                      spec = list(name = "jw", title = "jaccard",
                                  abstract = "bow", year = "year"))
w    <- er_weights(sims)  # auto → fellegi_sunter (no truth)

S      <- er_pairs_to_sparse(pairs, er_combine(sims, w), n = nrow(d))
labs   <- er_cluster_all(S, methods = c("louvain", "leiden", "threshold_cc"))
final  <- er_merge(labs, S, method = "consensus")
```

### Example 4: Supervised SVM and GBM Clustering

```r
truth_vec <- setNames(d$entity_id, d$id)
id_vec    <- as.character(d$id)

# Provide truth_vec to enable SVM and GBM
labs_all <- er_cluster_all(S,
                           methods   = c("louvain", "svm", "gbm"),
                           truth_vec = truth_vec[id_vec],
                           X         = X)   # X from irlba SVD embedding

final <- er_merge(labs_all, S, method = "consensus")
```

---

## 18. Common Questions and Misconceptions

This section covers questions that arise in research reviews, advisor meetings, and seminars — from foundational concepts to implementation details.

---

### Part A: Entity Resolution Fundamentals

**Q: What is the difference between deduplication and record linkage?**

Deduplication works within a single dataset — find records that refer to the same entity. Record linkage works across two or more datasets — match records in database A to corresponding records in database B. ERBOT handles both. Mode is auto-detected by checking whether a `source_id` column is present.

---

**Q: Why does blocking matter so much? Can't we just compare all pairs?**

For n records, full pairwise comparison requires n(n-1)/2 pairs. At n = 10,000 that is ~50 million pairs; at n = 77,707 (full NCVR) it is ~3 billion pairs. Even at 1 microsecond per comparison, 3 billion pairs takes 50 minutes just for comparison — before clustering. Blocking reduces this to a manageable set (thousands to tens of thousands of pairs) while retaining nearly all true matches.

---

**Q: What is the "both-null artifact" and why does it matter?**

Standard string similarity functions return sim("","") = 1 — two empty strings are "identical." This is mathematically correct for the similarity function but semantically wrong for ER: two records that are both missing a field provide no evidence of being the same entity. If an entity always has a field missing (e.g., a data source that never records middle name), sim = 1 fires for all pairs involving that entity — completely false match signal. ERBOT returns NA instead, and `er_combine()` ignores that field for that pair, regardless of why it is missing.

---

**Q: Is entity-disjoint evaluation truly the right approach in practice?**

Entity-disjoint evaluation is appealing in theory: it withholds all records from test entities during training, so the model cannot memorize entity-specific patterns. The resulting ARI inflation from record-disjoint splits can be 5–15%. However, entity-disjoint splitting requires pre-assigned entity identifiers to define the split — which is precisely what ER is trying to discover. This circular dependency makes entity-disjoint splits impractical in most real settings. ERBOT's pipeline is primarily unsupervised and does not train on entity labels; ground-truth labels are used only as a post-hoc quality check. In benchmark settings (CORA, NCVoters) where labels are available, either split strategy can be used, but the labeling bottleneck — not the split choice — is the dominant practical constraint.

---

**Q: When does high missingness in a field become a real problem?**

High overall missingness (e.g., 60% of records missing `middle_name`) is handled gracefully by ERBOT: `er_combine()` simply skips that field for any pair where it is absent. The more dangerous pattern is **systematic missingness** — all records from one data source lack a field entirely. In that case, every within-source pair triggers the both-null artifact if similarities are not NA-coded. ERBOT avoids this by returning NA at the similarity level and ignoring those field-pair combinations in `er_combine()`. The practical rule: fields with miss rate > 0.3 are excluded as blocking keys; below that threshold they participate normally in similarity computation with per-pair NA handling.

---

### Part B: Similarity and Weight Learning

**Q: Why does ERBOT return NA instead of 0 or 0.5 for missing similarities?**

0 signals "definite non-match" and penalizes a pair for missing data. 0.5 is arbitrary neutral — still injects fake data that biases the combined score. NA is truthful: "we have no information about this field for this pair." The adaptive combination in `er_combine()` handles NA by excluding that field from the weighted average entirely, re-normalizing over only the fields that are actually observed.

---

**Q: Why is IDF-weighted BoW preferred over plain Jaccard for long text?**

Jaccard treats all words equally. In a corpus of academic abstracts, words like "the", "is", "of" appear in every document and carry no discriminative information — yet they contribute to the Jaccard union. IDF (Inverse Document Frequency) down-weights terms that appear in many documents and up-weights rare, discriminative terms. A paper about "zymogen cascade" will have high IDF weight on "zymogen," making it highly specific. The smoothed IDF formula log((N+1)/(df(t)+1)) + 1 avoids zero-division and ensures even universal terms get a small positive weight.

---

**Q: Why use Fellegi-Sunter weights instead of equal weights?**

Different fields have radically different discriminative power. In a bibliography database, "year" alone has low discriminative power (many papers share any given year), while "author name" is highly discriminative. Fellegi-Sunter estimates this automatically: it fits a Beta mixture to each field's similarity distribution and extracts the log Bayes factor log(m_k / u_k). Fields where matches score much higher than non-matches get high weight — without requiring any labeled training data.

---

**Q: Why not use ridge or lasso for field weight learning?**

Ridge and lasso are regularized regression techniques that minimize a labeled loss function (e.g., mean squared error). They require labeled training examples. `er_weights()` strategies `variance`, `bimodal`, and `fellegi_sunter` are all unsupervised — they derive weights purely from the similarity distributions without any labels. When labels are available, `er_weights(method = "ari")` directly measures each field's clustering quality, which is more relevant to ER than a regression loss. Ridge would add unnecessary complexity. ERBOT uses ridge only if it were fitting weights in a regression model — it doesn't.

---

**Q: What does "ARI-based weighting" do exactly?**

For each field k, it builds a threshold-CC clustering using just that field's similarity (at the median threshold) and computes ARI between that clustering and the true entity labels. Fields whose individual clustering already aligns well with the truth get higher weight. This is the most principled supervised weight learning method available in ERBOT.

---

**Q: Why does Fellegi-Sunter EM only run for 5 iterations?**

The 2-component Beta mixture model is well-identified from a reasonable initialization (median split). In practice, the EM updates converge to within numerical precision in 3–5 iterations for this parametrization. More iterations would give identical weights while costing O(5×) as much computation. This is a deliberate efficiency choice, not a limitation.

---

### Part C: Combining Multiple Fields

**Q: What are the two approaches for handling multiple fields? When is each preferred?**

**Approach 1** (default): Compute per-field similarities → weighted average → one combined matrix S → cluster once on S. Preferred when fields have compatible scales, or when interpretability of weights matters, or when missing data patterns vary by pair (the adaptive NA handling is its key strength).

**Approach 2** (new, `er_field_ensemble`): Cluster each field independently → merge by majority-vote co-membership. Preferred when fields are very heterogeneous in scale or measurement type, when you suspect fields carry complementary rather than redundant signals, or as a robustness check against Approach 1.

---

**Q: What does `merge_alpha` in `er_field_ensemble` control?**

`merge_alpha` is the fraction of fields that must agree on co-clustering a pair for it to appear in the final partition. With alpha = 0.5 (majority vote), a pair is co-clustered if more than half the fields' individual clusterings agree. With alpha = 1.0, all fields must agree (intersection — conservative, high precision). With alpha approaching 0, any single field agreeing is enough (union — liberal, high recall). The right value depends on field quality: if some fields are very noisy, a lower alpha is safer.

---

**Q: How is `er_field_ensemble` different from `er_consensus` in Stage 8?**

`er_consensus` (Stage 8) merges results from **multiple clustering methods** all applied to the **same combined similarity matrix S**. It addresses the question: "which pairs do most clustering algorithms agree on?" `er_field_ensemble` (Stage 6) clusters each **field's similarity matrix separately** and then merges those field-level label vectors. It addresses the question: "which pairs do most fields individually suggest co-clustering?" They are complementary and can be combined: use `er_field_ensemble` for Stage 6, then apply `er_consensus` across multiple `er_field_ensemble` runs with different `cluster_method` values.

---

### Part D: Clustering Methods

**Q: Why does Ward.D2 require Euclidean distance but not average linkage?**

Ward.D2 minimizes the increase in total within-cluster *sum of squares* when two clusters are merged. Sum of squares requires computing a centroid (the mean of the cluster). A centroid is a well-defined concept only in Euclidean space — it minimizes the sum of squared Euclidean distances. Cosine distance does not define a valid centroid: the mean of two unit vectors is not a unit vector, and the "mean direction" has no clean algebraic interpretation in the context of Ward merging. Average linkage avoids centroids entirely — it only averages the pairwise distances between members of the two clusters, which is valid for any distance metric including cosine.

---

**Q: Why use Louvain and Leiden rather than k-means directly on S?**

k-means requires: (1) a Euclidean feature space, (2) a predetermined k, and (3) centroid updates. The similarity matrix S is not directly a feature matrix (it's n×n and can be sparse). Graph community methods (Louvain, Leiden) work directly on the weighted graph defined by S — no feature extraction, no k specification, no centroid. They are also specifically designed to find "communities" (densely internally connected, sparsely connected externally), which is exactly the structure we expect for entity clusters.

---

**Q: SVM is a classifier — how does it produce cluster labels?**

SVM is trained as a pairwise binary classifier: for each candidate pair (i,j), predict "match" (1) or "non-match" (0). The feature for pair (i,j) is the element-wise absolute difference |X_i - X_j| where X is the SVD embedding. After training, the SVM predicts match probabilities for all pairs. These probabilities form a new similarity matrix, which is then processed by `threshold_cc` to produce cluster labels. So SVM provides a better-calibrated pairwise similarity matrix than the raw Jaro-Winkler/BoW scores; the actual clustering step is still graph-based.

---

**Q: How is class imbalance in pairwise ER classification handled?**

In entity resolution, positive pairs (true matches) are vastly outnumbered by negative pairs (non-matches). For 1,000 records with 10 entities of 100 members each, there are C(100,2) × 10 = 44,550 positive pairs but ~455,000 negative pairs — a 1:10 ratio. In practice it can be 1:100 or worse after blocking. Without correction, a classifier that predicts "non-match" for everything achieves 99%+ accuracy. ERBOT handles this by:
- **SVM**: `class.weights = c("0" = 1, "1" = n_neg/n_pos)` — penalizes missing a true match proportionally more
- **GBM**: `scale_pos_weight = n_neg/n_pos` — same idea in XGBoost's objective

Both use the actual observed ratio of negative to positive pairs from the labeled training data.

---

**Q: What is the resolution limit of modularity-based clustering?**

Louvain and Leiden optimize modularity Q, which has a built-in resolution scale. Communities smaller than approximately √(2m) nodes (in an unweighted graph with m total edges) tend to be incorrectly merged with neighboring communities. This is the *resolution limit* (Fortunato & Barthélemy, 2007). For ER with many small entities (e.g., 10,000 entities averaging 3 records each in NCVR), this can cause problems. Mitigations: (1) increase the resolution parameter γ, (2) use `threshold_cc` or graph coloring (GC), which do not have this resolution limit, or (3) use `er_field_ensemble` which applies simpler per-field clustering.

---

**Q: What is the CPM score and how does it differ from modularity?**

CPM (Constant Potts Model) is:
```
CPM = Σ_c [e_c - γ × n_c(n_c-1)/2]
```
where e_c = edges within cluster c, n_c = cluster size, γ = resolution parameter.

Unlike modularity (which divides by 2m, making it density-relative), CPM uses an absolute density threshold γ. This means CPM does not have a resolution limit — communities of any size can be identified correctly as long as their internal density exceeds γ. CPM is used in GCMER (`er_cpm_score()`) as a quality metric for graph coloring results.

---

**Q: What does the `er_tune()` function do, and how is it different from `er_cluster_all()`?**

`er_cluster_all()` applies multiple clustering methods to the pairwise similarity matrix S (output of `er_combine()`). `er_tune()` does a grid-search benchmarking over hyperparameters (embedding dimension, k-NN k, resolution, etc.) for a set of embedding-based methods (`kmeans`, `agglo`, `dbscan`, `louvain`, `leiden`, `cw`, `threshold_cc`, `mst_edit`). Crucially, `er_tune()` operates on an **embedding matrix Z** (n×d feature matrix), not on S. It is designed for comparing many configurations and produces a tidy `curves` data frame suitable for `er_save_report_pdf()`.

---

### Part E: Evaluation Metrics

**Q: Why use ARI instead of accuracy for clustering evaluation?**

Cluster labels are arbitrary — if the true clustering has labels {1,2,3} and the predicted clustering assigns {3,1,2} to the same records, they are identical clusterings but the label vectors differ. Accuracy requires label alignment. ARI operates on the co-clustering structure (which pairs are in the same cluster) and is invariant to label permutation. It is also corrected for chance, so a random clustering gets ARI ≈ 0.

---

**Q: When should NMI be preferred over ARI?**

ARI is sensitive to the number of clusters and to the presence of many small clusters (singletons dominate the count). NMI is based on information theory and is more stable when comparing clusterings with very different numbers of clusters or very different size distributions. If you are comparing a fine-grained clustering (many small clusters) with a coarse one (few large clusters), NMI is more interpretable. ERBOT reports the harmonic-mean NMI (Fred & Jain formulation) as `NMI` and the raw Variation of Information as `VI`.

---

**Q: What is Variation of Information (VI) and when is it useful?**

VI = H(C|K) + H(K|C): the sum of how much information is lost going from the predicted clustering C to the true K, plus the reverse. VI = 0 means perfect agreement; higher VI means greater divergence. Unlike ARI and NMI, VI is a *metric* (satisfies the triangle inequality), making it suitable for theoretical analysis of clustering algorithms. It is also directly interpretable as "bits of information lost." Useful when you want to compare multiple clusterings in a principled distance sense, not just a scalar quality score.

---

**Q: What is the difference between B-cubed F and pairwise F?**

Both measure precision and recall of co-clustering decisions, but at different granularities. Pairwise F operates at the pair level: a cluster of 100 records contributes C(100,2) = 4,950 pairs to the calculation. B-cubed operates at the record level: every record contributes equally regardless of its cluster size. For ER datasets with highly uneven entity sizes (some entities have 100 records, most have 2), pairwise F is dominated by the large entities and may look good even when small-entity performance is poor. B-cubed gives a more representative overall picture.

---

**Q: What is pair completeness (PC) and how is it different from recall?**

PC is the *recall of the blocking step*: the fraction of all true-match pairs that appear in the candidate set generated by blocking. It measures whether blocking misses any true matches — if PC < 1, some true matches were discarded before comparison even began and can never be recovered. Standard recall (in clustering evaluation) measures whether co-clustered pairs are truly matches. They measure different things: PC is about what pairs survive to comparison; recall is about how those surviving pairs are classified.

---

### Part F: Neural Module

**Q: What is temperature in contrastive learning, and why is τ = 0.1?**

Temperature τ in the InfoNCE loss controls the sharpness of the similarity distribution used in the softmax:
```
L = -log[ exp(z_i · z_j / τ) / Σ_{k} exp(z_i · z_k / τ) ]
```
Low τ (e.g., 0.1) makes the softmax very peaked: the model is forced to assign nearly all probability to the single most similar negative in the denominator — training is focused on the hardest negatives. High τ (e.g., 1.0) gives a flatter distribution where all negatives contribute roughly equally — easier training but less discriminative representations. τ = 0.1 is the standard in contrastive metric learning (SimCLR, MoCo, etc.) and works well in practice.

---

**Q: What is the negative ratio in contrastive learning, and how is it set?**

The negative ratio is the number of negative pairs (non-matching) used per positive pair (matching) in the training loss. In ERBOT's implementation, all non-positive pairs in each training batch serve as negatives (in-batch negatives). If a batch has B records and P positive pairs, there are approximately B² - P negatives available. The negative ratio is thus approximately B²/P - 1. A higher ratio (more negatives) generally improves representation quality by providing harder contrast signal, at the cost of more computation per batch. The batch size B controls this implicitly.

---

**Q: What is the K-theory stability penalty conceptually?**

K-theory is a branch of mathematics (topology/algebra) that studies stable properties of structures under continuous deformations. ERBOT borrows the key intuition: a good clustering should be *stable* — small perturbations of the input graph should not change the clustering dramatically. The stability penalty computes a projected Laplacian PLP (where P = HHᵀ is the cluster indicator projection) and measures how much its top eigenvalues change when the graph is slightly noisy. Eigenvalue changes signal that cluster boundaries shifted. The penalty is added to the loss as a regularizer. Currently the penalty is not differentiated through (discrete clustering is not differentiable), so it serves as a monitoring signal rather than a gradient signal.

---

### Part G: Datasets and Benchmarks

**Q: Why does ERBOT use a sample from NCVoters? How is the sample drawn?**

The full NCVR file contains ~9 million records — too large for routine testing and development. Samples of 5k, 10k, or larger are drawn to make experiments manageable. The sample should preserve the entity group structure: if entity X has 4 records in the full data, ideally all 4 records appear in the sample, rather than stratified random sampling that might split groups. ERBOT uses `ncvr_read(n_records = 10000)` which draws a sequential slice from the file (or a configurable random sample). For rigorous benchmarking, entity-stratified sampling is recommended.

---

**Q: Why is CORA a useful benchmark?**

CORA is a citation network dataset with 1,879 records and ground-truth entity clusters (papers that are the same publication cited differently). Its small size (n = 1,879, ~1.76 million pairs without blocking) makes it feasible to run without blocking (`method = "none"`), which means blocking errors don't contaminate results. It has multiple fields (title, authors, venue, year, pages) with varying types of noise, making it a good test of multi-field similarity and weight learning.

---

### Part H: Software Design

**Q: Why is the data structure for pairs a tibble of (idx1, idx2) rather than record IDs?**

Row indices enable O(1) array lookup: `d[idx1, "name"]` is immediate without hash table lookup. Record IDs (strings or arbitrary integers) would require a join or dictionary lookup for every similarity computation. With millions of pairs, this overhead is significant. Indices also guarantee the upper-triangle convention (idx1 < idx2), avoiding duplicate pair computation.

---

**Q: What is dgCMatrix and why use sparse matrices?**

`dgCMatrix` is the Compressed Sparse Column (CSC) format from R's `Matrix` package. It stores only non-zero entries using three arrays: `x` (non-zero values), `i` (row indices), `p` (column start pointers). For n = 10,000 records with 200,000 candidate pairs (after blocking), the sparse matrix uses ~5 MB vs ~800 MB for a dense n×n matrix. Sparse arithmetic (matrix-vector products, etc.) is also much faster when most entries are zero.

---

**Q: What is the fallback operator `%||%` used for?**

`a %||% b` returns `a` if `a` is not `NULL`, otherwise returns `b`. It is the R equivalent of JavaScript's `??` operator or Python's `or` pattern. Used extensively in ERBOT for default parameter handling: `param <- user_value %||% default_value`. This pattern is cleaner than repeated `if (is.null(x)) x <- default` checks.

---

**Q: What does `er_data_profile()` do that `er_diagnose()` doesn't?**

`er_diagnose()` returns a structured R list and tibble with numeric summaries of each field — it is designed for downstream programmatic use by the pipeline. `er_data_profile()` takes those summaries and produces a human-readable multi-page PDF with charts (value distribution histograms, type bar charts, missingness bars) and a recommendations table. It bridges the gap between machine-readable diagnostics and a visual document you can share with collaborators or an advisor.

---

## 19. Glossary

| Term | Definition |
|------|-----------|
| **ARI** | Adjusted Rand Index; clustering agreement with truth, corrected for chance; range [-1, 1] |
| **B-cubed (B³)** | Per-record precision/recall metric; equal weight to every record regardless of cluster size |
| **Bimodality coefficient** | Sarle's BC; measures how bimodal (two-peaked) a distribution is |
| **Blocking** | Restricting pairwise comparisons to candidate pairs within groups to avoid O(n²) cost |
| **Both-null artifact** | Spurious sim = 1 when standard string metrics compare two missing (empty) values |
| **Candidate pair** | A pair (i,j) that passed blocking and is compared at the similarity stage |
| **Chromatic number χ** | Minimum colors needed to properly color a graph (no adjacent vertices share a color) |
| **Cluster** | A group of records predicted to refer to the same real-world entity |
| **Cluster ensemble** | Combining multiple clustering results into one final partition |
| **Co-membership** | Two records being in the same cluster |
| **Connected component** | Maximal subset of graph nodes where every pair has a path between them |
| **Consensus clustering** | Merging multiple clustering results by majority vote on co-membership |
| **Contrastive loss** | Loss that pushes match-pair embeddings together and non-match embeddings apart |
| **Cosine distance** | 1 - cosine similarity = 1 - (u·v)/(‖u‖ ‖v‖) |
| **Cosine similarity** | Dot product of unit-norm vectors; angle-based similarity |
| **Covariate shift** | Change in input feature distribution between training and deployment |
| **CPM** | Constant Potts Model; graph clustering quality: Σ_c[e_c - γ n_c(n_c-1)/2] |
| **Deduplication** | ER within a single dataset |
| **dgCMatrix** | Sparse compressed-column matrix format from R's Matrix package |
| **EM algorithm** | Expectation-Maximization; alternates E-step (hidden variable posteriors) and M-step (parameter updates) |
| **Entity** | A real-world object (person, paper, product) that may appear in multiple records |
| **Entity-disjoint split** | Train/test split where test entities are entirely absent from training |
| **Fellegi-Sunter** | Classical probabilistic record linkage model using field-level likelihood ratios |
| **Graph coloring** | Assigning colors to graph vertices so no two adjacent vertices share a color |
| **Homogeneity** | Each cluster contains only records from a single true entity |
| **IDF** | Inverse Document Frequency; down-weights common terms: log((N+1)/(df+1)) + 1 |
| **InfoNCE** | Information Noise-Contrastive Estimation; contrastive loss variant used in ERBOT |
| **irlba** | Implicitly Restarted Lanczos Bidiagonalization Algorithm; efficient truncated SVD |
| **Jaccard** | |A∩B|/|A∪B|; similarity of two sets |
| **Jaro** | String similarity based on matching characters and transpositions |
| **Jaro-Winkler** | Jaro + prefix bonus (p × ℓ × (1 - jaro)) |
| **K-theory** | Branch of topology; ERBOT borrows stability-under-deformation intuition |
| **k-NN** | k-Nearest Neighbors; the k points with smallest distance to a query point |
| **Laplacian** | L = D - W (diagonal degree matrix minus weighted adjacency) |
| **Leiden** | Graph community detection guaranteeing well-connected communities |
| **Levenshtein** | Edit distance: minimum insertions/deletions/substitutions between two strings |
| **Linkage criterion** | Rule for distance between clusters in hierarchical clustering |
| **Louvain** | Fast modularity-maximizing graph community detection |
| **MAR** | Missing At Random; missingness depends only on observed data |
| **MCAR** | Missing Completely At Random; missingness independent of all data |
| **Medoid** | Actual data point in a cluster minimizing total distance to all other members |
| **MNAR** | Missing Not At Random; missingness depends on the unobserved value |
| **Modularity Q** | Community structure quality: edge density within communities vs. chance |
| **NFKC** | Unicode normalization form; canonical decomposition + composition |
| **NMI** | Normalized Mutual Information; measures shared information between two clusterings; range [0,1] |
| **Pair completeness (PC)** | Fraction of true matches captured in the candidate pair set (blocking recall) |
| **PAM** | Partition Around Medoids; k-medoid clustering on a distance matrix |
| **Pairwise F-score** | F-score at the pair level; dominated by large clusters |
| **Projection matrix P** | P = HHᵀ; orthogonal projection onto cluster indicator subspace |
| **Record linkage** | ER across two or more datasets |
| **Reduction ratio (RR)** | Fraction of all pairs skipped by blocking |
| **Resolution limit** | Minimum community size below which modularity-based methods may incorrectly merge |
| **Resolution parameter γ** | Controls community granularity in Louvain/Leiden; higher → smaller communities |
| **Silhouette width** | Per-record cluster quality: s(i) = (b-a)/max(a,b); a = intra-cluster dist, b = nearest-cluster dist |
| **Sorted neighborhood (SN)** | Blocking: sort by key, compare all records within a sliding window of width w |
| **Sparse matrix** | Matrix storing only non-zero entries; efficient for large n with few candidate pairs |
| **Spectral embedding** | Low-dimensional representation derived from leading singular vectors of S |
| **SVD** | Singular Value Decomposition: S ≈ UΣVᵀ; top-d vectors give spectral embedding |
| **Temperature τ** | Contrastive loss parameter; low τ → sharp distribution, focus on hard negatives |
| **Threshold-CC** | Graph clustering: keep edges ≥ threshold, find connected components |
| **Transitivity** | If A=B and B=C then A=C; enforced as post-processing via transitive closure |
| **V-measure** | Harmonic mean of homogeneity and completeness |
| **VI** | Variation of Information: H(C|K) + H(K|C); a metric on the space of clusterings; VI=0 is perfect |
| **Ward.D2** | Hierarchical linkage minimizing increase in within-cluster sum of squares; requires Euclidean space |

---

*ERBOT v0.2.0 — End of Technical Reference*
