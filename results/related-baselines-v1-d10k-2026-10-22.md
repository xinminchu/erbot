# Related-methods comparison v1 — D10K (2026-10-22, Mas VM run)

## Data reconstruction note (read first)
The lab CSVs (10Kfull.csv / 10Kduplicates.csv) exist only on Sam's PC.
Reconstructed here from JedAI's official JSO distribution
(Zenodo 10.5281/zenodo.6950980, synDatasets.tar.gz): 10Kprofiles +
10KIdDuplicates. Verified: 10,000 records / 8,705 pairs / 6,000
entities — fingerprint EXACT. Id mapping established empirically:
duplicates reference 0-based indices into the profile list (content
check: mean shared attribute values 4.73 vs 0.18 for 1-based), and
the pair-derived partition is identical to the JSO entityUrl groups.
Aggregate Value = 11 Febrl attribute values, canonical field order,
space-joined.
Residual divergence vs the lab copy: the lab's tuning picked k*=50
(B_test 101,201, baseline PC 1.0000); this reconstruction picks k*=20
(train PCs 0.9997/0.9998/0.9998) giving B_test 40,825, baseline PC
0.9996. The gold and the qualitative picture are identical; absolute
pair counts are reconstruction-specific. All methods below are
compared on THIS copy under the same split/pool/budget protocol.

## Results (test rows 2,948; B_test = 40,825)
| Method | Pairs | PC |
|---|---|---|
| M0 fixed-k baseline (k*=20) | 40,825 | 0.9996 |
| M1 global top-B | 40,825 | 0.9996 |
| M6 marg-alloc A (Platt) | 40,215 | 0.9996 |
| M6 marg-alloc B (mixture) | 40,825 | 0.9992 |
| M3 adaptive-k (alpha*=0.75) aligned@B | 40,825 | 0.9996 (native 30,259 / 0.9117) |
| M5 canopy (T2*=0.265) native | **5,668** | **1.0000** |
| M2 meta-blocking CBS aligned@B | 40,825 | 0.9935 |
| M2 meta-blocking JS aligned@B | 40,825 | 0.9906 |
| M2 meta-blocking WNP native | 7,772 | 0.9471 |
| M4 sorted neighborhood (win*=20) | 55,822 | 0.6851 |

## Readings
1. Ceiling, as in v2: every allocation variant ties at ~0.9996. Dead.
2. Canopy is the efficiency story: PC 1.0000 with 5,668 pairs (14% of
   budget) — Febrl near-copies form tight canopies. It changes nothing
   at B, but on a cost-per-pair view it dominates this dataset.
3. Meta-blocking is respectable here (dense graph, 164,991 edges) yet
   still slightly BELOW the fixed-k baseline at equal budget.
4. Sorted neighborhood fails on D10K too (0.6851 despite more pairs).

## Addendum: flattening variants (checked 2026-10-22)
Three Aggregate Value variants were scored at train level against the
lab fingerprints (k*=50; test pairs @k50 = 101,201):
- V1 space-joined, canonical order (USED): trainPC .9997/.9998/.9998,
  test pairs @k50 = 101,800 (Δ +599)
- V4 no-separator join: trainPC .9995/.9998/.9998, pairs 101,305 (Δ +104)
- V2 tokens sorted alphabetically: trainPC .9998/1.0000/1.0000, pairs
  100,866 (Δ −335)
No variant makes k=50 the unique train winner, so the lab copy likely
differs in some further detail (join + row order + tie handling).
Residual is ≤0.6% of pair count with identical gold; V1 kept and the
comparison above stands as documented.
