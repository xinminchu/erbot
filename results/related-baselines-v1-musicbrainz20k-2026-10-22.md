# Related-methods comparison v1 — MusicBrainz-20K (2026-10-22, Mas VM run)

Sample rebuilt on this VM by streaming replication of
build-musicbrainz20k.R (the monolithic builder OOMs at 7 GB RAM):
two streaming passes over the 2.3 GB Leipzig file + the builder's own
sampling lines (seed 20261020, whole-cluster). Fingerprint EXACT:
20,000 records / 10,294 entities / 16,934 pairs; source totals also
match (19,375,000 rows / 10,000,000 CID / 16,250,000 pairs).
Baseline PC here 0.9946 vs archived v2 0.9954 (delta ~5 pairs; R
version / tie handling on Sam's Windows build) — every v2 verdict
replicates (A -0.0022 vs archived -0.0010; B -0.0026 = archived).
Test rows 5,891 (88 duplicate-id rows dropped by the standard loader);
B_test = 219,882.

| Method | Pairs | PC |
|---|---|---|
| M0 fixed-k baseline (k*=50) | 219,882 | **0.9946** |
| M1 global top-B | 219,882 | 0.9924 |
| M6 marg-alloc A (Platt) | 219,141 | 0.9924 |
| M6 marg-alloc B (mixture) | 219,811 | 0.9920 |
| M2 meta-blocking JS aligned@B | 219,882 | 0.9911 |
| M2 meta-blocking CBS aligned@B | 219,882 | 0.9899 |
| M2 meta-blocking WNP native | 97,406 | 0.9804 |
| M3 adaptive-k (alpha*=0.55) | 231,478 native | 0.9889 |
| M5 canopy (T2*=0.3121) | 56,721 native / aligned@B | 0.9848 / 0.9924 |
| M4 sorted neighborhood (win*=40) | 228,969 | 0.2075 |

Readings: near-ceiling regime — the plain fixed-k baseline is the best
method on the table; every alternative ties or loses. Sorted
neighborhood collapses (0.2075): one lexicographic key cannot group
music metadata duplicates. WNP's native efficiency (0.9804 at 44% of
budget) is the only meta-blocking bright spot anywhere.
