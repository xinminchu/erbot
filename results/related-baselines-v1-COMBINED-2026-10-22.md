# Related-methods comparison v1 — COMBINED (2026-10-22)

One protocol (v2 split seed 20261016, entity-disjoint 70/30, top-200
pool, budget B = fixed-k baseline pair count on test, PC at B) across
Cora, D10K (JedAI-official reconstruction, gold fingerprint exact),
MusicBrainz-20K (builder sample rebuilt, fingerprint exact).
Harness self-check on Cora reproduces archived v2 numbers exactly.

## PC at equal budget B (native-pair exceptions noted per file)
| Method | Cora (B=20,898) | D10K (B=40,825) | Music (B=219,882) |
|---|---|---|---|
| M0 fixed-k | 0.5865 | 0.9996 | **0.9946** |
| M1 global top-B | **0.9227** | 0.9996 | 0.9924 |
| M6 marg-alloc A | 0.9092 | 0.9996 | 0.9924 |
| M6 marg-alloc B | 0.9092 | 0.9992 | 0.9920 |
| M3 adaptive-k | 0.9173 | 0.9996 | 0.9889 |
| M2 meta CBS | 0.5135 | 0.9935 | 0.9899 |
| M2 meta JS | 0.5135 | 0.9906 | 0.9911 |
| M4 sorted nbhd | 0.5279 | 0.6851 | 0.2075 |
| M5 canopy native | 0.2007 (4,517 pr) | **1.0000 (5,668 pr)** | 0.9848 (56,721 pr) |
| M2 WNP native | 0.1790 (4,373 pr) | 0.9471 (7,772 pr) | 0.9804 (97,406 pr) |

## Conclusions (descriptive; no permutation run for the baselines)
1. Scope law confirmed from the other side: where the baseline can
   saturate (D10K, Music), NO method — classic or ours — beats it;
   where it cannot (Cora), allocation-style ranking gains +0.32..+0.34.
2. In the one open regime, plain global ranking by cosine beats the
   calibrated waterfill (+0.0135) and adaptive-k sits between them.
   Calibration and per-record floors add nothing on Cora; the v2
   permutation result (allocation profile >> shuffled profile) is
   untouched, but "calibrated marginal allocation is the best
   allocator" does NOT survive this comparison.
3. Meta-blocking never wins at equal budget on any dataset; on Cora
   its token graph cannot even fill the budget (15,009 edges < B).
4. Sorted neighborhood is uniformly weak, catastrophically on Music.
5. Side-finding: canopy is the efficiency champion on near-duplicate
   data (D10K: PC 1.0000 at 14% of budget; Music WNP: 0.9804 at 44%).
