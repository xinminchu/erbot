# Paper direction brief — 2026-10-01

Decision needed at the 2026-10-02 meeting. This brief states both directions
fairly, then gives a recommendation. The decision is Sam's and the advisor's.

## Direction A — Scaling: blocking / candidate-space method innovation

**Core question.** How should candidate generation work when n is too large
for all-pairs comparison — and what exactly is being approximated?

**Where it connects to existing work.**

- The Restaurant correction already showed that the candidate universe is part
  of the task definition (mechanism card 7); blocking recall reported without
  the selection procedure invites misreading.
- ERBOT already has blocking (`er_block`: standard / prefix / sorted
  neighbourhood), block statistics, and a scaling-relevant pipeline shape.
- The advisor's own warning is the sharpest formulation of the research gap:
  **record-level blocking and pair-level blocking are not the same object.**
  Record-level blocking partitions records; pair-level blocking decides which
  pairs survive. Much of the literature evaluates the first and reports the
  second.

**What a contribution would have to be.** Not another benchmark table. A
method-level idea, for example: a pair-level blocking criterion with a
recall/precision trade-off that can be estimated *without* gold, or a blocking
scheme whose candidate budget adapts to local density with a measurable
failure mode. The v3 study supplies the evaluation discipline (entity-disjoint
splits, tie-aware selection, structural diagnostics) to evaluate it honestly.

**Costs and risks.**

- Lowest setup cost of the two directions; reuses ERBOT directly.
- Sits closest to the advisor's expertise, so supervision and reviewing are
  strongest here.
- Risk: the space is well-trodden; the novelty bar is "method innovation", and
  the record-level vs pair-level distinction must be made precise early or the
  project drifts into benchmarking.

## Direction B — Deep learning: embeddings / LLM-based entity resolution

**Core question.** Can learned representations (embeddings, LLM judgements)
resolve entities where hand-crafted comparison features are too weak?

**Where it connects to existing work.**

- Affiliation is the motivating failure: one weak comparison feature fails in
  both directions at once (mechanism card 6). An embedding view is a concrete
  remedy candidate.
- ERBOT already has neural hooks (`R/20-embed.R`, `R/42-neural_encoder.R`,
  DeepMatcher / Ditto baseline wrappers), but they are not the centre of the
  current study.

**What a contribution would have to be.** An embedding/LLM component evaluated
under the same honesty discipline as the rest of ERBOT (entity-disjoint
selection, natural-prevalence Test, structural diagnostics), with a clear
statement of where it beats the feature-based pipeline and where it does not —
for example on weak-feature datasets such as Affiliation — rather than a
leaderboard claim.

**Costs and risks.**

- Higher setup cost: representation pipeline, compute, and a new evaluation
  surface all have to be built and defended.
- The advisor has said this is open territory for him too, so supervision is
  thinner and the reviewing risk is higher.
- Risk: "we applied an LLM to ER" is not a contribution; the contribution has
  to be a mechanism (when does the learned view help, when does it over-merge?)
  stated sharply enough to be falsifiable.

## Side-by-side

| | A. Scaling / blocking | B. Deep learning / embedding / LLM |
|---|---|---|
| Continuity with current ERBOT work | Direct (blocking, candidate space, Card 7) | Partial (neural hooks exist; Affiliation motivates it) |
| Advisor expertise / supervision | Strong | Self-described open territory |
| New machinery to build | Little | Substantial |
| Novelty requirement | Method innovation, not benchmarking | A falsifiable mechanism, not an application report |
| Main risk | Drifting into a benchmark comparison | High setup cost + weaker supervision + hype exposure |
| Time to first defensible result | Shorter | Longer |

## Recommendation

**Primary: Direction A (scaling / blocking).** It continues the strongest
existing asset — the candidate-space work and the evaluation discipline around
it — needs the least new machinery, and sits where the advisor can supervise
and review most effectively. The record-level vs pair-level blocking
distinction gives it a precise, ownable question.

Keep Direction B warm with one cheap probe: run an embedding view on
Affiliation (the weakest-feature dataset) under the existing protocol. If the
probe shows a large, mechanistically explainable gain, B becomes a credible
second paper or a pivot; if not, nothing is lost.

**For tomorrow:** ask the advisor to confirm A as the primary direction (or
override to B), and confirm the Item 3 positive-ratio design, since the split
discipline it defines will underwrite either direction's experiments.
