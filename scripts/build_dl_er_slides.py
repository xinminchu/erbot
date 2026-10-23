#!/usr/bin/env python3
"""Build the traditional PPTX deck: Deep Learning for Entity Resolution literature review.
Output: ~/workspace/your_files/deep-learning-entity-resolution-slides-2026-10-02.pptx
Design: white background + light gray accents (no dark blocks)."""

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR, MSO_AUTO_SIZE

INK = RGBColor(0x1F, 0x24, 0x2B)
GRAY = RGBColor(0x6B, 0x72, 0x80)
LIGHT = RGBColor(0xF3, 0xF4, 0xF6)
ACCENT = RGBColor(0x1D, 0x4E, 0x89)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)

prs = Presentation()
prs.slide_width = Inches(13.33)
prs.slide_height = Inches(7.5)
BLANK = prs.slide_layouts[6]


def base(slide, footer="September 2026"):
    bg = slide.background
    fill = bg.fill
    fill.solid()
    fill.fore_color.rgb = WHITE
    # thin top accent bar
    bar = slide.shapes.add_shape(1, Inches(0), Inches(0), prs.slide_width, Inches(0.06))
    bar.fill.solid()
    bar.fill.fore_color.rgb = ACCENT
    bar.line.fill.background()
    # footer
    tf = slide.shapes.add_textbox(Inches(0.6), Inches(7.0), Inches(12), Inches(0.4)).text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = footer
    p.font.size = Pt(12)
    p.font.color.rgb = GRAY
    p.font.name = "Calibri"


def add_title(slide, text, size=34):
    tf = slide.shapes.add_textbox(Inches(0.6), Inches(0.35), Inches(12.1), Inches(1.1)).text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = Pt(size)
    p.font.bold = True
    p.font.color.rgb = INK
    p.font.name = "Calibri"
    return Inches(1.6)


def add_subtitle(slide, text):
    tf = slide.shapes.add_textbox(Inches(0.6), Inches(1.15), Inches(12.1), Inches(0.7)).text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = Pt(20)
    p.font.color.rgb = GRAY
    p.font.name = "Calibri"


def est_lines(text, size, width_in):
    """Deliberately generous estimate (assume wide chars) so PowerPoint
    never needs to expand a box at render time (which caused overlaps)."""
    cpl = max(20, int(width_in / (size / 72.0 * 0.52)))
    lines, cur = 1, 0
    for w in text.split():
        if cur + len(w) + 1 > cpl:
            lines += 1
            cur = len(w)
        else:
            cur += len(w) + 1
    return lines


def plan_bullets(items, size, top=1.7, gap_after=0.16, width=11.9):
    """Pre-compute layout; returns (plan, bottom) without creating shapes."""
    y = top
    plan = []
    for item in items:
        bold_head, rest = (item[0], item[1]) if isinstance(item, tuple) else ("", item)
        lines = est_lines(bold_head + rest, size, width)
        line_h = size / 72.0 * 1.45  # margin over 1.3 line spacing
        box_h = lines * line_h + 0.18
        plan.append((bold_head, rest, box_h))
        y += box_h + gap_after
    return plan, y - gap_after


def add_bullets(slide, items, top=1.7, size=19, gap_after=0.16):
    plan, _ = plan_bullets(items, size, top=top, gap_after=gap_after)
    y = top
    for bold_head, rest, box_h in plan:
        box = slide.shapes.add_textbox(Inches(0.7), Inches(y), Inches(11.9), Inches(box_h))
        tf = box.text_frame
        tf.word_wrap = True
        tf.auto_size = MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT  # expand if estimate was low
        p = tf.paragraphs[0]
        p.line_spacing = 1.3
        p.space_after = Pt(4)
        if bold_head:
            r = p.add_run()
            r.text = bold_head
            r.font.bold = True
            r.font.size = Pt(size)
            r.font.color.rgb = INK
            r.font.name = "Calibri"
        r = p.add_run()
        r.text = rest
        r.font.size = Pt(size)
        r.font.color.rgb = INK
        r.font.name = "Calibri"
        # bullet glyph
        bp = slide.shapes.add_textbox(Inches(0.35), Inches(y + 0.03), Inches(0.3), Inches(0.4))
        bpp = bp.text_frame.paragraphs[0]
        bpp.text = "•"
        bpp.font.size = Pt(size)
        bpp.font.color.rgb = ACCENT
        bpp.font.name = "Calibri"
        y += box_h + gap_after
    return y


def content_slide(title, items, footer="September 2026", size=19, gap_after=0.45):
    s = prs.slides.add_slide(BLANK)
    base(s, footer)
    add_title(s, title)
    # auto-shrink font until the planned layout fits above the footer
    # (note: plan_bullets returns bottom in inches, not EMU)
    sz = size
    while sz > 15:
        _, bottom = plan_bullets(items, sz, gap_after=gap_after)
        if bottom <= 6.85:
            break
        sz -= 1
    add_bullets(s, items, size=sz, gap_after=gap_after)
    return s


def title_slide(title, subtitle):
    s = prs.slides.add_slide(BLANK)
    base(s, "")
    tf = s.shapes.add_textbox(Inches(0.6), Inches(2.2), Inches(12.1), Inches(1.6)).text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = title
    p.font.size = Pt(44)
    p.font.bold = True
    p.font.color.rgb = INK
    p.font.name = "Calibri"
    tf2 = s.shapes.add_textbox(Inches(0.6), Inches(3.9), Inches(12.1), Inches(1.0)).text_frame
    tf2.word_wrap = True
    p2 = tf2.paragraphs[0]
    p2.text = subtitle
    p2.font.size = Pt(24)
    p2.font.color.rgb = GRAY
    p2.font.name = "Calibri"


# ---------------- slides ----------------
title_slide("Deep Learning for Entity Resolution",
            "A Literature Review  •  September 2026")

content_slide("Three Waves of Deep Learning in ER", [
    ("Wave 1 (2017–2019) — Learned representations. ",
     "DeepER and DeepMatcher replace handcrafted similarity features with learned tuple/attribute representations (RNNs over word embeddings)."),
    ("Wave 2 (2020–2022) — Pretrained LMs + learned blocking. ",
     "Ditto reframes matching as sequence-pair classification (up to ~+30% F1); AutoBlock / DeepBlocker make dense, label-free candidate generation practical."),
    ("Wave 3 (2022–2026) — LLMs. ",
     "From “train a matcher” to “prompt a matcher”: few-shot GPT-3 beats fine-tuned Ditto on 4 of 7 datasets."),
    ("Takeaway for direction choice. ",
     "Pairwise matcher accuracy on small benchmarks is saturated. The frontier moved to systems questions — blocking, scale, cost."),
])

content_slide("Wave 1a: DeepER (Ebraheem et al., PVLDB 2018)", [
    "Compose each tuple into a distributed vector with uni-/bi-directional LSTMs over word embeddings (GloVe or task-tuned).",
    "Score tuple similarity on vectors — no handcrafted features.",
    "Blocking: LSH over tuple vectors, using all attributes instead of a few hand-picked blocking keys.",
    "Outperformed prior solutions on benchmark, biomedical, and multilingual datasets, with less labeled data.",
])

content_slide("Wave 1b: DeepMatcher (Mudgal et al., SIGMOD 2018)", [
    ("Design space. ",
     "Attribute embedding → summarization → alignment → aggregation → classification, shipped as an open-source package inside the Magellan workflow."),
    ("Findings. ",
     "Deep models beat classical ML (RF/SVM over similarity features) on textual and dirty data, but need large labeled sets and struggle on structured/numeric attributes; hybrid neural + classical is strongest."),
    ("Side effect. ",
     "Standardized the Magellan benchmark datasets as the field’s evaluation suite."),
])

content_slide("Wave 2a: Ditto (Li et al., PVLDB 2020) — the strong baseline", [
    "Cast EM as sequence-pair classification: serialize record as “COL attr VAL value …”, concatenate pair with [SEP], fine-tune BERT/RoBERTa cross-encoder.",
    "Three optimizations: domain-knowledge injection (highlight numbers/IDs/names), TF-IDF summarization of long strings, label-preserving data augmentation.",
    ("Results. ",
     "Up to ~30% F1 over prior SOTA; matches prior SOTA with ≤ ½ the labels; real 789K × 412K company task: 96.5% F1, with blocking giving 3.8× end-to-end speedup."),
    "Every later matcher paper compares against Ditto — “beat Ditto on Abt-Buy” is a hard sell today.",
])

content_slide("Benchmarks: why the classic suite is saturated", [
    "Magellan suite (the de-facto eval since 2018): Amazon-Google 11,460 pairs; DBLP-ACM 12,363; DBLP-Scholar 28,707; Walmart-Amazon 10,242; Abt-Buy 9,575; iTunes-Amazon just 539.",
    "Larger successors: WDC Training (26M offers, 20.7M positive pairs, WWW 2019); WDC Products (multi-dimensional robustness, 2023); Machamp (heterogeneous schemas, CIKM 2021).",
    "The classic suite is tiny and overused — near-saturation and overfitting are real concerns.",
    "Cross-paper absolute F1 numbers should not be compared naively (different splits/preprocessing); only within-paper deltas are meaningful.",
])

content_slide("Wave 2b: Learned Blocking — the recipe", [
    "Blocking has opposite economics from matching: very high recall (a missed match is unrecoverable) at extreme efficiency — and its output prices the matcher.",
    ("AutoBlock (WSDM 2020). ",
     "Similarity-preserving representation learning + NN search; sub-quadratic; no blocking-key engineering."),
    ("DeepBlocker (PVLDB 2021). ",
     "Design space of 8 self-supervised deep blockers (no labels) + ANN search — the label-free dense-blocking recipe."),
])

content_slide("Wave 2b: Learned Blocking — recent advances", [
    ("SC-Block (ESWC 2024). ",
     "Supervised contrastive learning + FAISS/HNSW; candidate sets ~50% smaller, pipelines 1.5–4× faster, no F1 loss; new WDC-Block benchmark (200B max pairs)."),
    ("UniBlocker (arXiv 2024). ",
     "Universal dense blocker — contrastive pretraining on generic tabular data, no per-domain tuning; complementary to sparse blocking (union improves recall)."),
    ("Consensus. ",
     "Dense buys semantic recall (synonyms, paraphrase); sparse is cheap and strong on surface overlap — run both, union them."),
])

content_slide("Label Efficiency: attacking the annotation bottleneck", [
    ("ZeroER (SIGMOD 2020). ",
     "Zero labels: fit a GMM over similarity vectors to separate matches/unmatches; comparable to supervised on 5 benchmarks."),
    ("Transfer + active learning (Kasai et al., ACL 2019). ",
     "Learn in a high-resource setting, adapt with a few actively selected examples — order of magnitude fewer labels."),
    ("Sudowoodo (ICDE 2023). ",
     "Contrastive self-supervised pretraining; one shared embedder serves both the blocker (NN retrieval) and the matcher."),
    ("DADER (SIGMOD 2022). ",
     "Domain adaptation from labeled source ER datasets to unlabeled targets (six DA strategies)."),
])

content_slide("Wave 3a: Prompting LLMs for Matching", [
    ("Narayan et al. (PVLDB 2022). ",
     "GPT-3 few-shot beat fully fine-tuned Ditto on 4 of 7 EM datasets with a handful of demonstrations."),
    ("Peeters & Bizer (ADBIS 2023). ",
     "Zero-shot ChatGPT reached 82.35% F1 where RoBERTa needed 2,000 training examples; similarity-based demo selection +7.85%."),
    ("MatchGPT (EDBT 2025). ",
     "Best LLMs rival fine-tuned PLMs with zero/few examples; key edge is robustness to unseen entities (fine-tuned PLMs memorize training entities); no single best prompt — tune per model × dataset; GPT-4 can diagnose labeling errors."),
])

content_slide("Wave 3b: Small Fine-tuned LLMs — the cost answer", [
    ("AnyMatch (arXiv 2024). ",
     "Zero-shot EM with a fine-tuned GPT-2 (124M) + smart data selection: 2nd-highest overall F1 across 9 benchmarks vs 13 baselines, within 4.4% of MatchGPT/GPT-4 at 3,899× lower inference cost."),
    ("Jellyfish (EMNLP 2024). ",
     "Instruction-tuned open models (7B–13B) on data-prep tasks incl. EM; competitive with GPT-3.5/4, runs on a single local GPU — the self-hostable, privacy-preserving matcher archetype."),
])

content_slide("Beyond Pairwise Calls — and LLM Blocking", [
    ("LLM-CER (arXiv 2025). ",
     "LLM performs in-context clustering over record sets: next-record-set creation, misclustering guardrail, transitivity-aware merge."),
    ("Retrieve–Match–Escalate (2026). ",
     "Production cascade: per-record top-20 retrieval, cheap cross-encoder routes by confidence, only the ambiguous band escalates to an agentic VLM; blocking/recall explicitly bounds the funnel."),
    ("Consensus on LLM blocking. ",
     "LLMs are prohibitively expensive for blocking (quadratic pairs × per-pair cost) — use them selectively post-blocking."),
    ("Shah et al. (Algorithms 2025). ",
     "Blocking + fine-tuned LLaMA3/BERT improves both runtime and F1 over LLM-only linkage. Blocking remains critical in the LLM era."),
])

content_slide("Scalability: record-level vs pair-level blocking", [
    "Candidate generation prices the matcher: with cross-encoder / LLM matchers, per-pair inference dominates — candidate-set size (and recall) is the scaling lever.",
    ("Record-level. ",
     "Per record, retrieve a bounded top-k (e.g., ANN k-NN over tuple embeddings). Volume O(n·k) — predictable, index-friendly, with a recall-vs-cost knob (k)."),
    ("Pair-level. ",
     "Emit candidate pairs directly (e.g., all pairs co-occurring in key-based blocks). Volume is hard to bound and explodes on skewed blocks."),
])

content_slide("Scalability: why the distinction matters", [
    ("Why it matters. ",
     "Expensive matchers need bounded, per-record candidate budgets — record-level maps cleanly onto ANN infrastructure; pair-level enumeration has no such knob."),
    "Standard metrics: Pairs Completeness (recall), Reduction Ratio, Pairs Quality (Papadakis et al., ACM CSUR 2020).",
    "Blocking recall upper-bounds pipeline recall — a missed match here is unrecoverable downstream.",
])

content_slide("Graph NNs: a smaller, specialized thread", [
    "Collective signals (transitivity, shared attributes/neighbors) that pairwise classifiers ignore.",
    ("HierGAT (SIGMOD 2022). ",
     "Token → attribute → entity graph attention; attention surfaces discriminative tokens per decision."),
    ("GraLMatch (EDBT 2025). ",
     "Post-hoc graph correction: build a graph from Ditto’s predictions, remove false positives via transitivity constraints."),
    ("GraphER (arXiv 2024). ",
     "Hybrid rule + learning — graph differential dependencies guide GNN representation learning; best on graph ER, competitive on relational."),
])

content_slide("Open Problems (1 of 2)", [
    "Blocking for the LLM era — bounded per-record candidate sets for expensive matchers; learned sparse–dense hybrid blocking.",
    "Universal / zero-config blocking — recall guarantees, automatic configuration, principled fusion of sparse and dense signals.",
    "Realistic large benchmarks — WDC-Block is a start; missing: dirty multilingual, temporal, multimodal ER; evaluation on realistic post-blocking distributions.",
    "Collective ER at scale — joint inference over clusters with guarantees, not post-hoc patches.",
])

content_slide("Open Problems (2 of 2)", [
    "Cost, calibration, trust — per-pair LLM cost/throughput, prompt sensitivity, score calibration, privacy (hosted vs local), data contamination.",
    "Hard cases — jargon-heavy domains, schema drift, identifier-heavy records where semantic models underperform symbolic ones.",
    "Fairness/bias — blocking keys and embeddings vs protected attributes: essentially unstudied.",
])


def table_slide(title, headers, rows):
    s = prs.slides.add_slide(BLANK)
    base(s)
    add_title(s, title)
    tbl_shape = s.shapes.add_table(len(rows) + 1, 2, Inches(0.6), Inches(1.7),
                                   Inches(12.1), Inches(4.9))
    tbl = tbl_shape.table
    tbl.columns[0].width = Inches(5.6)
    tbl.columns[1].width = Inches(6.5)
    for j, h in enumerate(headers):
        cell = tbl.cell(0, j)
        cell.text = ""
        p = cell.text_frame.paragraphs[0]
        p.text = h
        p.font.bold = True
        p.font.size = Pt(18)
        p.font.color.rgb = WHITE
        p.font.name = "Calibri"
        cell.fill.solid()
        cell.fill.fore_color.rgb = ACCENT
    verdicts = {
        "Saturated": RGBColor(0xB9, 0x1C, 0x1C),
        "Crowded": RGBColor(0xB4, 0x53, 0x09),
        "Open, high-impact": RGBColor(0x16, 0x65, 0x2E),
        "Open": RGBColor(0x16, 0x65, 0x2E),
        "Partially open": RGBColor(0x1D, 0x4E, 0x89),
    }
    for i, (area, verdict, why) in enumerate(rows, start=1):
        c0 = tbl.cell(i, 0)
        c0.text = ""
        p = c0.text_frame.paragraphs[0]
        p.text = area
        p.font.size = Pt(16)
        p.font.color.rgb = INK
        p.font.name = "Calibri"
        c0.text_frame.paragraphs[0].space_after = Pt(2)
        q = c0.text_frame.add_paragraph()
        q.text = verdict
        q.font.size = Pt(16)
        q.font.bold = True
        q.font.color.rgb = verdicts.get(verdict, INK)
        q.font.name = "Calibri"
        c1 = tbl.cell(i, 1)
        c1.text = ""
        p = c1.text_frame.paragraphs[0]
        p.text = why
        p.font.size = Pt(16)
        p.font.color.rgb = INK
        p.font.name = "Calibri"
        for c in (c0, c1):
            c.fill.solid()
            c.fill.fore_color.rgb = LIGHT if i % 2 else WHITE
            c.vertical_anchor = MSO_ANCHOR.MIDDLE
    return s


table_slide("Saturated vs Open", ["Sub-area", "Assessment"],
    [
        ("Supervised pairwise matching on Magellan-size benchmarks",
         "Saturated",
         "Ditto (2020) is a strong, simple ceiling; datasets tiny and overused."),
        ("LLM prompting recipes for pairwise matching",
         "Crowded",
         "Headline findings established 2023–2025; needs a new angle to stand out."),
        ("Learned dense blocking (self-supervised + ANN)",
         "Open, high-impact",
         "Recall–cost tradeoffs unsettled; universal blockers new; blocking for LLM matchers wide open."),
        ("Scaling ER end-to-end",
         "Open",
         "Cost models, cascades, large-scale evaluation just emerging."),
        ("Collective / graph-structured ER",
         "Open",
         "Young theory, young methods."),
        ("Label-efficient ER",
         "Partially open",
         "Active learning for blocking config underexplored."),
        ("Trustworthy ER (calibration, fairness, privacy)",
         "Open",
         "Barely studied; growing in importance."),
    ])

content_slide("Recommendation: choose Scaling / Blocking", [
    "A scaling/blocking paper has clearer headroom, a more direct industry audience, and composes with — rather than competes against — every matcher advance.",
    "The more expensive matchers get (LLMs), the more valuable blocking becomes: it prices the whole pipeline.",
    "If going the matcher route anyway, aim at an unsolved axis: OOD / unseen entities, identifier-heavy domains, multimodality, calibration — not another point of F1 on DBLP-ACM.",
    "Required: method innovation, not benchmarking — e.g., principled recall guarantees for learned blocking, cost-aware cascades, universal blockers with bounded per-record budgets.",
])

content_slide("Selected References (1 of 2)", [
    "Ebraheem et al., DeepER — PVLDB 2018;  Mudgal et al., DeepMatcher — SIGMOD 2018",
    "Li et al., Ditto — PVLDB 2020;  Tu et al., DADER — SIGMOD 2022",
    "Zhang et al., AutoBlock — WSDM 2020;  Thirumuruganathan et al., DeepBlocker — PVLDB 2021",
    "Wu et al., ZeroER — SIGMOD 2020;  Kasai et al. — ACL 2019;  Wang et al., Sudowoodo — ICDE 2023",
    "Narayan et al., “Can Foundation Models Wrangle Your Data?” — PVLDB 2022",
], size=17)

content_slide("Selected References (2 of 2)", [
    "Peeters & Bizer, “Using ChatGPT for Entity Matching” — ADBIS 2023;  Peeters et al., MatchGPT — EDBT 2025",
    "Brinkmann et al., SC-Block — ESWC 2024;  Wang et al., UniBlocker — arXiv:2404.14831",
    "Shah et al., “The Critical Role of Blocking” — Algorithms 2025",
    "Surveys: Barlaug & Gulla (TKDD 2021);  Papadakis et al. (CSUR 2020)",
], size=17)

out = "/home/hatch/workspace/your_files/deep-learning-entity-resolution-slides-2026-10-02.pptx"
prs.save(out)
print("saved:", out)
