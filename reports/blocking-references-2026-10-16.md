# Blocking 参考文献（按与你工作的相关度排序）

整理：2026-10-15。带"为什么要读"——每篇都连到你的三个创新或 E1/E2。

---

## 必读（5 篇，直接决定周五发言质量）

1. **CorpFam** — Gupta, arXiv:2609.04269 (2026-09, preprint)
   https://arxiv.org/abs/2609.04269
   为什么读：开场弹药。54,864 对上 7 种 blocking 全军 <3% recall，93.2% 真实链接进不了候选集。"blocking 才是瓶颈"的硬数字。

2. **Progressive ER** — Maciejewski et al., arXiv:2503.08298 (2025-03)
   https://arxiv.org/abs/2503.08298
   为什么读：E2（adaptive-k）的方法论现货。filtering→weighting→scheduling→matching 框架 + 18 数据集评测，直接套用。

3. **UniBlocker** — Wang et al., arXiv:2404.14831 (2024-04)；代码 https://github.com/tshu-w/uniblocker
   https://arxiv.org/abs/2404.14831
   为什么读：自监督通用 dense blocker（零逐数据集训练）；Table 4 的 sparse+dense ensemble 结论支撑"多通道 union"论点。

4. **Mugeni & Amagasa** — kNN 图 + 社区检测 blocking（~2023，经 SC-Block 引文找到）
   入口：https://www.researchgate.net/publication/369035298_SC-Block_Supervised_Contrastive_Blocking_within_Entity_Resolution_Pipelines
   为什么读：E1 的直接前人。读完确认三点差异化（MinHash vs transformer embedding、无跨社区保险、无预算对齐对照）站得住。

5. **pBlocking** — Galhotra et al., VLDB Journal (2021)
   https://arxiv.org/pdf/2005.14326
   为什么读：唯一带 no-gold recall 定理的 blocking 论文。创新三（审计）的"前人只做到这"基准线。

---

## 选读（与创新点逐个相关）

6. **LLM-CER** — Fu et al., arXiv:2506.02509 (2025-06)
   https://arxiv.org/abs/2506.02509
   相关：LLM 时代 blocking 被降格为"种子步骤"的代表作；9 数据集、5× API 调用 reduction。

7. **NLSHBlock** — Wang et al., arXiv:2401.18064 (2024-01)
   https://arxiv.org/abs/2401.18064
   相关：学 hash 函数而非 embedding；注意它要 labeled pairs（非无标）。

8. **ER in Practice** — Pavani et al., arXiv:2607.26298 (2026-07)
   https://arxiv.org/abs/2607.26298
   相关：6 benchmarks bake-off；"只有多样并行 blocking 能提 recall"（HNSW ensemble +0.4pp，identifier 索引 +0.3pp）。

9. **Shah et al.** — "Efficient Record Linkage in the Age of LLMs: The Critical Role of Blocking", Algorithms 2025, 18, 723
   https://www.mdpi.com/1999-4893/18/11/723
   相关：论点"LLM 时代 blocking 决定 pipeline 上限"——给你做 blocking 的正当性背书。

10. **AgenticER** — Papadakis et al., arXiv:2607.27435 (2026-07, vision paper，无实验)
    https://arxiv.org/abs/2607.27435
    相关：blocker 组合的 agent 视角；vision 篇，了解方向即可。

11. **DistillER** — Zeakis et al., arXiv:2602.05452 (EDBT 2026)
    https://arxiv.org/abs/2602.05452
    相关：LLM 蒸馏到 1–3B 小模型的方法——以后你想给 exclusion 谓词学生模型时用得上。

12. **SC-Block** — arXiv:2303.03132 (2023)
    https://arxiv.org/pdf/2303.03132v2
    相关： supervised contrastive blocking；主要是找 Mugeni & Amagasa 引文的入口。

---

## 背景（有空再看）

13. **BlockingPy** — arXiv:2504.04266 (2025)：GPU FAISS ANN blocking 工程实现，recall ~0.83。
    http://arxiv.org/pdf/2504.04266
14. **MERAI** — arXiv:2508.03767 (2025)：15.7M 记录企业级 pipeline。
    https://arxiv.org/pdf/2508.03767v1
15. **STEPMatch / EM-Join** — ICEIS 2025：跨语言产品链接、embedding similarity join。
    https://www.scitepress.org/Link.aspx?doi=10.5220/0013285000003929
16. **Streaming ER with embeddings** — Springer chapter (2026)：增量图 + embedding 更新。
    https://link.springer.com/chapter/10.1007/978-3-032-02215-8_8
17. **Transformer-ER scoping review** — MDPI (2026)：155 篇综述，55% 来自 2025–26。
    https://www.mdpi.com/2078-2489/17/9/917

---

*注：以上为 2026-10-15 索引级调研（摘要/摘录），非逐篇精读；正式引用前请核对原文。原始调研笔记在 `research_notes/erbot-blocking-new-strategies-20261015-2005/`。*
