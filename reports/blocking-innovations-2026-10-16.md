# Blocking 新策略：三个原创方向（周五组会报告稿）

日期：2026-10-16（周五组会）| 状态：构思 + 实验设计完成，代码待写
| 英文摘要见文末，可直接贴 slides |

---

## 一页总览

E1（集体阻塞）原型已写完，待 Windows 跑数。在此之外，本次提出**三个新的原创方向**，统一在一句话里：

> **Blocking 的本质是预算分配问题，不是相似度排序问题。**

- **创新一 · 边际分配（Marginal-allocation blocking，直觉昵称 water-filling）**：把 pair budget 当资本，按"边际期望召回"在记录间分配。给 E2（Adaptive-k）一个理论骨架。
- **创新二 · 排除式阻塞（Exclusion blocking）**：用"冲突证据"（conflict features）在 pair 级别**排除**不可能是 duplicate 的对，省下的预算回收再分配。阻塞文献几乎只做 inclusion，这是反方向。
- **创新三 · 无金标阻塞审计（Gold-free blocking audit）**：借鉴选举审计（risk-limiting audit）思想，用分层 near-miss 抽样估计 PC 的置信区间——没有 gold 的数据集也能报告 PC ± CI。

三者可独立成篇，也可串成一条 pipeline：**提名（边际分配）→ 排除（exclusion）→ 审计（audit）**。

---

## E1 现状（30 秒带过）

- 机制：MinHash kNN 图 → Leiden 社区 → 社区内候选 + 跨社区保险对，对打 per-record top-k，同预算。
- 原型：`experiments/exp-e1-collective-blocking.R`（entity-disjoint 70/30，预算对齐 ±5%，末尾打印 KILL LINE）。
- 待办：Windows 跑 `Rscript experiments/exp-e1-collective-blocking.R cora`。Kill 线：`treatment PC ≤ baseline PC ⇒ 想法死`。
- 组会上只需一句话：E1 跑完之前不下结论。

---

## 创新一 · 边际分配 Blocking（Marginal-allocation，昵称 water-filling）

### 痛点

Per-record top-k 对所有记录一视同仁：第 k 个候选对的"含金量"在不同记录间天差地别。
对孤立记录，第 3 个候选就已经是噪声；对高歧义记录，第 30 个候选仍可能是真 match。
固定 k 是在**歧义的地方欠投、在清晰的地方浪费**。

### 机制（一句话）

把总 pair budget B 看作资本，每条记录 i 有一条"相似度衰减曲线" s₁ ≥ s₂ ≥ … ≥ sₘ。
用校准函数把相似度映射为 P(match | s)，第 k 个候选的边际价值 ≈ P(match | sₖ)。
**边际分配**（marginal allocation，直觉即 water-filling：把预算当水倒向边际价值最高处）：每次把一个 pair 名额给当前边际价值最高的那条记录，直到 B 用完。
这是带嵌套结构的背包问题，贪心即最优。

### 为什么是新的

- 文献里的自适应 k 都是启发式（按块大小、按密度调参）；**把 blocking 写成"显式期望召回最大化 + 边际分配求解"的，据我们所知没有**（2026-10-15 检索确认）。
- 它不替代 E1/E2，而是给 E2 一个理论骨架：E2 的各种启发式都可以看成边际分配在某种近似下的特例。组会上这句话很加分——**我们不是在调参，是在解一个优化问题**。

### 实验设计（exp-e2-waterfill.R）

- 数据：restaurant（冒烟）、Cora（主）、MusicBrainz 20K（规模）。
- 基线：per-record 固定 top-k（train entities 上调优）。
- 处理组：边际分配，校准函数两种：(a) 有标版——train 上 Platt 校准 3-gram cosine；(b) 无标版——相似度分布的双组分混合模型自举。
- 纪律：entity-disjoint 70/30，预算对齐 ±2%，test 上报 PC/RR，一次。
- **Kill 线**：test 上 PC_margalloc ≤ PC_fixedk ⇒ 死；无标版单独 kill。

### 诚实风险

- 校准是命门：相似度本身是垃圾，边际分配只是更高效地分配垃圾。无标版混合模型可能不稳定——这就是为什么把它单独 kill，而不是混在一起报喜。
- 如果赢了，赢的幅度可能小（1–3 个点 PC）。小而稳也是贡献，但要预先承认。

---

## 创新二 · 排除式 Blocking（Exclusion）+ 预算回收

### 痛点与反直觉

阻塞文献 30 年几乎只做 **inclusion**：想办法别漏掉真对。
但有一类信号被完全浪费了——**冲突证据**：让两个记录"不可能是同一实体"的强信号。
例如：同名同姓、但 rare surname token 冲突；blocking key 相同、但年份差 40 年。
Inclusion 视角下这些对只是"相似度不够高"；exclusion 视角下它们是"**确定不是**"，应该主动踢出去。

### 机制

1. **提名**：常规 inclusion blocker 产生 B 个候选对（可以是 top-k、E1、边际分配，任选）。
2. **排除**：在 pair 级别应用冲突谓词 E(i,j)，踢出高置信非 duplicate。
   冲突谓词举例（从易到难）：
   - 稀有 token 冲突：共享 blocking key，但一方有 IDF > τ 的判别字段 token 而另一方没有；
   - 数值冲突：年份/电话等字段差值超过物理可能；
   - （未来）用小模型学冲突谓词。
3. **回收**：踢掉 e 个对，预算 B−e → e 个名额**回收**，用创新一的边际分配重新分配给高歧义记录。

三步闭环：**提名 → 排除 → 再分配**。据我们所知，文献里没有这个闭环。

### 为什么是新的（先打预防针）

组会上可能有人提："这不就是 block purging / block filtering 吗？"
标准回答，三点区别：

| | Block purging / filtering（Papadakis 等） | 我们的 exclusion |
|---|---|---|
| 粒度 | **块级别**：按块大小启发式丢块 | **pair 级别**：按冲突证据踢对 |
| 信号 | 相似度/块大小的反面 | **dissimilarity 本身作为一等信号** |
| 闭环 | 丢完就完 | 踢出的预算**回收再分配**（接创新一） |

JedAI 的"low matching likelihood pruning"也是相似度 pruning，不是冲突证据。
**Claim 的准确表述**：pair-level conflict-predicate exclusion with budget recycling——这个组合是新的。

### 实验设计（exp-e3-exclusion.R）

- Inclusion 固定为 top-k（控制变量，exclusion 是唯一改动）。
- 先实现最简单的稀有 token 冲突谓词（IDF > τ 且判别字段缺失）。
- 双 Kill 线（预注册）：
  1. **安全 Kill**：test 上被踢掉的真对 > 1% ⇒ 谓词太激进，死；
  2. **价值 Kill**：回收再分配后 ΔPC ≤ 0 ⇒ 闭环无增益（但 exclusion 作为纯 RR 提升器可单独存活，预先声明）。
- **Benchmark 偏差声明**：在干净的 Cora 上可能什么都踢不掉——这不证伪想法，只说明干净 benchmark 测不出脏数据的价值。这是我们 10-12 文献调研里自己指出的 gap（"评测集小而干净"），组会上主动说，显得诚实。

### 诚实风险

- 冲突谓词需要领域知识（哪个字段是判别的），跨数据集迁移要重调。这是真短板，不回避。
- 最坏情况：exclusion 退化成"更聪明的 pruning"，那它就是一个工程优化而非科学贡献—— Kill 线 2 就是为此准备的。

---

## 创新三 · 无金标阻塞审计（Gold-free Blocking Audit）

### 痛点

PC（pair completeness）是 blocking 的核心指标，但**没有 gold 就测不出 PC**。
现实是：大部分真实数据没有 gold。于是所有人都只能在 Cora 那几个小干净数据集上自嗨——
这正是我们指出的 "benchmark bias" gap。**如果我们能无 gold 估计 PC，整个游戏规则就变了。**

### 机制（借自选举审计）

Risk-limiting audit（RLA）是选举审计的成熟方法：抽查少量选票，对"宣布的结果正确"给出统计保证。
搬到 blocking：

1. Blocker 产生候选集 C；matcher 在 C 上跑出预测 match 集合 M。
2. **关键**：被 blocker 剪掉的非候选对，按"离入选差多远"（near-miss 分数）分层。
3. 每层抽少量对，人工（或强 LLM 做 judge）标注，估计每层的漏网率，外推总体。
4. 输出：**PC 的点估计 + 置信区间**，以及"为把 CI 收窄到 ±x 需要再标 y 个对"的预算公式。

### 为什么是新的

- RLA 在选举审计外，据我们所知**从未用于 ER blocking**（2026-10-15 检索确认）。
- 这是一个**方法论贡献**，不是算法贡献：它让"无 gold 数据集上的 blocking 比较"成为可能。
  组会话术："以后我们可以在 D10K 这种无 gold 数据上报告 PC ± CI，这是别人报不出来的数。"

### 实验设计（exp-e4-audit.R，两阶段）

- **阶段一（验证审计本身）**：在 Cora / restaurant（有 gold）上跑 blocker，算出真 PC；
  再跑审计流程（用 gold 模拟人工标注），重复抽样看 CI 的覆盖率是否达到名义水平。
  **Kill 线**：覆盖率 < 名义 − margin ⇒ 审计方法死。
- **阶段二（兑现价值）**：在无 gold 数据集（D10K）上报告 PC ± CI，并展示"标注预算 vs CI 宽度"的 trade-off 曲线。

### 诚实风险

- 审计的 CI 宽度取决于 near-miss 层的漏网率：如果 blocker 很差，CI 会很宽——诚实，但不好看。预先说明：审计是温度计，不是退烧药。
- LLM-as-judge 的标注质量是威胁：阶段一先用 gold 模拟，LLM judge 作为独立变体，分别报告。

---

## 三个创新的关系（一张图的话术）

```
        ┌─────────────┐
        │  提名 Nomination  │  ← 创新一：边际分配（预算怎么分）
        └──────┬──────┘
               ▼
        ┌─────────────┐
        │  排除 Exclusion    │  ← 创新二：conflict predicate（踢什么）
        └──────┬──────┘
               ▼  回收预算 → 回到提名（闭环）
        ┌─────────────┐
        │  审计 Audit        │  ← 创新三：PC ± CI（怎么知道好不好）
        └─────────────┘
```

三个可独立发表，也可合成一篇"预算视角下的 blocking"。

---

## 文献定位（2026-10-15 系统调研回填）

2024–2026 blocking 文献没有出现单一新范式，但有五个明确 thrust。我们的三个创新定位如下
（新颖性声明仍用"据我们所知"，组会口径不变；以下均为 index-level 来源，未逐篇精读原文）：

### 与创新一（边际分配）最相关的

- **Progressive ER（Maciejewski et al., arXiv:2503.08298, 2025）**：提出 filtering→weighting→scheduling→matching 框架，
  其中 scheduling 就是"预算约束下的候选排序"。E2 的 adaptive-k 正是 scheduling 的一个实例。
  **行动**：直接采用它的 progressive-recall-vs-budget 评测方法（18 个数据集），给 E2 现成的
  methodology 和竞争基线——互补，不重复。
- **Open problem #5（2025 blocking survey §10）**："hybrid/ensemble blocking 缺乏理论——union 经验上有效，
  但没有跨通道的 principled fusion 或预算分配理论。"**我们的边际分配正好填这个空**：
  它就是跨记录（未来可扩展到跨通道）的预算分配理论。组会上可以说：文献点名了这个 gap，我们给了第一个构造性答案。
- Shah et al.（Algorithms 2025）："in the age of LLMs, blocking dominates pipelines"——blocking 仍是瓶颈，
  做 blocking 不是做边角料。

### 与创新二（Exclusion）最相关的

- **最接近的已有工作**：pBlocking（Galhotra et al., VLDBJ 2021）的 block-level exclusion、
  Papadakis 的 block purging/filtering。区别已在上文列出（pair 级 vs 块级、冲突证据 vs 大小启发式、闭环回收）。
- **最强动机**：**CorpFam（Gupta, arXiv:2609.04269, 2026）**：54,864 对 / 10,307 families 上，
  7 种 blocking（phonetic/attribute/semantic-NN）全军 <3% recall，union 仅 6.8%——
  **93.2% 的真实链接根本进不了候选集**。blocking 是 binding constraint，不是 matching。
  诚实注：exclusion 修的是"看得见的对里预算花得好不好"，修不了 invisible pairs——claim 别吹过头。
- UniBlocker（2024）sparse+dense ensemble 互补、ER-in-Practice（2026）"+0.4pp HNSW ensemble / +0.3pp identifier 索引"：
  说明"多样通道的 union"是当前实践最优——我们的 exclusion+回收是同一哲学在预算侧的表达。

### 与创新三（Audit）最相关的

- **Gap #1 原样站立**：2024–2026 没有任何一篇论文给出 learned/ANN blocker 的 recall 保证或证书。
  CorpFam 从反面确认了为什么难。我们的审计不宣称"保证"，而是"估计 + 置信区间"——
  这是诚实且可发表的降维：**certificates 没人做出来，estimation 我们第一个做**。
- pBlocking 的 perfect-recall 定理（2021）是唯一有 no-gold 定理的工作，但它依赖 progressive feedback 机制，
  不通用。我们的审计是 blocker-agnostic 的，任何 blocker 跑完都能审计。

### 与 E1 最相关的（答辩弹药）

- **直接 precursor**：Mugeni & Amagasa（~2023，经 SC-Block 引用）：kNN 图 + 社区检测做 blocking，
  在 4 个数据集上 F1 超过 deep blocking 基线。**区别三点**：他们用 transformer embedding（我们用 MinHash，
  可扩展）；他们没有跨社区保险对；他们没有预算对齐的对照实验。E1 的 claim 要精确落在这三点上。
- **未来扩展**：ER-in-Practice 的 hybrid union 结论建议 E1 之后加 dense 通道 + identifier 保险通道，
  但那是 E1 跑完之后的事，组会上只提一句。

### 文献给我们的三个现成抓手（ranked）

1. Progressive scheduling 的评测框架 → E2 直接用（methodology 现成）。
2. Sparse+dense+identifier union → E1 跑完后的扩展方向（recall 最便宜的增量）。
3. CorpFam 分层 benchmark → 检验 community blocking 能否啃下"invisible pairs"（没人宣称过，啃下一块就是 differentiator）。

主要来源：UniBlocker https://arxiv.org/abs/2404.14831 ；NLSHBlock https://arxiv.org/abs/2401.18064 ；
LLM-CER https://arxiv.org/abs/2506.02509 ；AgenticER https://arxiv.org/abs/2607.27435 ；
CorpFam https://arxiv.org/abs/2609.04269 ；ER-in-Practice https://arxiv.org/abs/2607.26298 ；
Progressive ER https://arxiv.org/abs/2503.08298 ；DistillER https://arxiv.org/abs/2602.05452 。

---

## 周五 5 分钟发言提纲

1. **E1 状态**（30 秒）：原型写完，待跑数，不下结论。
2. **视角切换**（1 分钟）：blocking 的本质是预算分配，不是排序。固定 k 在歧义处欠投、在清晰处浪费。
3. **创新一**（1 分钟）：边际分配（marginal allocation）——显式最大化期望召回，贪心即最优；给 E2 理论骨架。
4. **创新二**（1 分钟）：反方向——用冲突证据做 pair 级排除，预算回收再分配；与 block purging 的三点区别。
5. **创新三**（1 分钟）：无 gold 审计——RLA 思想搬运，以后在无 gold 数据上也能报 PC ± CI。
6. **问组里**（30 秒）：三个方向先做哪个？审计的方法论贡献是否值得单独成篇？

---

## 下周动作（建议）

- Sam：Windows 跑 E1（`Rscript experiments/exp-e1-collective-blocking.R cora`），看 KILL LINE。
- Sam：Windows 跑 E2（`Rscript experiments/exp-e2-waterfill.R cora`），看两条 KILL LINE（A: labeled-Platt 校准；B: 无金标混合模型校准）。
- Mas：E2 原型已写完（2026-10-16）：`experiments/exp-e2-waterfill.R`，复用 E1 的诚实框架（entity-disjoint 70/30、预算精确对齐、每记录保底 1 对）；贪心边际分配的最优性论证写在文件头注释里；两个脚本都已放进文件库。
- 组会后定：E3/E4 的排期。

---

## 诚实声明（组会前自己看）

- 三个创新的"新颖性"基于 2026-10-15 定向检索 + 正在进行的系统调研；正式 claim 前需等调研回填。
- 所有实验设计均为**预注册**口径：kill 线写在跑数之前，这是我们对导师诚实的核心资产。
- 创新二在干净 benchmark 上可能测不出效果——已预先声明为 benchmark bias 问题，不是事后找借口。
- 创新三的阶段一必须先通过，阶段二才有意义；不要跳过验证直接报数。

---

## English abstract (for slides)

> We reframe blocking as a **budget allocation problem** rather than a ranking problem, and propose three original directions: (1) **Marginal-allocation blocking** (water-filling) — allocate the pair budget across records by marginal expected recall, the greedy solution to an explicit expected-recall maximization that also gives our adaptive-k heuristic (E2) a theoretical backbone; (2) **Exclusion blocking** — pair-level exclusion predicates built on *conflict evidence* (dissimilarity as a first-class signal), with freed budget recycled via marginal allocation — a propose→exclude→reallocate loop absent from the literature; (3) **Gold-free blocking audit** — adapt risk-limiting audits from election science to estimate pair completeness with confidence intervals on datasets without ground truth. Each direction ships with a pre-registered experiment and a kill criterion.
