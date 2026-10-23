# Deep Learning 在 Entity Resolution 中的应用：文献综述

> 编译日期：2026-10-02。基于网络公开文献的调研整理，供 ERBOT 论文方向决策参考。
> 来源分级说明：`verified live` = 2026-10-02 当天在浏览器中打开原文核实；`index` = 通过网络索引的原文/摘要页核实；`secondary` = 来自第三方综述笔记，引用前请再核对原文。

---

## 0. 结论先行（TL;DR）

深度学习进入 ER 分三波：**Wave 1（2017–2019）** DeepER/DeepMatcher 用 RNN 把手工相似度特征换成学出来的表示；**Wave 2（2020–2022）** Ditto 把匹配写成 sequence-pair 分类，F1 一口气抬高约 30%，同时学习型 blocking（AutoBlock/DeepBlocker）把候选生成也做成了可学的；**Wave 3（2022–2026）** LLM 把"训练一个 matcher"变成了"prompt 一个 matcher"——GPT-3 few-shot 在 4/7 个数据集上打败了全量微调的 Ditto。

对论文方向最直接的结论：**纯粹在 Magellan 小数据集上再涨 matcher 精度的路基本走死了**（Ditto 2020 就是一个又强又简单的天花板）。开放且有冲击力的，是**系统级问题**：学习型 blocking 与 recall 保证、给 LLM matcher 配有界候选预算的 blocking、sparse–dense 混合、真实大尺度评估、cost-aware 的级联流水线。这和 `paper-direction-brief` 里"scaling/blocking 为主方向"的建议是同一个方向。

---

## 1. 早期深度模型：DeepER、DeepMatcher（2017–2019）

### 1.1 DeepER (Ebraheem et al., PVLDB 2018)
- 来源：https://arxiv.org/pdf/1710.00597v3 (`index`)
- 核心：用 uni-/bi-LSTM 把每个 tuple 的词嵌入合成分布式向量，在向量上算 tuple 相似度——**不再做手工特征**；blocking 用基于 tuple 向量的 LSH，且用上全部属性而不是几个手工 blocking key。
- 结果：在 benchmark、生物医学、多语言数据集上超过当时已有方案，标注量需求更小。

### 1.2 DeepMatcher (Mudgal et al., SIGMOD 2018)
- 来源：https://dl.acm.org/doi/abs/10.1145/3183713.3196926；代码 https://github.com/anhaidgroup/deepmatcher (`index`)
- 核心：把神经实体匹配形式化成一个设计空间（attribute embedding → summarization → alignment → aggregation → classification），并做成开源包，嵌入 Magellan 的 EM 工作流。
- 结果/发现：深模型在**文本型、脏数据**上打败经典 ML 基线（Magellan 的 RF/SVM），但**需要大量标注**，在强结构化/数值型属性上不行；神经+经典混合最强。同时把 Magellan 数据集标准化成了全领域的评测套件。

### 1.3 它们改变了什么
- 之前：Magellan 式流水线要人工给每个属性选相似度函数（Jaro-Winkler、TF-IDF cosine、数值距离……），拼 comparison vector 再训分类器——人力全在特征工程上。
- 之后：相似度端到端学出来，人的工作从"设计特征"变成"提供标注对"（+ blocking）。这个标注瓶颈直接催生了第 5 节的 label-efficiency 线。
- 相关早期工业工作：Shah, Kopru, Ruvini, "Neural Network Based Extreme Classification and Similarity Models for Product Matching", ACL 2018（eBay 商品匹配）——工业界先转向了学出来的方法。

---

## 2. 预训练语言模型时代（2020–2022）

### 2.1 Ditto (Y. Li et al., PVLDB 2020) —— 之后所有 matcher 的基准线
- 来源：https://arxiv.org/pdf/2004.00584v3；代码 https://github.com/megagonlabs/ditto (`index`)
- 核心：把 EM 写成**序列对分类**：每条记录序列化成 `COL attr VAL value …`，两条用 `[SEP]` 拼接，微调 BERT/RoBERTa 做 cross-encoder。三招优化：(1) 注入领域知识（高亮数字、ID、名字等 span）；(2) 超长字符串的 TF-IDF 摘要；(3) 保标签的数据增强（span 删除/打乱、记录交换）。
- 关键数字：F1 相对之前 SOTA **最高提升约 30%**（摘要说 29%，正文有 31% 的说法，按 "~30%" 理解）；三招优化合计 +9.8%；**用最多一半的标注量就追平之前的 SOTA**；在一个 789K × 412K 的真实公司匹配任务上达到 96.5% F1，其中好的 blocking 让端到端流水线比 naive blocking 快 **3.8×**。
- 后续：journal 版 "Effective entity matching with transformers", VLDB Journal 2023；鲁棒性分析（Paganelli et al., VLDB Journal 2023；RoBEM）。

### 2.2 DADER (Tu, Fan et al., SIGMOD 2022 + PVLDB 2022 demo)
- 来源：https://github.com/ruc-datalab/DADER (`index`)
- 核心：用多个标注好的**源** ER 数据集，训练出能在零/少标注**目标**上跑的 deep matcher——挑合适的源 pair，再用六种 domain adaptation 策略（MMD、K-order、GRL、InvGAN、InvGAN+KD、ED）对齐源/目标分布。
- 注：它也属于第 5 节的 label-efficiency 线，放在这里是因为它的 matcher 主干是 PLM pair classifier。

### 2.3 标准评测集（了解"饱和"从何而来）
- **Magellan/DeepMatcher 套件**（2018 起的事实标准）：Amazon-Google 11,460 pairs / DBLP-ACM 12,363 / DBLP-Scholar 28,707 / Walmart-Amazon 10,242 / Abt-Buy 9,575 / iTunes-Amazon 539 / BeerAdvocate-RateBeer 450 / Fodors-Zagats 946。注意 iTunes-Amazon 只有 539 个 pair——**又小又被反复刷**。
- **WDC Training + Gold**（Primpeli, Peeters, Bizer, WWW 2019 companion）：2600 万商品 offer、约 2070 万正例 pair——因为公开 benchmark 太小、喂不饱深模型而建。
- **WDC Products**（arXiv:2301.09521, 2023）：多维评测（corner case 频率、unseen entity 比例、dev 集大小），测的是鲁棒性而不是一个总分。
- **Machamp**（Wang, Li, Hirota, CIKM 2021）：含异构 schema 任务的广义 EM benchmark。
- 横向比较（只能看**各自论文内的相对提升**，跨论文的绝对数因 split/预处理不同不可比）：Magellan 经典 ML < DeepMatcher/DeepER（文本/脏数据上明显赢）< Ditto（~+30% F1，标注减半）< HierGAT（见 §4）< LLM zero-shot（见 §6）。
- WDC 一线复现了 Mudgal 的发现：embedding/深 matcher 在**非结构化数据**上比符号方法高 5–10% F1，在强结构化数据上差不多。

---

## 3. 学习型 Blocking（2020–）：候选生成的经济学和匹配完全不同

Blocking 要的是**极高 recall**（漏掉的 match 下游永远找不回来）+ 极致效率，它的输出量直接给 matcher 定价。

### 3.1 AutoBlock (Zhang et al., WSDM 2020)
- 来源：https://arxiv.org/pdf/1912.03417v1 (`index`)
- 核心：免手工的 blocking——学一个保相似度的表示 + 最近邻检索，不用设计 blocking key、不用清洗数据。次二次时间复杂度，可上百万记录；脏/非结构化数据上优势最大。

### 3.2 DeepBlocker (Thirumuruganathan et al., PVLDB 2021)
- 来源：https://www.vldb.org/pvldb/vol14/p2459-thirumuruganathan.pdf；代码 https://github.com/anhaidgroup/deepblocker (`index`)
- 核心：8 种 deep blocker 的设计空间（autoencoder-based、CTT、Tuple-BERT、USI……），产出**自监督** tuple embedding（零标注），喂给 ANN 检索。
- 意义：确立了"自监督 embedding + ANN"这条免标注 dense blocking 的标准配方。

### 3.3 SC-Block (Brinkmann, Shraga, Bizer, ESWC 2024)
- 来源：https://2024.eswc-conferences.org/wp-content/uploads/2024/04/146640116.pdf (`index`)
- 核心：有监督对比学习（SupCon）把同实体的描述在 embedding 空间里聚拢，FAISS/HNSW 最近邻检索出候选集；同时发布 **WDC-Block** benchmark（3,259 个 e-shop、最大 200B pair、700 万词表）。
- 关键数字：候选集比竞品小 **约 50%**，整条 ER 流水线快 **1.5–4×**，F1 不掉。

### 3.4 UniBlocker (T. Wang et al., arXiv:2404.14831, 2024)
- 来源：https://arxiv.org/abs/2404.14831 (`verified live`)
- 核心：**通用型** dense blocker——在领域无关的表格语料上做对比预训练，新领域直接用、不用逐个微调；附带多领域 blocking benchmark。
- 结果：零领域学习就打败之前的自监督/无监督 dense blocker，可比肩 SOTA sparse blocking；**sparse + dense 取并集能进一步提 recall**——两者互补。

### 3.5 Recall–效率的权衡（当前共识）
- Dense blocker 用 embedding/index 成本换语义 recall（同义词、改写、跨语言）；sparse blocker（token/q-gram/TF-IDF、MinHash-LSH）便宜，在表面重叠上很强。**近期共识：两者互补，都跑、取并集。**
- 监督轴：自监督（DeepBlocker、UniBlocker、Sudowoodo）零标注；有监督对比（SC-Block）用 pair 标注换来小得多的候选集。
- 评测指标沿用 Papadakis et al., ACM CSUR 2020：Pairs Completeness（recall）、Reduction Ratio、Pairs Quality。**Blocking recall 是整条流水线 recall 的上界。**

---

## 4. GNN 做 ER（小而专的一条线）

围绕 pairwise classifier 忽略的**集体信号**（传递性、共享属性/邻居）。

- **HierGAT**（Yao et al., SIGMOD 2022）：三层图（token → attribute → entity）+ 图注意力，attention 边能指出判定时关键的 token/属性。有第三方综述称比 Ditto 高 +8.7 F1（`secondary`，引用前请核原文）。
- **GraLMatch**（De Meer, Zhou et al., EDBT 2025）：不是图编码器，而是**事后图修正**——先跑 Ditto，把 pairwise 预测建成图，用传递性约束检测并删掉 false positive。
- **GraphER / "When GDD meets GNN"**（Hu et al., arXiv:2410.04783, 2024）：规则+学习混合——图差分依赖（GDD）编码记录匹配规则，指导 GNN 学表示；在 17 个图数据集 + 7 个关系数据集上对 10 个 SOTA 评测，图 ER 最好，关系型也有竞争力。
- 理论：Ganesan 的 MPNN-based ER 表达力层级——检测"有没有共享属性"是便宜的局部计算，检测"有**多个**共享属性"需要严格更强的架构（尖锐的复杂度断层）。

---

## 5. 标注效率：无监督、自监督、主动学习

- **ZeroER**（Wu et al., SIGMOD 2020）：零标注下，match/unmatch 的相似度向量分布可分——用 GMM 拟合两种分布，加 ER 专用的自适应正则防特征过拟合，并把传递性折进生成模型。在 5 个 benchmark 上大幅超过已有无监督方法、**可比肩有监督方法**。
- **Kasai et al.（ACL 2019）**：高资源场景学可迁移的深 ER 模型，再用主动学习在低资源目标上挑少量信息量大的样本微调——**标注量少一个数量级**，达到或超过当时 SOTA。
- **Sudowoodo**（R. Wang, Y. Li, J. Wang, ICDE 2023）：统一的数据集成 formulation；SimCLR 式对比预训练从**无标注**序列化实体学相似度感知的表示，**一个共享 embedder 同时服务 blocker（最近邻）和 matcher**；多监督级别下多个 SOTA。
- Domain adaptation 一线：DADER（§2.2）、DAME（Trabelsi et al., WSDM 2022）、TransER（Kirielle et al., EDBT 2022）。
- 主动学习：风险采样（arXiv:2012.12960）等深主动学习；古典对应是 Corleone/Falcon 系统和 Isele & Bizer 2013 的遗传编程学 linkage rule。

---

## 6. LLM 时代（2022–2026）

### 6.1 Prompting / In-context learning
- **Narayan et al., "Can Foundation Models Wrangle Your Data?", PVLDB 2022**：把 5 个数据清洗/集成任务（含 EM）写成 prompting（行序列化成文本，FM 答 Yes/No）。**GPT-3 few-shot 在 7 个 EM 数据集中的 4 个上打败了全量微调的 Ditto**，只用几个示例。这篇把 EM 从"训练模型"改写成了"prompt 模型"。指出的短板：私有/领域数据、非专家可用性。
- **Peeters & Bizer, "Using ChatGPT for Entity Matching", ADBIS 2023**：zero-shot ChatGPT 在一个 hard 任务上拿到 **82.35% F1**，RoBERTa 需要 2,000 条训练样本才能追平；基于相似度选 demonstration 最多 **+7.85%** F1；写明匹配规则的效果和给 demo 差不多。
- **MatchGPT**（Peeters, Steiner, Bizer, EDBT 2025）：系统评测 hosted + 开源 LLM。结论：最好的 LLM 零/少样本就能追平训了几千对的 PLM；**对 unseen entity 更鲁棒**（这是微调 PLM 记训练实体的致命弱点）；**没有万能 prompt**，必须按 model×dataset 调；GPT-4 能输出结构化解释、帮忙诊断标注错误。

### 6.2 小而专的微调模型（成本答案）
- **AnyMatch**（arXiv:2409.04073, 2024）：zero-shot EM 写成序列分类，微调 **GPT-2（124M）** 做迁移，配新的数据选择策略（AutoML 选 hard pair、生成的属性级样本、标签平衡控制）。在 9 个 benchmark 上对 13 个 baseline 拿**第 2 高的总 F1**——离 MatchGPT/GPT-4 只差 **4.4%**，推理成本是 **1/3,899**。
- **Jellyfish**（EMNLP 2024）(`secondary`)：在 4 个数据预处理任务（含 EM）上指令微调开源模型（Mistral-7B、Llama-3-8B 等），号称可比肩 GPT-3.5/4、单卡本地跑——"可自建、保隐私"的 matcher 原型。引用数字前请核原文。

### 6.3 超出独立 pairwise 调用
- **ComEM**（COLING 2025）(`secondary`)：LLM 做 match/compare/**select**，在集合层面而非独立 pairwise 做决策。
- **LLM-CER**（arXiv:2506.02509, 2025）：LLM 做 **in-context clustering**——NRS（生成下一记录集）、MDG（错聚检测护栏）、CMR（利用传递性/反传递性做簇合并）。
- **"Retrieve, Match, Escalate"**（arXiv:2608.25037, 2026）：生产式级联——每记录 top-20 检索（文本 ANN + 图像 ANN + GTIN 通道），便宜 cross-encoder 按置信度分流，**只有模糊带才升级到 agentic VLM**；blocking/recall 显式给漏斗定界。

### 6.4 LLM 做 blocking？
- 文献共识：LLM 做 blocking **贵到不可行**（二次方 pair × 单 pair 成本），正确用法是**选择性**的——blocking 之后对模糊候选做精修，底下还是 dense embedding blocker（SC-Block、UniBlocker）做可扩展的候选生成。
- **Shah et al., "Efficient Record Linkage in the Age of Large Language Models: The Critical Role of Blocking", Algorithms 18(11):723, 2025** (`verified live`)：blocking + 微调 LLaMA3/BERT **同时**在运行时间和 F1 上打败纯 LLM linkage——**blocking 在 LLM 时代依然关键**。

---

## 7. 效率与可扩展性

- 已知数据点：AutoBlock 次二次、可上百万记录；Ditto 的 789K × 412K 部署 96.5% F1，blocking 带来 3.8× 端到端加速；SC-Block 候选集减半、流水线快 1.5–4×（200B pair benchmark）；BlockingPy 的具体操作点（Abt-Buy：4.4 秒、recall 0.82）。
- **候选生成给 matcher 定价**：cross-encoder/LLM matcher 的单 pair 推理占主导，所以候选集大小（及其 recall）就是扩展性的杠杆——这也是领域从"blocking 是 key 工程"转向"blocking 是学出来的检索"的原因。
- **Record-level vs pair-level blocking**（近期 dense-blocking 评测中使用的区分；没找到单一 canonical 出处，概念在 embedding-blocking 文献中一致使用）：
  - *Record-level*：blocker 按记录行动，每条记录取有界的 top-k 候选（embedding 上的 ANN k-NN）。候选量 O(n·k)——**可预测**，天然适配 ANN 基础设施，给 recall–cost 一个旋钮 k。这是 DeepBlocker、UniBlocker、SC-Block、BlockingPy 的模式。
  - *Pair-level*：blocker 直接输出候选 **pair**（如 key-based block 内共现的所有 pair，或便宜的 pairwise scorer 剪枝）。候选量难定界，skewed block 会爆炸。
  - 为什么重要：贵的 matcher 需要**有界的、按记录的**候选预算；record-level 检索天然映射到 ANN，pair-level 的枚举没有这个旋钮。这正是导师说的"record-level blocking 和 pair-level blocking 不一样"的技术实质。

---

## 8. Open Problems

1. **给 LLM 时代的 blocking**：如何给昂贵的 matcher 配有界的按记录候选集而不丢 recall；学出来的 sparse–dense 混合 blocking；LLM 的**选择性**使用（精修而非枚举）。"Retrieve, Match, Escalate" 是早期答案，不是定论。
2. **通用/零配置 blocking**：UniBlocker 指向领域无关的 dense blocking；开放的是 recall 保证、自动配置（LSH/ANN 参数、阈值）、sparse 与 dense 信号的原则性融合。
3. **Benchmark**：经典套件又小又饱和；WDC-Block（200B pair）、WDC Products（corner case、unseen entity）是进步，但缺脏多语言、时序、多模态 ER benchmark；在**真实的后 blocking 候选分布**（而非平衡 pair 集）上评测依然少见。
4. **大规模集体 ER**：传递性/一致性现在是事后处理（GraLMatch）或被忽略；带保证的簇级联合推断依然难；GNN 表达力理论刚起步。
5. **LLM matcher 的成本、校准与信任**：单 pair 推理成本/吞吐；prompt 敏感（没有万能 prompt）；LLM match 分数的校准；记录发给 hosted LLM 的隐私（Jellyfish/AnyMatch 式本地微调是部分答案）；web 衍生 benchmark 的数据污染。
6. **硬 case 的鲁棒性**：预训练给不了 grounding 的行话领域；异构 schema 与 schema drift；数值/标识符为主的记录（语义模型不如符号方法）。
7. **标注效率（继续）**：ZeroER/DADER/Sudowoodo 减少了但没消灭标注需求；**给 blocking 配置做主动学习**（而不只是 matching）基本没人做。
8. **公平性与偏见**：blocking key 和 embedding 可能与受保护属性相关；深 ER 文献里几乎没人研究。
9. **可解释/可调试**：早期工作（LIME 经验报告、CERTA/CERTEM 反事实、LLM 生成解释）——还没成为 ER 系统的一等公民。

---

## 9. 饱和 vs 开放：给论文方向选择的建议

| 子方向 | 判断 | 理由 |
|---|---|---|
| Magellan 级小数据集上的有监督 pairwise matcher（RNN/PLM cross-encoder） | **饱和** | Ditto（2020）是一个又强又简单的天花板；之后都是增量，数据集又小又被刷烂。"在 Abt-Buy 上超过 Ditto" 很难卖出去 |
| LLM prompting 做 pairwise matching 的配方 | **拥挤** | 2023–2025 已有大量 prompt/ICL 研究；headline 结论（few-shot ≈ 微调 PLM、OOD 鲁棒、无万能 prompt）已确立；需要新角度（成本、保证、新任务形式）才能出头 |
| 学习型 dense blocking（自监督 embedding + ANN） | **开放，有冲击力** | 大规模下的 recall–cost 权衡没定论；通用/领域无关 blocker（UniBlocker）刚起步；sparse–dense 混合缺原则性融合；**给 LLM matcher 配 blocking（有界按记录预算）大片空白** |
| 端到端扩展（blocking → matching → clustering） | **开放** | 真实成本模型、级联、WDC-Block 级评测刚出现；工业界要这个胜过再涨一个点的 F1 |
| 集体/图结构 ER | **开放** | 传递性多是事后处理；联合推断、GNN 表达力、规则指导的神经 ER（GraphER）都很年轻 |
| 标注高效 ER（自监督、DA、主动） | **半开放** | 核心想法有了（ZeroER、DADER、Sudowoodo），但 blocking 配置的主动学习、DA 的源选择理论、标注高效的**多模态** ER 还是空白 |
| 可信 ER（校准、公平、隐私、解释） | **开放** | 几乎没人做；随着 ER 进生产级 LLM 流水线越来越重要 |

**底线**：选"scaling/blocking"，不要选"再做一个 deep matcher"。Scaling/blocking 方向 headroom 更清楚、工业界 audience 更直接，而且**和每一个 matcher 进展是组合关系而不是竞争关系**——matcher 越贵（LLM），blocking 越值钱。如果执意做 matcher 一侧，就打没被解决的轴：OOD/unseen entity、行话领域、多模态、校准——而不是在 DBLP-ACM 上再涨一个点。

---

## 10. 与 ERBOT 的对接点

- ERBOT 现在是"blocking/候选生成 + pairwise 相似度 + 分类"的架构，正好落在文献的成熟区：它的 matcher 对应 Wave 2 的 PLM cross-encoder 位置（但 ERBOT 用的是手工相似度特征 + 传统分类器——按 DeepMatcher/Ditto 的叙事，这是**被替代掉的那一端**，值得在论文里正面讨论为什么保留/何时升级）。
- ERBOT 的 candidate-space 工作天然对应 §3 和 §7：把 ERBOT 的候选生成表述成 **record-level blocking**（按记录的有界 top-k 检索），recall–cost 的旋钮就是 k——这正是近期文献的语言，导师也点名了这个区分。
- 低成本探针（paper brief 里提过的）：在 Affiliation 上跑一个 embedding probe（Sudowoodo/UniBlocker 思路的轻量版），看 dense 信号相对现有 blocking 能补多少 recall——花小钱验证"要不要做深"。

---

## References

1. Ebraheem et al., "Distributed Representations of Tuples for Entity Resolution" (DeepER) — PVLDB 2018 — https://arxiv.org/pdf/1710.00597v3
2. Mudgal et al., "Deep Learning for Entity Matching: A Design Space Exploration" (DeepMatcher) — SIGMOD 2018 — https://dl.acm.org/doi/abs/10.1145/3183713.3196926；PDF https://pages.cs.wisc.edu/~anhai/papers1/deepmatcher-sigmod18.pdf；代码 https://github.com/anhaidgroup/deepmatcher
3. Brunner & Stockinger, "Entity Matching with Transformer Architectures" — EDBT 2020 — https://openproceedings.org/2020/conf/edbt/paper_205.pdf
4. Li et al., "Deep Entity Matching with Pre-Trained Language Models" (Ditto) — PVLDB 2020 — https://arxiv.org/pdf/2004.00584v3；代码 https://github.com/megagonlabs/ditto
5. Tu et al., "Domain Adaptation for Deep Entity Resolution" (DADER) — SIGMOD 2022 — https://dl.acm.org/doi/10.1145/3514221.3517870；代码 https://github.com/ruc-datalab/DADER
6. Yao et al., "Entity Resolution with Hierarchical Graph Attention Networks" (HierGAT) — SIGMOD 2022 — https://dl.acm.org/doi/10.1145/3514221.3517872；代码 https://github.com/CGCL-codes/HierGAT
7. Zhang et al., "AutoBlock: A Hands-off Blocking Framework for Entity Matching" — WSDM 2020 — https://arxiv.org/pdf/1912.03417v1
8. Thirumuruganathan et al., "Deep Learning for Blocking in Entity Matching: A Design Space Exploration" (DeepBlocker) — PVLDB 2021 — https://www.vldb.org/pvldb/vol14/p2459-thirumuruganathan.pdf；代码 https://github.com/anhaidgroup/deepblocker
9. Wu et al., "ZeroER: Entity Resolution using Zero Labeled Examples" — SIGMOD 2020 — https://arxiv.org/abs/1908.06049；代码 https://github.com/chu-data-lab/zeroer
10. Kasai et al., "Low-resource Deep Entity Resolution with Transfer and Active Learning" — ACL 2019 — https://aclanthology.org/P19-1586
11. Narayan et al., "Can Foundation Models Wrangle Your Data?" — PVLDB 2022 — https://www.vldb.org/pvldb/vol16/p738-narayan.pdf；代码 https://github.com/HazyResearch/fm_data_tasks
12. Peeters & Bizer, "Using ChatGPT for Entity Matching" — ADBIS 2023 — https://arxiv.org/pdf/2305.03423
13. Peeters, Steiner, Bizer, "Entity Matching using Large Language Models" (MatchGPT) — EDBT 2025 — arXiv:2310.11244；代码 https://github.com/wbsg-uni-mannheim/matchgpt
14. Wang, Li, Wang, "Sudowoodo: Contrastive Self-supervised Learning for Multi-purpose Data Integration and Preparation" — ICDE 2023 — https://arxiv.org/pdf/2207.04122v2；代码 https://github.com/megagonlabs/sudowoodo
15. Brinkmann, Shraga, Bizer, "SC-Block: Supervised Contrastive Blocking within Entity Resolution Pipelines" — ESWC 2024 — https://2024.eswc-conferences.org/wp-content/uploads/2024/04/146640116.pdf
16. Wang et al., "Towards Universal Dense Blocking for Entity Resolution" (UniBlocker) — arXiv:2404.14831, 2024 — https://arxiv.org/abs/2404.14831
17. Zhang et al., "AnyMatch — Efficient Zero-Shot Entity Matching with a Small Language Model" — arXiv:2409.04073, 2024 — https://arxiv.org/abs/2409.04073v2
18. Hu et al., "When GDD meets GNN" (GraphER) — arXiv:2410.04783, 2024 — https://arxiv.org/abs/2410.04783
19. De Meer, Zhou et al., "Matching Groups of Entities with Graphs and Language Models" (GraLMatch) — EDBT 2025 — arXiv:2406.15015
20. "In-context Clustering-based Entity Resolution with Large Language Models" (LLM-CER) — arXiv:2506.02509, 2025 — https://arxiv.org/pdf/2506.02509
21. Shah et al., "Efficient Record Linkage in the Age of Large Language Models: The Critical Role of Blocking" — Algorithms 18(11):723, 2025 — https://www.mdpi.com/1999-4893/18/11/723
22. Primpeli, Peeters, Bizer, "The WDC Training Dataset and Gold Standard for Large-Scale Product Matching" — WWW 2019 companion — http://webdatacommons.org/largescaleproductcorpus/v2/
23. Peeters, Der, Bizer, "WDC Products: A Multi-Dimensional Entity Matching Benchmark" — arXiv:2301.09521, 2023 — https://arxiv.org/abs/2301.09521
24. Wang, Li, Hirota, "Machamp: A Generalized Entity Matching Benchmark" — CIKM 2021 — DOI 10.1145/3459637.3482008
25. Barlaug & Gulla, "Neural Networks for Entity Matching: A Survey" — ACM TKDD 15(3), 2021 — https://arxiv.org/pdf/2010.11075
26. Papadakis et al., "Blocking and Filtering Techniques for Entity Resolution: A Survey" — ACM CSUR 53(2), 2020 — https://arxiv.org/pdf/1905.06167
27. Suri et al., "Ember: No-Code Context Enrichment via Similarity-Based Keyless Joins" — PVLDB 15(3), 2022 — https://arxiv.org/abs/2106.01501
28. ComEM (COLING 2025)、Jellyfish (EMNLP 2024)、"Match, Compare, or Select?" (arXiv:2405.16884)、"Retrieve, Match, Escalate" (arXiv:2608.25037)、DAME (WSDM 2022)、TransER (EDBT 2022) —— 经第三方综述笔记转引（`secondary`），正式引用前请核对原文。
