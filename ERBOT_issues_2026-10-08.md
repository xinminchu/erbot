# ERBOT 问题清单（2026-10-08 排查，Sam 已批准全部修复 ✅）

## P1 ✅ 已修： DESCRIPTION 缺 VignetteBuilder —— vignette 构建不出来
- **是什么**：DESCRIPTION 里没有 `VignetteBuilder: knitr` 这一行。
- **影响**：`R CMD build` 会直接跳过 `vignettes/erbot.Rmd`，打出来的包没有 vignette。
- **修法**：DESCRIPTION 加一行 `VignetteBuilder: knitr`（knitr 已在 Suggests 里）。

## P2 ✅ 已修： .Rprofile 引用不存在的 renv/activate.R —— 目录下 Rscript 直接报错
- **是什么**：仓库根目录 `.Rprofile` 里 source 了 `renv/activate.R`，但 renv/ 目录不存在。
- **影响**：在这个目录直接跑 `Rscript` 会报错退出（实测踩到），必须加 `--vanilla`。
- **修法**：删掉 `.Rprofile`，或改成条件加载。

## P3 ✅ 已修（按 b 方案）： 内置数据集文件缺失 —— "affiliation"/"d10k" 关键词会报错
- **是什么**：`er_load()` 文档说 `"affiliation"` 和 `"d10k"` 是内置数据集（`inst/extdata/`），但目录不存在，文件在 10-03 搬仓时移走了。
- **影响**：`er_run("affiliation")` 会报 "Built-in dataset not found"（报错清楚，不会崩）。导师照文档试会撞墙。
- **修法（三选一）**：a) 从 archive 拷小文件回 inst/extdata/；b) 文档改"需手动下载"；c) 删关键词。

## P4 ✅ 已修： irlba 2.4.1 与旧版 Matrix/R 不兼容 —— 环境问题，非包 bug
- **是什么**：最新 irlba 2.4.1 在 R 4.3 + Matrix 1.6 下彻底跑不起来（`irlba(X)` 直接报 "LENGTH or similar applied to NULL object"，连 100x20 矩阵都挂）。
- **影响**：`er_tune` 里走 `er_tfidf_svd` 的 2 个测试在我这挂了（Sam Windows 上是绿的，说明他那边的 irlba 版本没问题）。换 irlba 2.3.5.1 后全绿。
- **修法**：DESCRIPTION 里给 irlba 加版本上限，或文档注明"irlba 建议用 2.3.x"。不修也行（Sam 环境是好的），但别人复现时可能踩坑。

## 跑通验证结果（本机 R 4.3.3实测）
- 包安装加载正常；vignette 的 toy 例子端到端跑通（4 实体全对，louvain/final ARI=1.0）。
- testthat 全套件：**FAIL 0 | WARN 0 | SKIP 4 | PASS 146**（4 个 skip 是缺 MASS/brglm2 可选包）。
- 30 个 R 文件语法全过；115 个导出函数都有定义；Suggests 包调用都有保护；R/experimental 确实被构建忽略。

## 验证（2026-10-08 22:40）
- 包重装加载正常；testthat 全套件 FAIL 0/WARN 0/SKIP 4/PASS 146（4 skip 缺 MASS/brglm2）。
