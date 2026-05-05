# OSS Pulse — Pipeline Insights & Analysis Framework
_Group 9 | jl17797 (Jhe Chen Li) + by2566 (Bo Yu)_
_Last updated: 2026-05-05_

---

## Research Question

**AI 開源生態系統的健康度如何？Coding agent 如何改變 OSS 開發模式？**

---

## Layer 1: Who Are the Winners? (Static Ranking)

### Job 04 — top_repos_all
- 2025 全 GitHub 最活躍 Top 1000 repos
- AI 相關 repo 在 top 50 中佔比顯著（ollama, langchain, dify, open-webui, claude-code...）

### Job 02 — hf_gh_join
- HuggingFace 模型下載量 × GitHub 活動交叉比對（35 筆）
- **Key finding:** HF 下載量與 GH stars 不正相關
  - sentence-transformers：5.19 億下載，僅 1,588 stars（成熟穩定）
  - ollama：241 下載，44,520 stars（GH 熱門但不走 HF 生態）

### Job 03 — health_score
- 三方合併健康分數：HF downloads 30% + PyPI 20% + GH stars 15% + GH pushes 15% + GH PRs 10% + active_days 10%
- **Top 5 健康分數：**
  1. transformers (14.7)
  2. ultralytics (12.8)
  3. vllm (12.7)
  4. sentence-transformers (12.7)
  5. diffusers (12.6)

**Story: Stars ≠ Health. 真正的健康需要三角驗證（GH 活躍度 + HF 模型採用 + PyPI 安裝量）**

---

## Layer 2: AI vs General Repos (Comparative Analysis)

### Job 05 — ai_vs_general
- Top 1000 repos 標記 `is_ai`
- AI repo 的 stars/forks/PR 密度是否更高？

### Job 07 — contributor_health
- Bus factor、top1_push_ratio、push/PR 貢獻者數
- **Key findings:**
  - open-webui `top1_push_ratio = 0.997` → 一個人在推
  - awesome 類 repo `top1_push_ratio > 0.95` → 單人維護
  - AI 類 repo 傾向貢獻者高度集中

### Job 06 — star_growth_hype
- peak_ratio（peak month stars / avg monthly stars）偵測 hype
- **Key finding:** peak_ratio > 8 的專案多數是 AI 相關（DeepSeek, OpenManus, Cosmos...）

**Story: AI repo 星多但貢獻者集中，bus factor 風險高。Hype 型專案多數是 AI 相關。**

---

## Layer 3: What Changed Over Time? (5-Year Q1 Trend, 2022–2026)

### Job 08 — era_comparison

#### Summary Metrics

| Era | Events | Repos | Developers | Pushes | Merged PRs |
|-----|--------|-------|------------|--------|------------|
| 2022-Q1 | 2.07 億 | 1698 萬 | 700 萬 | 1.46 億 | 135,134 |
| 2023-Q1 | 2.47 億 | 1986 萬 | 783 萬 | 1.86 億 | 276,993 |
| 2024-Q1 | 3.64 億 | 1759 萬 | 892 萬 | 3.08 億 | 153,935 |
| 2025-Q1 | 3.76 億 | 2024 萬 | 1025 萬 | 3.14 億 | 236,555 |
| 2026-Q1 | 2.66 億 | 2086 萬 | 1057 萬 | 2.25 億 | 131,700 |

#### Key Trends
1. **開發者持續增長：** 700 萬 → 1057 萬（+51%），即使 2026 事件量下降
2. **2026 事件量下降 31%：** 但 repos/actors 仍增長 — agent 減少重複性操作？
3. **Push 集中化：** 單 commit push 比例 87% (2022) → 94% (2025)，小而頻繁
4. **PR/Push ratio 下降：** 0.098 → 0.077，更多直接 push，PR 流程被簡化
5. **每 repo 貢獻者數下降：** 4.88 → 2.84，bot/agent 帳號集中化
6. **2026 merge rate 下降：** 44.9% vs 2025 的 64.6%

#### Data Limitations (2026-Q1)
- `push_distinct_size` = NULL（GH Archive schema 變更，欄位已移除）
- `avg_commit_size` 無法計算
- `pr_merged` 改為 `payload_action = "merged"`（已修正）

**Story: Coding agent 時代（2025-2026），開發者更多、repo 更多，但每個 repo 的活動強度降低。Agent 讓開發「更輕」了。**

---

## Layer 4: Micro-Behavior Changes (Job 09 — Pending)

### Job 09 — repo_era_deep_dive
- ~50 AI seed repos + Top 200 non-AI repos，跨 5 個 era 比較

| Metric | Expected Insight |
|--------|-----------------|
| pr_merge_time | AI repo 的 PR merge 是否逐年縮短？（agent auto-review） |
| daily_pr_count | 2025-2026 AI repo PR 頻率是否暴增？ |
| daily_commits | commit 量趨勢（2026 因 schema 缺失無法計算） |
| contributor_flow | 新 vs 回頭貢獻者比例 — AI repo 是否靠少數 bot 帳號？ |
| group AI vs Non-AI | 每個指標的 AI/Non-AI 差異隨時間如何變化？ |

**Story: AI repo 的開發節奏在加速（merge 更快、PR 更頻繁），但貢獻者多樣性在下降 — 這健康嗎？**

---

## Layer 5: Development Rhythm Changes (Job 10 — Completed)

### Job 10 — dev_rhythm_analysis

#### Weekend vs Weekday Activity Ratio

| Era | Weekday Events | Weekend Events | Weekend % | Weekend Actors % |
|-----|---------------|---------------|-----------|-----------------|
| 2022-Q1 | 1.53 億 | 5328 萬 | 25.8% | 34.3% |
| 2023-Q1 | 1.89 億 | 5876 萬 | 23.7% | 34.1% |
| 2024-Q1 | 2.68 億 | 9591 萬 | 26.3% | 35.0% |
| 2025-Q1 | 2.75 億 | 1.01 億 | 26.9% | 35.4% |
| 2026-Q1 | 1.87 億 | 7910 萬 | **29.7%** | **36.9%** |

Natural weekend ratio in Q1 (~90 days) ≈ 29%. 2026 nearly matches it — weekday/weekend gap is closing. This is a strong signal of 24/7 agent activity.

Note: Weekend defined by UTC timezone; consistent across all eras for trend comparison.

#### Push Frequency per Actor

| Metric | 2022 | 2023 | 2024 | 2025 | 2026 |
|--------|------|------|------|------|------|
| Push actors | 4.52M | 5.20M | 5.89M | 6.95M | **8.71M** |
| Avg pushes | 32.4 | 35.7 | **52.2** | 45.1 | 25.8 |
| Median pushes | 6 | 6 | 6 | 6 | **4** |
| P90 pushes | 46 | 49 | 47 | 46 | **32** |
| P99 pushes | 235 | 250 | 239 | 245 | **206** |
| >1000 pushes accounts | 4,168 | 4,135 | 5,347 | **9,788** | 6,351 |
| >50 pushes/day accounts | 4,139 | 4,171 | 6,197 | **12,743** | 6,575 |
| Avg repos per actor | 2.77 | 2.88 | 2.82 | 2.77 | **2.41** |

**Key findings:**
1. **2026 push actors surged to 8.71M** (+25% YoY) — but per-actor pushes dropped sharply (median 6→4, avg 45→26). More people participating, each doing less.
2. **2025 was peak for high-frequency pushers:** >1000 pushes accounts nearly doubled (5,347→9,788), >50/day tripled (6,197→12,743) — early agent explosion.
3. **2026 high-frequency accounts dropped back** — likely GitHub's bot account cleanup reducing automated pushers.
4. **Avg repos per actor declining** (2.88→2.41) — contrary to expectation that agents spread across repos. May reflect more single-repo casual contributors.

**Story: The coding agent era shows two phases — 2025 was the explosion (high-frequency automated pushers peaked), 2026 is the normalization (platform cleanup + behavior adaptation). Weekend activity ratio climbing toward natural levels confirms 24/7 automated development.**

---

## Overall Narrative (Presentation Main Thread)

> **2022-2026，OSS 生態經歷了三個轉變：**
>
> 1. **規模擴張：** 開發者 +51%，repo 數持續增長
> 2. **行為轉變：** Push 更小更頻繁，PR 流程簡化，agent 減少了重複操作
> 3. **集中化風險：** AI repo 貢獻者更集中，bus factor 更低，stars 與真實採用脫鉤
>
> **Conclusion: GitHub stars alone are misleading — we need multi-dimensional metrics to measure OSS health.**

---

## Technical Challenges

1. **HDFS Quota (500 GB):** Rolling pipeline — download → clean → delete raw
2. **YARN Lifetime (5h):** Per-era processing with intermediate saves
3. **90 GB Union OOM:** Process each era independently, merge intermediate CSVs
4. **PR Double Counting:** `countDistinct(pr_number)` instead of event count
5. **GH Archive 2026 Schema Change:** `pr_merged` removed → fallback to `payload_action="merged"`; `push_distinct_size` removed → cannot recover

---

## Data Scale

- **Total events:** ~1.46 billion (across 5 Q1 eras)
- **Cleaned Parquet:** 87.9 GB (Q1 eras) + 67.5 GB (2025 full year) + 27.3 GB (supplement)
- **Raw ingested:** ~930+ GB
- **HF Hub models:** 2,815,064
- **PyPI libraries:** 46 × 12 months
