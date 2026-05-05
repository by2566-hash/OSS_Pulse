# OSS Pulse — Presentation Slides Plan
_Group 9 | jl17797 (Jhe Chen Li) + by2566 (Bo Yu)_
_Deadline: 2026-05-05 | Last updated: 2026-05-02_

---

## Slide 1 — Title

- **Title:** OSS Pulse: Measuring AI Open-Source Ecosystem Health
- **Team:** Group 9
- **Members:** Jhe Chen Li (jl17797) · Bo Yu (by2566)

---

## Slide 2 — Abstract + Platform

> We analyze the health of AI open-source repositories by combining GitHub Archive event data (334 days, ~100M events), Hugging Face Hub model metadata (2.8M models), and PyPI monthly download statistics (46 libraries). Using Apache Spark on NYU Dataproc (Google Cloud), we compute a composite health score across three dimensions: community engagement (GitHub), model adoption (Hugging Face), and engineering usage (PyPI).

**Platform:** Google Cloud Dataproc (Spark 3.x, YARN), HDFS

---

## Slide 3 — Motivation

- **Who uses it:** ML engineers, researchers, project maintainers, VCs evaluating OSS frameworks
- **Beneficiaries:** Anyone deciding which AI framework to adopt or contribute to
- **Why important:** GitHub stars alone are misleading — hype ≠ health; this project triangulates real adoption signals across three independent dimensions
- **Era analysis (optional):** 5 年 Q1 時間軸（2022–2026）追蹤 coding agent 對全 OSS 生態的影響
  - 2022-Q1：Pre-ChatGPT 基準
  - 2023-Q1：ChatGPT 爆發
  - 2024-Q1：LLM 百花齊放（GPT-4 / Claude / Gemini）
  - 2025-Q1：Agent 元年（Cursor / Claude Code / Devin）
  - 2026-Q1：Coding agent 全面滲透

---

## Slide 4 — Goodness (Data Quality)

| Dimension | Metric | Value |
|-----------|--------|-------|
| Completeness | GH Archive coverage | 485+ days across 5 Q1 eras (2022-2026) + 2025 full year |
| Volume | Total GH events | ~1.46B (Q1 eras) + 2025 full year |
| Data size | Raw ingested | ~930+ GB |
| Data size | Cleaned Parquet | ~183 GB (87.9 GB Q1 + 67.5 GB 2025 + 27.3 GB supplement) |
| Freshness | HF Hub snapshot | April 2026 |
| Breadth | PyPI libraries tracked | 46 |
| Scale | HF models indexed | 2,815,064 |

---

## Slide 5 — Data Sources

| Source | Description | Size |
|--------|-------------|------|
| GitHub Archive | Hourly event logs: Watch / Fork / Push / PR / Issue | ~1.46B events, 485+ days, 87.9 GB cleaned (930+ GB raw) |
| Hugging Face Hub | Model metadata snapshot (April 2026) | 2,815,064 models, 229 MB |
| PyPI | Monthly download counts per library (2025) | 46 libraries × 12 months |

---

## Slide 6 — Data Sample: GitHub Archive

```
event_type       | event_date | actor_login | repo_name               | push_size | pr_merged
WatchEvent       | 2025-03-15 | user_abc    | huggingface/transformers | null      | null
PushEvent        | 2025-03-15 | dev_xyz     | pytorch/pytorch          | 3         | null
PullRequestEvent | 2025-03-15 | contrib_1   | tensorflow/tensorflow    | null      | true
IssuesEvent      | 2025-03-15 | reporter_2  | microsoft/onnxruntime    | null      | null
ForkEvent        | 2025-03-15 | forker_3    | openai/whisper           | null      | null
```

Schema: `event_type, event_date, actor_login, repo_name, push_size, push_distinct_size, commit_count, pr_merged, issue_state`

---

## Slide 7 — Data Sample: Hugging Face Hub

```
model_id                      | library_name | pipeline_tag     | downloads | likes | parameter_count | has_safetensors
meta-llama/Llama-3-8B         | transformers | text-generation  | 1,234,567 | 8,920 | 8,000,000,000   | true
stabilityai/stable-diffusion  | diffusers    | image-generation |   890,123 | 5,430 | 2,600,000,000   | true
openai/whisper-large-v3       | transformers | automatic-speech |   456,789 | 3,210 | 1,550,000,000   | false
```

Schema: `model_id, author, library_name, pipeline_tag, downloads, likes, created_at, parameter_count, has_safetensors`

---

## Slide 8 — Data Sample: PyPI

```
project       | month      | downloads
transformers  | 2025-01-01 | 12,345,678
torch         | 2025-01-01 | 45,123,456
diffusers     | 2025-02-01 |  3,456,789
accelerate    | 2025-03-01 |  2,134,500
datasets      | 2025-04-01 |  4,890,123
```

Schema: `project, month (YYYY-MM-01), downloads`

---

## Slide 9 — Design Diagram

```
Data Ingestion                Processing (Spark)            Output
──────────────                ──────────────────            ──────
GH Archive (HDFS) ──┐
                    ├──► Job 01: AI repo daily metrics
HF Hub API ─────────┤    Job 02: HF + GH join            health_score.csv
                    ├──► Job 03: Three-way health score ──►
PyPI BigQuery ──────┘    Job 04: Top 1000 repos           Tableau vizs
                         Job 05: AI vs General ──────────►
                         Job 06: Star hype detection ────►
                         Job 07: Contributor health ─────►
```

**Tools:** Python (ingest / clean) · PySpark (analytics) · Scala Spark (HF pipeline) · Tableau (visualization)

**Health Score Framework:**
We extend the [OpenSSF Criticality Score](https://github.com/ossf/criticality_score) concept by adding two adoption dimensions that the original score does not capture:
- **Original OpenSSF signals:** contributor count, commit frequency, org count, recent releases, issue/PR age
- **Our extension:** HF model downloads (30%) + PyPI engineering usage (20%) — measures real-world adoption beyond GitHub activity
- **Formula:** `health_score = Σ log1p(metric_i) × weight_i` across 6 signals (HF downloads 30%, PyPI 20%, GH stars 15%, pushes 15%, PRs 10%, active_days 10%)

---

## Slide 10 — Code Challenge: jl17797 — HDFS Quota & Rolling Pipeline

**Challenge 1 — Data freshness gap:**
HF Hub snapshot is April 2026, but GH Archive only covered through Nov 2025 — 5-month gap. Needed to ingest 3,624 hourly files without occupying /tmp disk.

**Solution:** Stream directly to HDFS via `curl | hdfs dfs -put -f -`. One-time `hdfs dfs -ls` + local `grep` for resume-safe skip (vs. 1 HDFS round-trip per file):
```bash
# One HDFS call at start instead of N round-trips
hdfs dfs -ls "$HDFS_DEST" | awk '{print $NF}' | sed 's|.*/||' > "$EXISTING_LIST"
if grep -qF "$fname" "$EXISTING_LIST"; then continue; fi
curl -sfL "$URL" | hdfs dfs -put -f - "$HDFS_DEST/$fname"
```

**Challenge 2 — HDFS quota: 500 GB hard limit, 5 years × Q1 raw = ~700 GB needed:**
Downloading 2022/2023/2024 Q1 raw simultaneously would blow the quota (each year ~150–235 GB raw, but only ~18 GB after Spark cleaning).

**Solution — Rolling pipeline orchestration:**
Process one year at a time: download → Spark clean → delete raw → next year. Cleaned Parquet is 8× smaller than raw gz, so space is freed before the next download starts.

```bash
wait_for_download "$RAW_2022" 2160 "2022-Q1"
spark-submit 00_clean_gharchive_2022q1.py
hdfs dfs -rm -r "$RAW_2022"   # free ~151 GB before 2023-Q1 starts
```

**Outcome:** 5-year Q1 timeline (2022–2026) ingested within 500 GB quota. Era comparison across Pre-ChatGPT → LLM explosion → Coding-agent saturation enabled.

**Challenge 3 — Era comparison: 90 GB across 5 eras, 48 GB executor memory:**
Job 08 needs to aggregate 5 years of Q1 data (~90 GB). Naïve approach (cache all → 4 aggregations) failed: cache spills to disk → slower than direct parquet read → YARN kills job after 1.5h.

**Solution v1 — Column pruning + aggregation merging + dynamic shuffle partitions:**
```python
# 1. Each aggregation loads only needed columns (3-6 vs 22 total)
#    Parquet column pruning cuts I/O by 50-70%
gh = load_all(["event_type", "repo_name", "actor_login", ...])

# 2. Merge summary_metrics + push_size_distribution into single scan
#    (both groupBy "era" = 5 groups → one pass instead of two)
combined = gh.groupBy("era").agg(
    # summary metrics + push distribution in one agg()
)

# 3. Dynamic shuffle partitions: 24 for groupBy("era"),
#    200 for groupBy("era", "repo_name")
spark.conf.set("spark.sql.shuffle.partitions", "24")  # 5 groups
```

**What we would do differently — Per-era pre-aggregation:**
Current design unions 5 eras (90 GB) then shuffles — each aggregation shuffles the full 90 GB. Better approach: process each era independently (15-23 GB each), compute all 4 metrics per era, then merge 5 small result sets.

| | Current (union-first) | Per-era (aggregate-first) |
|---|---|---|
| Peak memory | 90 GB (exceeds 48 GB RAM) | 15-23 GB (fits easily) |
| Shuffle per aggregation | 90 GB | 15-23 GB |
| Failure blast radius | All eras restart | Only failed era restarts |
| Estimated runtime | 2-3 hours | 40-60 minutes |
| YARN timeout risk | High | None (each era ~10 min) |

```python
# Per-era design (more resilient):
for path, era_label, date_filter in ERA_SOURCES:
    df = load_era(path, era_label, date_filter, columns)
    metrics = df.groupBy(F.lit(era_label).alias("era")).agg(...)
    metrics.write.mode("overwrite").parquet(f"{OUT}/intermediate/{era_label}")

# Combine 5 tiny results → final CSV
spark.read.parquet(f"{OUT}/intermediate/").coalesce(1).write.csv(...)
```

**Challenge 4 — PR event double counting:**
GH Archive records multiple events per PR lifecycle (opened, closed, synchronize, edited). Counting `event_type == 'PullRequestEvent'` overcounts PRs. Agent-generated PRs trigger more `synchronize` events, biasing the "higher PR merge rate" hypothesis.

**Solution:** Use `countDistinct(pr_number)` instead of event count, and filter `payload_action == 'closed' AND pr_merged == True` for merge rate:
```python
F.countDistinct(F.when(
    F.col("event_type") == "PullRequestEvent", F.col("pr_number")
)).alias("distinct_prs"),
F.countDistinct(F.when(
    (F.col("payload_action") == "closed") &
    (F.col("pr_merged").eqNullSafe(True)),
    F.col("pr_number")
)).alias("merged_prs"),
```

**Challenge 5 — GH Archive 2026 breaking schema change:**
GitHub officially announced Events API payload simplification on **2025-08-08**, effective **2025-10-07** ([GitHub Blog Changelog](https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/)):
- **PushEvent:** `commits`, `distinct_size`, and `size` fields removed — only `before`, `head`, `push_id`, `ref` remain
- **PullRequestEvent:** `pull_request.merged` field removed, `merged_at` removed; instead `payload.action` now emits `"merged"` as a new action value (previously only `opened/closed/synchronize/edited`)

This caused 2026-Q1 `merged_prs = 0` (false zero) and `push_distinct_size = NULL` (all rows) in the initial era comparison run. Verified by downloading raw JSON from `data.gharchive.org` for 2025-03-15 vs 2026-03-15 and comparing payload keys.

**Solution:** Add backward-compatible merged PR detection that works across both schemas:
```python
F.countDistinct(F.when(
    (F.col("event_type") == "PullRequestEvent") &
    (
        ((F.col("payload_action") == "closed") & (F.col("pr_merged").eqNullSafe(True))) |
        (F.col("payload_action") == "merged")   # 2026+ schema
    ),
    F.col("pr_number")
)).alias("merged_prs"),
```
`push_distinct_size` cannot be recovered — the field no longer exists in the raw data. Noted in analysis as a data limitation for 2026-Q1.

**Challenge 6 — 2026 Q1 WatchEvent/ForkEvent anomalous drop:**
Data quality check revealed WatchEvent (stars) dropped **63%** (19.6M → 7.2M) and ForkEvent dropped **67%** (4.6M → 1.5M) between 2025-Q1 and 2026-Q1. Each WatchEvent represents a new star action. GH Archive is a historical snapshot — previously recorded events are not retroactively removed. Therefore the drop reflects **fewer new stars being created in 2026-Q1**, not historical events being deleted. The most likely explanation is that fake accounts identified by GitHub's anti-spam efforts have already been deleted and can no longer generate new stars/forks. An ICSE 2026 study ([arxiv.org/html/2412.13459v2](https://arxiv.org/html/2412.13459v2)) identified ~6 million fake stars across 300K accounts, and researchers observed that 90.42% of flagged repos and 57.07% of flagged accounts were subsequently removed. However, GitHub has not officially announced a mass purge — this is inferred from academic research and observed data patterns. Other contributing factors (changes to event recording criteria, natural behavior shifts) cannot be ruled out.

**Impact:** 2026-Q1 star and fork counts are not directly comparable to prior eras. IssuesEvent (+9%) and PullRequestEvent (-22%) are unaffected, suggesting real development activity remains relatively stable.

**Solution:** In cross-era analysis, treat WatchEvent/ForkEvent trends for 2026-Q1 with caution and note the likely influence of platform-level anti-spam measures. Use PushEvent, PREvent, and IssuesEvent as more reliable indicators for trend analysis.

---

## Slide 11 — Code Challenge: by2566 — Billion-Record Shuffle at Ecosystem Scale

**Challenge:** GH Archive data is partitioned by `event_date` (334 partitions), but ecosystem-wide analysis requires aggregation by `repo_name`. Computing top-1000 repos across 1B+ events forces a full cross-partition shuffle — all events for the same repo must be co-located on the same executor before aggregation can occur.

**Scale:** 72 GB input → 83 GB shuffle → 1B+ records regrouped across 2 workers, taking 3+ hours

**Solution:** Tuned `spark.sql.shuffle.partitions` to match cluster resources. Structured downstream jobs (05/06/07) to read from pre-aggregated Job 04 output instead of re-shuffling raw events.

**Key code** (`spark_jobs/04_top_repos_all.py`):
```python
spark.conf.set("spark.sql.shuffle.partitions", "400")

# GROUP BY repo_name forces full shuffle across date partitions
repo_stats = gh.groupBy("repo_name").agg(
    F.countDistinct("actor_login").alias("distinct_actors"),
    F.sum(...).alias("total_stars"), ...
).orderBy(F.desc("total_stars")).limit(1000)
```

**Lesson:** At billion-record scale, the cost of GROUP BY is dominated by network I/O (shuffle), not computation. Partition strategy at ingest time determines downstream query cost.

**What we would do differently:** Write cleaned GH Archive data bucketed by `repo_name` into a Hive table at ingest time. This would co-locate all events for the same repo on the same partition, eliminating the 83 GiB shuffle in Job 04 entirely and reducing aggregation time from 3+ hours to minutes.
```python
# Optimized write (requires Hive metastore)
cleaned.write \
  .bucketBy(256, "repo_name") \
  .sortBy("repo_name") \
  .saveAsTable("gharchive_cleaned")
```

**Additional optimizations applied:**
- Added `.cache()` on computed DataFrames before multiple write actions — without caching, each `.write()` and `.count()` re-triggers the full shuffle independently (3× redundant recomputation in Job 04)
- Replaced Python UDFs (`is_ai()`) with native `F.col().isin()` in Jobs 05/06/07 — Python UDFs force JVM↔Python serialization per row; native expressions run entirely in JVM
- Cached `gh` DataFrame in Job 07 where it is consumed 3 times by independent aggregations

---

## Slide 12 — Results

_Based on: `health_score.csv` (36 repos), `hf_gh_join.csv`, `top_repos_all` (1,000 repos), `star_growth_hype.csv` (1,000 repos), `contributor_health.csv` (1,000 repos)_

---

**Finding 1 — transformers 是三維全能冠軍**
`huggingface/transformers` health_score 14.68（最高），PyPI 年下載 **9.38 億次**，HF Hub 上 **834,591 個模型**基於它，active_days 332 天（幾乎全年無休）。真正的生態系統基礎設施。

**Finding 2 — Hype ≠ Health：Ollama & DeepSeek 案例**
- **Ollama**：GitHub 新增 44,259 stars（集合中最多），但 HF 下載量只有 **241 次**——本地執行工具，stars 高度誇大生態影響力
- **DeepSeek-R1/V3**：top_repos 全站排名 #2/#3（各超過 86,000 stars、93,000 distinct actors），但完全不在 health_score 集合——純模型發布熱潮，無持久工具生態

**Finding 3 — Quiet Powerhouse：低調的真實影響力**
`sentence-transformers`：2025 年僅 1,588 個新 stars，卻有 HF 下載 **5.19 億次**。
每個 star 對應 **327,014 次 HF 下載**，是 transformers 的 3 倍效率。
GitHub 活躍度嚴重低估了它在生產環境的滲透率。

**Finding 4 — 傳統 ML vs LLM 生態完全割裂**
`scikit-learn`：PyPI 年下載 **14.28 億次**（全集合第一，超過 transformers），HF 下載幾乎為零（122 次）。
`LangChain`：PyPI 排名 #3（8.72 億下載），但 HF 模型數 1、下載量 0。
→ orchestration / 傳統 ML 工具與 LLM 訓練框架是兩個平行宇宙。

**Finding 5 — GitHub stars 是最差的健康指標**
Health score 前 3 名（transformers / ultralytics / pytorch）在 top_repos 1000 名中的 stars 排名分別為 #109 / #67 / #186。
Stars 不足以預測生態健康；PyPI + HF 組合才能捕捉真實的工程採用訊號。

**Finding 6 — Star 爆紅模式：40% 有爆點，僅 0.2% 極端**
_Based on: `star_growth_hype.csv` (1,000 repos, Job 06)_
- **40%** top-1000 repo 的 peak_ratio > 3x（有明顯爆發），但多數無法持續
- 僅 **2 個 repo**（0.2%）peak_ratio > 10：`zama-ai/bounty-program`（10.83x，98.5% stars 集中單月）
- **DeepSeek** 14 個 repo 幾乎全部 > 5x——品牌型爆紅生態系
- **InkOnChain** 三個 repo 同步 ~8.9x，總 stars 各逾 4 萬——疑似 Web3 社群組織動員
- `build-your-own-x`：**95,857 stars，peak_ratio 僅 2.5**——最大規模來自最平穩成長

**反直覺：AI 框架比非 AI 更不爆紅**
- AI repo（is_ai=true）平均 peak_ratio：**2.10**
- 非 AI repo 平均 peak_ratio：**3.17**
- 原因：seed list 中的成熟 AI 框架（transformers/pytorch/vllm）靠口碑穩定成長，不靠爆紅事件。

> *"A high peak ratio tells you a project went viral. A high total stars with a low peak ratio tells you a project is healthy."*

**Finding 7 — Push 集中度 ≠ 單人風險：需結合 PR 貢獻者解讀**
_Based on: `contributor_health.csv` (1,000 repos, Job 07)_

GitHub PushEvent 包含 PR merge（歸屬按 merge 的人），因此 top1_push_ratio 高不一定代表單人寫 code，可能是集中 merge 權。需結合 `pr_contributors` 區分：

| Repo | top1_push_ratio | pr_contributors | 解讀 |
|------|----------------|-----------------|------|
| `hiyouga/llama-factory` | 0.961 | 123 | 集中 merge，但社群活躍 |
| `geekan/metagpt` | 0.929 | **22** | ⚠️ 真正單人風險 |
| `openai/whisper` | 0.875 | 45 | 中等風險 |
| `unslothai/unsloth` | 0.818 | **166** | 集中 merge，社群貢獻多 |
| `tensorflow/tensorflow` | 0.990 | 299 | CI bot 自動化，非真人 |

對比最健康的 AI 框架：`pytorch/pytorch` 僅 **0.140**（275 push + 1,605 PR contributors），`huggingface/transformers` **0.191**（72 push + 1,244 PR contributors）

**Finding 8 — Stars 多 ≠ 貢獻者多：觀眾 vs 建設者**
- `deepseek-ai/deepseek-r1`：93,947 distinct actors，但只有 **5 人 push、19 次 push** → 99.99% 是觀眾
- `codecrafters-io/build-your-own-x`：115,862 actors，僅 **3 人 push** → 教程倉庫，無協作開發
- 對比 `pytorch/pytorch`：14,563 actors、**275 push contributors、118,740 次 push** → 真正的社群協作
- `n8n-io/n8n`：113,341 actors、**100 push contributors、32,351 pushes** → stars 高且有真實開發深度

**Finding 9 — 最健康的開源項目：push 分散度指標**
- 全生態最健康：`grafana/grafana`（top1_push_ratio **0.052**，316 push contributors）
- AI 領域最健康：`pytorch/pytorch`（0.140）、`chroma-core/chroma`（0.164）、`vllm-project/vllm`（0.192）
- Push 分散度與 PR 貢獻者正相關：`llvm/llvm-project` 1,145 push + 2,870 PR contributors

> *"Stars count your audience. Push contributors count your builders. A healthy project needs both."*

---

## Slide 13 — Obstacles

1. **HDFS permission boundary**
   - jl17797 has read-only access to by2566's directory; cannot write there.
   - Workaround: all outputs go to `jl17797`'s HDFS; Spark jobs union both paths at read time.

2. **Data freshness mismatch**
   - GH Archive: 2025 only; HF Hub: April 2026 snapshot.
   - Required supplemental ingestion of 151 days (Dec 2025 – Apr 2026), streaming ~3,600 files to HDFS.

3. **Scale vs iteration speed**
   - 100M events make schema errors expensive (1–2h per job).
   - Solution: local smoke test on 1-day slice (`pipeline/gharchive/smoke_test.sh`) before every cluster run.

---

## Slide 14 — Summary

- Built an end-to-end Spark pipeline combining 3 heterogeneous data sources at ~146M-event scale (485+ days)
- Computed a composite health score for 36 AI repos across community, adoption, and engineering dimensions
- Analyzed contributor health and bus-factor risk for top 1,000 repos
- 5-year era comparison (2022–2026 Q1) tracking coding-agent impact on OSS ecosystem
- Deep-dive: ~250 repos (AI vs Non-AI) across 5 eras — PR merge time, commit patterns, contributor flow
- **Key takeaway:** GitHub stars are a noisy signal; PyPI + HF downloads together better predict sustained ecosystem health. Push contributor concentration (top1_push_ratio) reveals single-maintainer risk invisible to star counts.
- Framework-agnostic methodology — extensible to any open-source domain

---

## Slide 15 — Acknowledgements

- **NYU High Performance Computing** — Google Cloud Dataproc cluster access
- **Tableau** — Academic visualization license via NYU
- **GH Archive** — Open hourly event logs
- **Hugging Face Hub** — Public model metadata API
- **PyPI** — Public download statistics

---

## Slide 16 — References

- GH Archive: https://www.gharchive.org
- Hugging Face Hub API: https://huggingface.co/docs/huggingface_hub
- PyPI Stats: https://pypistats.org
- Apache Spark: https://spark.apache.org
- Google Cloud Dataproc: https://cloud.google.com/dataproc

---

## Slide 17 — Thank You

---

## TODO before May 5

- [x] Jobs 04 / 05 / 06 / 07 跑完 → 填入 Slide 12 實際數字（9 findings）
- [x] Supplement data cleaned (151 days, Dec 2025 – Apr 2026)
- [x] Contributor health analysis (Finding 7-9)
- [x] 2022 / 2023 / 2024 Q1 下載 + 清理完成
- [ ] Job 08 (era comparison) — 跑中（per-era 設計重寫，解決 YARN timeout）
- [ ] Job 09 (repo era deep dive) — 擴展到 ~250 repos（seed AI + top 200 non-AI），加 group-level AI vs Non-AI 對比
- [ ] Job 08 / 09 結果分析 → 填入 Slide 12 era findings
- [ ] Tableau 圖表截圖 → 插入 Slide 12（最多 3 張）
- [ ] Design Diagram 換成視覺圖（可用 draw.io / Keynote）
- [ ] Data Sample slides 換成真實資料截圖（from Jupyter or Spark output）
- [ ] Code Challenge slides 加入 code screenshot（非純文字）
