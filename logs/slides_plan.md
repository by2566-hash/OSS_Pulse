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
| Completeness | GH Archive day coverage | 334 / 365 days (bad-date filtered) |
| Volume | Total GH events | ~100M |
| Freshness | HF Hub snapshot | April 2026 |
| Breadth | PyPI libraries tracked | 46 |
| Scale | HF models indexed | 2,815,064 |

---

## Slide 5 — Data Sources

| Source | Description | Size |
|--------|-------------|------|
| GitHub Archive | Hourly event logs: Watch / Fork / Push / PR / Issue | ~100M events, 334 days |
| Hugging Face Hub | Model metadata snapshot (April 2026) | 2,815,064 models |
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

_Based on: `health_score.csv` (36 repos), `hf_gh_join.csv`, `top_repos_all.parquet` (1,000 repos)_

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

- Built an end-to-end Spark pipeline combining 3 heterogeneous data sources at 100M-event scale
- Computed a composite health score for 36 AI repos across community, adoption, and engineering dimensions
- **Key takeaway:** GitHub stars are a noisy signal; PyPI + HF downloads together better predict sustained ecosystem health
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

- [ ] Jobs 05 / 06 / 07 跑完 → 填入 Slide 12 實際數字
- [ ] Tableau 圖表截圖 → 插入 Slide 12（最多 3 張）
- [ ] Design Diagram 換成視覺圖（可用 draw.io / Keynote）
- [ ] Data Sample slides 換成真實資料截圖（from Jupyter or Spark output）
- [ ] Code Challenge slides 加入 code screenshot（非純文字）
