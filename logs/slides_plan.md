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

## Slide 10 — Code Challenge: jl17797 — Supplement Ingestion

**Challenge:** HF Hub snapshot is April 2026, but GH Archive only covered through Nov 2025 — 5-month data gap.

**Solution:** Stream 3,624 hourly `.json.gz` files directly to HDFS via `curl | hdfs dfs -put -f -` (no /tmp disk space needed), then Spark-clean into Parquet.

**Key code** (`spark_jobs/00_download_gharchive_supplement.sh`):
```bash
# Resume-safe: skip files already on HDFS
if hdfs dfs -test -e "$DEST/$fname"; then continue; fi
curl -sfL "$URL" | hdfs dfs -put -f - "$DEST/$fname"
```

**Outcome:** 151 additional days ingested (2025-12-01 ~ 2026-04-30), enabling time-aligned analysis with HF April 2026 snapshot.

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

---

## Slide 12 — Results

> Fill in actual numbers after Jobs 05 / 06 / 07 complete. Placeholder findings below based on `health_score.csv`.

**Finding 1 — AI repos dominate composite health**
Top 5 health_score repos are all AI frameworks: `transformers`, `diffusers`, `torch`, `timm`, `sentence-transformers`.

**Finding 2 — Hype ≠ Health**
Several repos with high star velocity (peak_ratio > 5×) show low PyPI adoption — community excitement without engineering adoption.

**Finding 3 — Three-dimension divergence**
~30% of AI repos are HF-popular but PyPI-light, suggesting hobbyist/researcher use without production deployment.

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
