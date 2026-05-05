# OSS Pulse — Presentation Slides Plan v2
_Group 9 | jl17797 (Jhe Chen Li) + by2566 (Bo Yu)_
_Deadline: 2026-05-05 | Last updated: 2026-05-05_

---

## Narrative Arc

**Core thesis:** _GitHub stars are the worst health metric. We analyzed 1.46B events across 5 years and found that coding agents are reshaping OSS development — weekends disappeared, developers multiplied, but individual contribution shrank._

**Structure:** 3-act story (Setup → Journey → Discovery)

---

## Act 1: Setup — What's the problem? (Slides 1–4)

---

### Slide 1 — Title

- **Title:** OSS Pulse: Measuring AI Open-Source Ecosystem Health
- **Subtitle:** Tracking the Coding Agent Era Through 1.46 Billion GitHub Events
- **Team:** Group 9 — Jhe Chen Li (jl17797) · Bo Yu (by2566)
- **Course:** BDAD Spring 2026

---

### Slide 2 — The Problem

**Hook:** _"A repo with 44,000 stars and 241 downloads — is it healthy?"_

- GitHub stars ≠ real adoption (Ollama: 44,520 stars, 241 HF downloads)
- Hype ≠ sustained engineering value (DeepSeek-R1: 93,947 actors, only 5 push contributors)
- Need multi-dimensional measurement: Community + Adoption + Engineering

**Visual:** One striking comparison — Ollama vs sentence-transformers (stars vs downloads bar)

---

### Slide 3 — Data & Scale

| Dimension | Value |
|-----------|-------|
| Total events | 1.46 billion |
| Time span | 5 × Q1 snapshots (2022–2026) |
| Cleaned data | 87.9 GB Parquet |
| Raw ingested | 930+ GB |
| Data sources | GitHub Archive + HuggingFace Hub (2.8M models) + PyPI (46 libraries) |
| Platform | Google Cloud Dataproc, 4 nodes, YARN, HDFS |

**Timeline context:**
- 2022-Q1: Pre-ChatGPT baseline
- 2023-Q1: ChatGPT boom
- 2024-Q1: LLM explosion (GPT-4, Claude, Gemini)
- 2025-Q1: Agent era begins (Cursor, Claude Code, Devin)
- 2026-Q1: Coding agent saturation

---

### Slide 4 — Pipeline Architecture

**Visual:** Flow diagram (one image, no text-heavy bullets)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  GH Archive │    │  HF Hub API │    │  PyPI Stats │
│  (hourly gz)│    │  (snapshot) │    │  (monthly)  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌──────────────────────────────────────────────────┐
│           Spark Clean & Deduplicate              │
│  (5 event types, partitioned by event_date)      │
└──────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│              10 Analytics Jobs                     │
│  Health Score │ Era Comparison │ Dev Rhythm │ ... │
└──────────────────────────────────────────────────┘
       │
       ▼
   CSV outputs → Visualization → Findings
```

---

## Act 2: Journey — How did we do it? (Slides 5–8)

---

### Slide 5 — ETL Design

**Schema design** (5 event types → unified schema):
```
event_type | event_date | event_ts | actor_login | repo_name
payload_action | pr_number | pr_merged | push_distinct_size
```

**Key decisions:**
- Column pruning: select only needed fields from nested JSON → 70% storage reduction
- Partition by `event_date` for time-range queries
- `dropDuplicates(["event_id"])` for dedup
- `ignoreCorruptFiles=true` for fault tolerance (580 corrupt gzip in 2026 data)

**Scale:** 930 GB raw → 87.9 GB cleaned (90.5% reduction)

---

### Slide 6 — Code Challenge: HDFS Quota & Per-Era Design (jl17797)

**Problem:** 5 years × Q1 raw data = ~700 GB needed, but HDFS quota = 500 GB. Single union of 5 eras (90 GB) exceeds executor memory → YARN kills.

**Solution 1 — Rolling Pipeline:**
```
download Q1 → clean → delete raw → next Q1
```
Stay within 500 GB at all times.

**Solution 2 — Per-Era Processing with Intermediate Saves:**
```python
for path, era in ERA_SOURCES:
    df = spark.read.parquet(path)
    # compute 4 metrics independently
    summary.write.csv(f"{OUT}/intermediate/summary_{era}")
    df.unpersist()
# Final merge reads only small intermediate CSVs
```

**Result:** Crash-safe, resumable, fits in memory. If YARN kills mid-job, completed eras are preserved.

---

### Slide 7 — Code Challenge: 2026 Schema Breaking Change (jl17797)

**Discovery:** Job 08 reported `merged_prs = 0` for 2026-Q1. Investigation revealed:

| Field | 2025 | 2026 |
|-------|------|------|
| `payload.distinct_size` | ✅ present | ❌ **removed** |
| `payload.commits` | ✅ present | ❌ **removed** |
| `pr.merged` | `true/false` | ❌ **NULL** |
| `payload.action` | opened/closed | + **"merged"** (new) |

**Root cause:** GitHub API payload simplification (announced 2025-08-08, effective 2025-10-07)

**Verification:** Downloaded raw JSON from both eras, compared payload keys directly.

**Fix:**
```python
# Backward-compatible merged PR detection
((F.col("payload_action") == "closed") & (F.col("pr_merged").eqNullSafe(True))) |
(F.col("payload_action") == "merged")  # 2026+ schema
```

**Validation:** Re-downloaded all 2160 raw files, re-cleaned, compared → diff < 0.39% (corrupt file tolerance).

---

### Slide 8 — Code Challenge: Billion-Record Shuffle (by2566)

**Problem:** Computing top-1000 repos across 1B+ events requires full cross-partition shuffle. All events for the same repo must be co-located.

**Scale:** 72 GB input → 83 GB shuffle → 3+ hours

**Solution:**
- Tuned `spark.sql.shuffle.partitions` to match cluster resources (400 → 200)
- Downstream jobs read from pre-aggregated output instead of re-shuffling
- Cache computed DataFrames before multiple write actions

**What we'd do differently:** Bucket by `repo_name` at ingest time → eliminate shuffle entirely.

---

## Act 3: Discovery — What did we find? (Slides 9–14)

---

### Slide 9 — Finding: Stars ≠ Health

**Visual:** Bubble chart — x: PyPI downloads (log), y: GH stars, size: HF models

**Three archetypes:**

| Archetype | Example | Stars | Real Usage |
|-----------|---------|-------|-----------|
| True leader | transformers | 13K | PyPI 938M + HF 835K models |
| Quiet powerhouse | sentence-transformers | 1.6K | HF 519M downloads |
| Hype machine | ollama | 44.5K | HF 241 downloads |

**Punchline:** _"Every star on sentence-transformers represents 327,014 HF downloads. Stars measure attention, not adoption."_

Health score formula: HF downloads 30% + PyPI 20% + GH stars 15% + pushes 15% + PRs 10% + active days 10%

---

### Slide 10 — Finding: Contributor Risk is Hidden

**Visual:** Horizontal bar chart — top1_push_ratio for selected repos, colored by interpretation

**Key insight:** High top1_push_ratio ≠ always bad. Must combine with PR contributors:

| Pattern | Example | top1_push_ratio | PR contributors | Risk |
|---------|---------|----------------|-----------------|------|
| Healthy | pytorch/pytorch | 0.14 | 1,605 | ✅ Low |
| Concentrated merge | hiyouga/llama-factory | 0.96 | 123 | 🟡 Medium |
| True single-point | geekan/metagpt | 0.93 | 22 | 🔴 High |
| Audience only | deepseek-r1 | — | 5 push contributors | 🔴 Fragile |

**Punchline:** _"93,947 actors watched DeepSeek-R1. Only 5 people actually push code."_

---

### Slide 11 — Finding: The 5-Year Ecosystem Shift

**Visual:** Multi-line chart across 5 eras (2022–2026 Q1)

| Metric | 2022 | → | 2026 | Change |
|--------|------|---|------|--------|
| Developers | 7.0M | → | 10.6M | +51% |
| Total events | 207M | → | 266M | +29% (peaked 376M in 2025) |
| Single-commit push % | 87% | → | 94% | +7pp |
| PR/Push ratio | 0.098 | → | 0.077 | -21% |
| Avg contributors/repo | 4.88 | → | 2.84 | -42% |

**Story:** More developers, more repos, but each repo has less activity and fewer contributors. Development is getting "lighter."

**Caveat:** 2026 WatchEvent -63% / ForkEvent -67% due to GitHub fake star account cleanup (ICSE 2026 study: ~6M fake stars removed).

---

### Slide 12 — Finding: Weekend/Weekday Gap Disappeared

**Visual:** Bar chart — weekend event % across 5 eras, with 28.9% natural baseline marked

| Era | Weekend % | vs Natural (28.9%) |
|-----|-----------|-------------------|
| 2022-Q1 | 25.8% | -3.1pp (humans rest) |
| 2023-Q1 | 23.7% | -5.2pp |
| 2024-Q1 | 26.3% | -2.6pp |
| 2025-Q1 | 26.9% | -2.0pp |
| 2026-Q1 | **29.7%** | **+0.8pp** (gap gone) |

**Push-specific weekend ratio:** 24.8% → **29.4%** (strongest signal — push = writing code)

**Punchline:** _"In 2026, GitHub doesn't know what day of the week it is anymore."_

---

### Slide 13 — Finding: Two Phases of the Agent Era

**Visual:** Dual-axis chart — (left axis) total push actors, (right axis) >50 pushes/day accounts

| | 2024 | 2025 (explosion) | 2026 (normalization) |
|--|------|-----------------|---------------------|
| Push actors | 5.89M | 6.95M | **8.71M** (+25%) |
| >1000 push accounts | 5,347 | **9,788** (↑83%) | 6,351 (↓35%) |
| >50 pushes/day | 6,197 | **12,743** (↑106%) | 6,575 (↓48%) |
| Avg pushes/actor | 52.2 | 45.1 | **25.8** (↓43%) |
| Median pushes | 6 | 6 | **4** (↓33%) |

**Phase 1 (2025):** Agent explosion — high-frequency pushers tripled, early adopters running agents at superhuman rates.

**Phase 2 (2026):** Normalization — GitHub cleaned bot accounts, agents became more efficient, participation broadened but individual intensity dropped.

**Punchline:** _"The coding agent era's signature: more people push, but each person pushes less. Development shifted from 'few people, high intensity' to 'many people, lightweight participation.'"_

---

### Slide 14 — Summary & Takeaways

**Three key takeaways:**

1. **Stars ≠ Health** — PyPI + HF downloads predict real adoption 10x better than star counts
2. **The Agent Era has two phases** — 2025 explosion (bots tripled) → 2026 normalization (broader, lighter participation)
3. **Weekend disappeared** — 29.7% weekend activity = agents work 24/7; contributor concentration increased

**Scale achieved:** 1.46B events, 87.9 GB, 10 Spark jobs, 5-year longitudinal analysis

**Methodology:** Framework-agnostic, extensible to any OSS domain

---

### Slide 15 — Acknowledgements

- NYU High Performance Computing — Google Cloud Dataproc cluster
- GH Archive — Open hourly event logs
- Hugging Face Hub — Public model metadata API
- PyPI — Public download statistics

---

## Presentation Tips

1. **Time allocation** (assuming 15-min slot):
   - Act 1 (Setup): 3 min
   - Act 2 (Journey): 5 min
   - Act 3 (Findings): 6 min
   - Summary: 1 min

2. **每張 Finding slide 的模式：**
   - 上半：一張大圖
   - 下半：一句 punchline quote
   - 口述補充數據，不放在 slide 上

3. **Demo moment（如果有時間）：**
   - 展示 HDFS 上的資料結構
   - 或 live 跑一個輕量 Spark query

4. **Backup slides（不講但準備好被問）：**
   - Data quality check 完整表格
   - Health score formula 細節
   - 2026 schema change 原始 JSON 對比
   - Weekend by event type 完整分拆
