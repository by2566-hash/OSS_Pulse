# OSS Pulse — Presentation Script (English)

Group 9 | Jhe Chen Li (jl17797) + Bo Yu (by2566)
Estimated duration: 15 minutes

---

## Slide 1 — Title

Hi everyone, we're Group 9. Our project is called OSS Pulse, and we're investigating how coding agents have reshaped open-source development. We tracked 1.46 billion GitHub events from 2022 to 2026 to answer this question.

---

## Slide 2 — Abstract & Platform

Let me start with the conclusion. We found that the coding agent era brought three fundamental shifts.

First, development got lighter — developers grew by 51%, but contributors per repo dropped by 42%.

Second, weekends disappeared — weekend activity in 2026 reached the natural baseline of 28.9%, meaning GitHub can no longer tell what day of the week it is.

Third, agent adoption happened in two phases — a 2025 explosion followed by 2026 normalization.

We ran this on Apache Spark on NYU's Google Cloud Dataproc cluster. We processed 930 GB of raw data, cleaned it down to 87.9 GB, and ran 10 analytics jobs.

---

## Slide 3 — Motivation

Why does this matter? Because traditional metrics are misleading.

Here's an example: DeepSeek-R1 ranks number two on GitHub by stars, but it has no PyPI library, and only 5 people actually push code to the repo. Stars don't equal ecosystem health.

And the timing is perfect. We have five Q1 snapshots spanning from the pre-ChatGPT baseline in 2022, through the ChatGPT explosion in 2023, the LLM bloom in 2024 with GPT-4, Claude, and Gemini, the year of agents in 2025 with Cursor, Claude Code, and Devin, to coding agent saturation in 2026.

---

## Slide 4 — Goodness

On data quality. Our four core fields — event type, date, actor login, and repo name — have zero nulls across all five eras.

For 2026 specifically, we did an extra re-validation: we re-downloaded all 2,160 raw files, re-cleaned them, and compared against our original output. The diff was less than 0.39%. We also tolerated 580 corrupt gzip files from April 2026 using Spark's ignoreCorruptFiles option.

Note that 2026 had a schema breaking change, which I'll cover in detail on Slide 9.

---

## Slide 5 — Data Sources

We used three data sources.

First, GitHub Archive — hourly event logs covering Watch, Fork, Push, PR, and Issue events. Five Q1 snapshots totaling 1.46 billion events.

Second, Hugging Face Hub — an April 2026 snapshot of 2.8 million public models.

Third, PyPI via BigQuery — monthly download counts for 46 AI and ML libraries.

---

## Slide 6 — Data Samples

This slide shows the schema and sample rows for each data source. GitHub Archive was cleaned into a unified 10-column schema, Hugging Face Hub includes model ID, library name, downloads, and likes, and PyPI is simply project, month, and download count. Total cleaned size is 87.9 GB in Parquet format.

(This slide can be covered quickly.)

---

## Slide 7 — Design Diagram

Here's our pipeline architecture. Three data sources feed into a Spark cleaning and deduplication layer with a unified schema, partitioned by event date. Downstream, we have five groups of jobs: Jobs 1 through 3 handle daily metrics, HF-GitHub joins, and health scores. Job 4 computes the top 1,000 repos across all of GitHub — that's the billion-record shuffle. Jobs 5 through 7 compare AI versus general repos, detect star hype, and analyze contributor health. Job 8 does the five-year cross-era comparison. And Jobs 9 and 10 do per-repo deep dives and development rhythm analysis.

---

## Slide 8 — Code Challenge: Jhe Chen Li

First engineering challenge. Our HDFS quota was only 500 GB, but five years of Q1 raw data needed 700 GB. The solution was a rolling pipeline — download one year, clean it to Parquet which is 8x smaller, delete the raw files, then move to the next year. This kept us within 500 GB at all times.

The second problem was that a 90 GB union of all five eras exceeded our 48 GB executor memory. YARN killed it after 1.5 hours. We switched to per-era processing — read one era at a time, compute metrics, write intermediate CSVs, then unpersist. This brought peak memory down to 15-23 GB, and made the job crash-safe and resumable. If YARN kills mid-job, completed eras are preserved.

---

## Slide 9 — Code Challenge: 2026 Schema Change

The second challenge was more insidious. Job 8 reported merged PRs equals zero for 2026 Q1. At first we thought it was a bug.

We downloaded raw JSON from both 2025 and 2026 and compared the payload keys directly. It turned out GitHub simplified their API schema in October 2025. The pr.merged field went from true/false to NULL. Instead, payload.action gained a new "merged" value. The push_distinct_size and commits fields were removed entirely.

The fix was a backward-compatible OR clause — for the old schema, check action equals "closed" AND pr_merged equals true; for the new schema, check action equals "merged". We validated by re-downloading all 2,160 raw files, re-cleaning, and confirming the diff was less than 0.39%.

---

## Slide 10 — Code Challenge: Bo Yu

Third challenge: billion-record shuffle. Computing the top 1,000 repos across one billion events required grouping by repo name across 334 date partitions. That's an 83 GB shuffle that took over three hours.

We optimized by tuning shuffle partitions from 400 down to 200 to match our cluster, caching aggregated DataFrames to avoid redundant shuffles, replacing Python UDFs with native Spark functions, and restructuring downstream jobs to read pre-aggregated output.

If we did it again, we'd bucket the cleaned data by repo name at ingest time. That would eliminate the 83 GB shuffle entirely — hours down to minutes.

---

## Slide 11 — Stars ≠ Health

Now let's get into findings. First, a background observation: stars do not equal health.

This bubble chart plots PyPI downloads on the x-axis, GitHub stars on the y-axis, and bubble size represents HF model count.

We see three archetypes. Transformers is the true leader — it wins on all three signals. Sentence-transformers has only 1,600 stars but 519 million HF downloads — that's 327,014 downloads per star, a quiet powerhouse. And Ollama has 44,500 stars plus 47.9 million PyPI downloads but only 241 HF downloads — because it's a runtime with its own model registry, not part of the HF ecosystem.

The point: no single signal captures every project. Triangulation is essential.

---

## Slide 12 — Event Composition

Next, let's look at how event composition changed over five years. This stacked bar chart shows the breakdown by event type for each era.

The key observation: 2026 total events fell 29% from the 2025 peak, but this was primarily driven by GitHub's removal of inauthentic accounts. Popularity signals were hit hardest — Watch events, which represent stars, collapsed by 63%, and Forks collapsed by 67%. Meanwhile, Push's share of total events actually grew from 71% to 85%.

In other words, engineering activity was least affected. What got cleaned out was the click-a-button behavior — starring and forking.

---

## Slide 13 — Paper Reference

Why are we confident this was an account cleanup? Because we have external corroboration.

This late-2024 arXiv paper identified approximately 6 million suspected fake stars on GitHub. 90% of flagged repositories were later observed to be deleted, at 16 times the normal deletion rate.

Our 2026 WatchEvent and ForkEvent collapse aligns directly with their observations. We chose to be transparent about this anomaly — we report observations, not an official GitHub announcement.

---

## Slide 14 — 5-Year Ecosystem Shift

This is our core finding.

Over five years, developers grew from 7.0 million to 10.6 million — a 51% increase. But total events peaked at 376 million in 2025 and fell back to 266 million in 2026.

More importantly, average contributors per repo dropped from 4.88 to 2.84 — a 42% decline. More people are developing, but each project has fewer contributors. Development is getting "lighter" — more people, smaller commits, less deep participation.

---

## Slide 15 — Bus Factor

We also analyzed contributor concentration. This chart shows the top-1 push ratio for selected repos — the share of all pushes made by the single largest contributor.

Red means high risk: open-webui's top-1 ratio is 0.957, and awesome-list repos are similar. But here's the nuance — concentration doesn't automatically prove fragility. Tensorflow's ratio is 0.99, but that's a CI bot, not a human. You need to read this together with PR contributor counts to get the real picture.

---

## Slide 16 — Bus Factor Evidence

To prove this point, we took screenshots directly from GitHub's contributor insights page.

On the left, open-webui: tjbck made 11,683 commits, versus 479 for the number two contributor — a 24x gap. This is genuine bus-factor risk. If this one person leaves, the project could stall.

On the right, tensorflow: the top contributor tensorflower-gardener has 57,761 commits, but it's a CI bot. The concentration looks extreme, but it's automation, not human dependency.

Same numbers, completely different stories.

---

## Slide 17 — Weekend Gap Disappeared

This is my favorite finding. We calculated the weekend share of events for each era. Q1 has roughly 90 days, 26 of which are weekends, giving a natural ratio of 28.9%.

In 2022, weekend activity was 25.8% — 3.1 percentage points below natural, because humans rest on weekends. In 2023, it was even lower at 23.8%. Then it climbed year over year, reaching 29.7% in 2026 — for the first time exceeding the natural baseline.

The push-specific weekend ratio rose from 24.8% to 29.4% — and pushes represent actually writing code, so this is the strongest signal. In 2026, GitHub doesn't know what day of the week it is anymore. Coding agents work 24/7.

---

## Slide 18 — Two Phases of the Agent Era

The final finding, and the climax of our story. The agent era has two distinct phases.

2025 was the explosion: accounts pushing more than 50 times per day surged from 6,197 to 12,743 — more than doubling. These were early agent adopters pushing code at superhuman rates.

But by 2026, those high-frequency accounts dropped back to 6,575 — because GitHub cleaned up bot accounts. At the same time, total push actors grew from 6.95 million to 8.71 million, a 25% increase. The bot peak is over; what replaced it is broader but lighter participation.

More people are pushing, but each person pushes less. That's the signature of the coding agent era.

---

## Slide 19 — Lessons Learned

Three lessons learned.

First, design for the constraint, not against it. The HDFS quota and cross-team permission boundaries forced design decisions that ended up improving our work. The rolling pipeline and per-era processing were more crash-safe than a quota-free design would have been.

Second, never trust a live API schema — assert it. The 2026 GitHub schema change wasn't loudly announced. Job 8 silently produced merged PRs equals zero for an entire era before we caught it. Going forward, always assert column counts, null ratios, and key value ranges explicitly.

Third, validate the unexpected with outside sources. When the 2026 WatchEvent and ForkEvent collapse looked like a bug, cross-checking with the arXiv paper confirmed it as real platform behavior. Anomalies need external corroboration.

---

## Slide 20 — Summary & Acknowledgements

To wrap up. The coding agent era brought three fundamental shifts to open-source development.

First, development got lighter — 51% more developers, but 42% fewer contributors per repo. Single-commit pushes rose from 87% to 94%.

Second, weekends disappeared — 2026 weekend activity hit 29.7%, reaching the natural baseline. Coding agents work around the clock.

Third, two-phase adoption — 2025 explosion with automated pushers more than doubling, followed by 2026 normalization with broader but lighter participation.

The implication is clear: traditional metrics like stars and fork counts are increasingly unreliable in the agent era. Multi-dimensional measurement is essential.

Thank you to NYU HPC for the Dataproc cluster, and to GitHub Archive, Hugging Face Hub, and PyPI for the public data that made this analysis possible. Thank you.
