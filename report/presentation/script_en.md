# OSS Pulse — Presentation Script (English)

Group 9 | Jhe Chen Li (jl17797) + Bo Yu (by2566)
Estimated duration: 15 minutes

---

## Slide 1 — Title

Hi everyone, we're Group 9. Our project is called OSS Pulse. We're studying how coding agents reshaped open-source development — using 1.46 billion GitHub events over five years.

---

## Slide 2 — Abstract & Platform

Here's our conclusion upfront. We found three shifts in the coding agent era.

First, development got lighter — more developers, but fewer contributors per repo.

Second, weekends disappeared — activity is now evenly spread across all days.

Third, agent adoption had two phases — explosion in 2025, then normalization in 2026.

We ran everything on Spark, on NYU's Dataproc cluster. 930 GB raw, cleaned down to about 88 GB, across 10 analytics jobs.

---

## Slide 3 — Motivation

Why does this matter? Because traditional metrics are misleading.

DeepSeek-R1 is number two on GitHub by stars — but only 5 people actually push code. Stars don't equal health.

And the timing is right. We have five snapshots: pre-ChatGPT in 2022, all the way to coding agent saturation in 2026. You can see the timeline at the bottom.

---

## Slide 4 — Goodness

Data quality. Zero nulls in our four core fields across all five eras.

For 2026, we re-downloaded all raw files and re-cleaned them.

One caveat: 2026 had a schema breaking change — I'll cover that on Slide 9.

---

## Slide 5 — Data Sources

Three data sources. GitHub Archive for event logs — 1.46 billion events. Hugging Face Hub for model metadata — 2.8 million models. And PyPI via BigQuery for download counts — 46 AI libraries.

---

## Slide 6 — Data Samples

This shows sample rows and schemas for each source. You can see the fields we kept after cleaning. I'll move on quickly.

---

## Slide 7 — Design Diagram

Pipeline architecture. Three sources feed into Spark for cleaning, then fan out into 10 analytics jobs — from health scores to era comparisons to development rhythm analysis.

---

## Slide 8 — Code Challenge: Jhe Chen Li

First challenge. We needed 700 GB but only had 500 GB quota. Solution: a rolling pipeline — download, clean, delete raw, repeat. Stays under quota the whole time.

We switched to per-era processing — compute one era at a time, save intermediates, release memory. Crash-safe.

---

## Slide 9 — Code Challenge: 2026 Schema Change

Second challenge. Job 8 reported zero merged PRs for 2026. We thought it was a bug.

Turns out GitHub changed their API schema in late 2025. The merged field became NULL, replaced by a new action value. You can see the diff in the table.

We wrote a backward-compatible fix — an OR clause that handles both old and new schemas. Then we re-validated everything

---

## Slide 10 — Code Challenge: Bo Yu

Third challenge: billion-record shuffle. Grouping by repo across a billion events produced an 83 GB shuffle. Over three hours.

We tuned partitions, cached DataFrames, and replaced Python UDFs with native Spark functions. If we did it again, we'd bucket by repo name at ingest — that eliminates the shuffle entirely.

---

## Slide 11 — Stars ≠ Health

Now, findings. First, a background observation: stars don't equal health.

Three archetypes here. Transformers wins on all signals. Sentence-transformers has very few stars but massive downloads. And Ollama has tons of stars and PyPI downloads but almost nothing on HF — because it uses its own model registry.

No single metric works for every project. You need triangulation.

---

## Slide 12 — Event Composition

Five-year event composition. The key point: 2026 total events dropped about 30% from the 2025 peak — but it was mainly GitHub removing inauthentic accounts.

Popularity signals got hit hardest — stars down 63%, forks down 67%. But push's share of all events actually grew from 71% to 85%. Engineering activity was least affected.

---

## Slide 13 — Paper Reference

Why are we confident this was an account cleanup? This arXiv paper found about 6 million fake stars on GitHub. 90% of flagged repos were later deleted.

Our 2026 data aligns directly with their findings.

---

## Slide 14 — 5-Year Ecosystem Shift

This is our core finding.

Developers grew over 50%. But total events peaked in 2025 and dropped in 2026. And contributors per repo fell over 40%.

More people developing, but each project gets less attention. Development is getting lighter.

---

## Slide 15 — Bus Factor

Contributor concentration. This chart shows how much the top contributor dominates each repo.

Red means high concentration. But concentration alone doesn't prove fragility — tensorflow's top pusher is a CI bot, not a human. You need to check PR contributors too.

---

## Slide 16 — Bus Factor Evidence

Here's proof from GitHub's own contributor page.

Left: open-webui — one person made 24 times more commits than the second contributor. Real bus-factor risk.

Right: tensorflow — the top contributor is a CI bot. Same high concentration, completely different story.

---

## Slide 17 — Weekend Gap Disappeared

My favorite finding. Weekend activity should naturally be about 29% of total. In 2022, it was 26% — humans rest on weekends. By 2026, it reached nearly 30% — the gap is gone.

Push-specific weekend ratio shows the same trend. In 2026, GitHub can't tell what day it is anymore. Agents work around the clock.

---

## Slide 18 — Two Phases of the Agent Era

Final finding. The agent era has two phases.

2025 was the explosion — high-frequency push accounts more than doubled. Early adopters running agents at superhuman speed.

2026 was the normalization — those high-frequency accounts dropped back as GitHub cleaned up bots. But total push actors kept growing. More people pushing, each one pushing less.

That's the signature of the coding agent era.

---

## Slide 19 — Lessons Learned

Three lessons.

First, design for the constraint. The quota limitation forced a rolling pipeline that ended up being more crash-safe.

Second, never trust a live API schema. The 2026 change silently broke our query — always assert your assumptions.

Third, validate anomalies with outside sources. The arXiv paper confirmed our 2026 data wasn't a bug.

---

## Slide 20 — Summary & Acknowledgements

Three takeaways.

Development got lighter — more developers, fewer contributors per repo.

Weekends disappeared — agents work 24/7.

Two-phase adoption — 2025 explosion, 2026 normalization.

Traditional metrics like stars are increasingly unreliable. Multi-dimensional measurement is essential.

Thanks to NYU HPC, GitHub Archive, Hugging Face, and PyPI. Thank you.
