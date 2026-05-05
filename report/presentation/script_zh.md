# OSS Pulse — 簡報逐字稿（中文版）

Group 9 | Jhe Chen Li (jl17797) + Bo Yu (by2566)
預估時長：15 分鐘

---

## Slide 1 — Title

大家好，我們是 Group 9。我們的專案叫 OSS Pulse，研究的問題是：coding agent 如何改變了開源軟體的開發模式。我們追蹤了 2022 到 2026 年共 14.6 億筆 GitHub 事件，來回答這個問題。

---

## Slide 2 — Abstract & Platform

先講結論。我們發現 coding agent 時代帶來了三個根本性的轉變：

第一，開發變「輕」了——開發者增加了 51%，但每個 repo 的貢獻者反而少了 42%。

第二，週末消失了——2026 年的週末活動比例達到自然基線 28.9%，等於 GitHub 已經分不出星期幾。

第三，agent 的採用分成兩個階段——2025 年爆發，2026 年正常化。

我們用的是 Apache Spark 跑在 NYU 的 Google Cloud Dataproc 叢集上，總共處理了 930 GB 原始資料，清洗後 87.9 GB，跑了 10 個分析 job。

---

## Slide 3 — Motivation

為什麼要做這個研究？因為傳統指標在誤導大家。

舉個例子，DeepSeek-R1 在 GitHub 的 star 數排名第二，但它沒有 PyPI library，整個 repo 只有 5 個人在 push code。Star 多不代表生態系健康。

而且現在正好是 coding agent 時代。從 2022 年 ChatGPT 之前的 baseline，到 2023 年 ChatGPT 爆發，2024 年 LLM 百花齊放，2025 年 Cursor、Claude Code、Devin 出現，到 2026 年 agent 飽和。我們剛好可以用五年的 Q1 資料來觀察這個變化。

---

## Slide 4 — Goodness

數據品質方面。我們的四個核心欄位——event type、日期、使用者、repo 名稱——在五個 era 中完全沒有 null。

針對 2026 年的資料，我們額外做了 re-validation：重新下載了 2,160 個原始檔案，重新清洗，跟原本的結果比對，差異小於 0.39%。另外 2026 年 4 月有 580 個損壞的 gz 檔，我們用 Spark 的 ignoreCorruptFiles 來容錯。

值得注意的是 2026 年有 schema 變更，等一下 Slide 9 會詳細講。

---

## Slide 5 — Data Sources

資料來源有三個。

第一是 GitHub Archive，每小時的事件日誌，涵蓋 Watch、Fork、Push、PR、Issue 五種事件類型，五年 Q1 共 14.6 億筆。

第二是 Hugging Face Hub，2026 年 4 月的 snapshot，281 萬個模型。

第三是 PyPI BigQuery，46 個 AI/ML library 的月下載量。

---

## Slide 6 — Data Samples

這頁展示三個資料來源的 schema 和樣本資料。GH Archive 清洗後統一成 10 個欄位，HF Hub 包含 model ID、library、下載量等，PyPI 是最簡單的 project、月份、下載量三欄。整體清洗後是 87.9 GB Parquet。

（這頁可以快速帶過）

---

## Slide 7 — Design Diagram

Pipeline 架構。三個資料來源進來後，先經過 Spark 做清洗跟去重，統一 schema，按日期 partition。然後下游分成五組 job：Job 1 到 3 做每日指標、HF 跟 GH 交叉比對、健康分數；Job 4 算全 GitHub Top 1000 repo；Job 5 到 7 做 AI vs 非 AI 比較、star hype 偵測、貢獻者集中度分析；Job 8 做五年跨 era 比較；Job 9 和 10 做 repo 深入分析和開發節奏分析。

---

## Slide 8 — Code Challenge: Jhe Chen Li

第一個工程挑戰。我們的 HDFS quota 只有 500 GB，但五年 Q1 的原始資料需要 700 GB。解法是 rolling pipeline——下載一年、清洗成 Parquet（體積縮小 8 倍）、刪掉原始檔、再處理下一年。這樣全程不超過 500 GB。

第二個問題是 90 GB 的五年聯集超過 executor 的 48 GB 記憶體。改成 per-era 處理——每次只讀一個 era、算完寫中介檔、釋放記憶體。這樣不僅記憶體降到 15-23 GB，而且如果 YARN kill 了 job，已完成的 era 不用重跑，是 crash-safe 的設計。

---

## Slide 9 — Code Challenge: 2026 Schema Change

第二個挑戰更隱蔽。Job 8 算出 2026 年的 merged PR 數量是 0，一開始以為是 bug。結果下載了 2025 和 2026 的原始 JSON 來比對，發現 GitHub 在 2025 年 10 月簡化了 API schema。

具體來說，pr.merged 欄位從 true/false 變成 NULL，取而代之的是 payload.action 多了一個 "merged" 值。push_distinct_size 和 commits 也被完全移除了。

修復方式是寫一個向後相容的 OR 條件——舊 schema 用 action=closed AND pr_merged=true，新 schema 用 action=merged。驗證方式是重新下載全部 2,160 個原始檔案，重新清洗，比對差異小於 0.39%。

---

## Slide 10 — Code Challenge: Bo Yu

第三個挑戰是 billion-record shuffle。要算全 GitHub Top 1000 repo，需要對 10 億筆事件按 repo name 做 group by，跨 334 個日期 partition，shuffle 量達到 83 GB，跑了三個多小時。

我們做了幾個優化：調整 shuffle partition 數量從 400 降到 200 來配合叢集資源、用 cache 避免重複 shuffle、把 Python UDF 換成原生的 Spark function。

如果重來一次，我們會在 ingest 階段就用 bucketBy repo_name，這樣下游 query 就完全不需要 shuffle——83 GB 的 shuffle 可以降到接近零。

---

## Slide 11 — Stars ≠ Health

進入 findings。先講一個背景觀察：Stars 不等於健康。

這張 bubble chart 把 PyPI 下載量放在 x 軸、GitHub stars 放在 y 軸、泡泡大小代表 HF 模型數量。

可以看到三種典型：transformers 是真正的領導者，三個指標全贏。sentence-transformers 只有 1,600 顆星，但 HF 下載量 5.19 億——每一顆星背後是 327,014 次下載，是隱形冠軍。Ollama 有 44,500 顆星加上 4,790 萬 PyPI 下載，但 HF 只有 241——因為它是 runtime，用自己的 model registry，不走 HF 生態。

重點是：沒有單一指標能抓住所有專案，三角驗證才是正確做法。

---

## Slide 12 — Event Composition

接下來看五年的事件組成變化。這張 stacked bar chart 展示每個 era 的事件類型分布。

最關鍵的觀察：2026 年總事件量從 2025 年的高峰下降了 29%，但主要是因為 GitHub 清除了大量不真實帳號。受影響最大的是 popularity signal——Watch 也就是 star 事件暴跌 63%，Fork 暴跌 67%。而 Push 事件的佔比反而從 71% 升到了 85%。

也就是說，engineering activity 受影響最小，被清掉的主要是按星號和 fork 的行為。

---

## Slide 13 — Paper Reference

我們為什麼這麼有信心說這是帳號清除？因為有外部文獻佐證。

這篇 2024 年底的 arXiv 論文發現 GitHub 上有大約 600 萬個可疑的假星號。90% 被標記的 repo 後來被觀察到已刪除，刪除率是正常的 16 倍。

我們的 2026 年 Watch 和 Fork 暴跌完全吻合他們的觀察。我們選擇誠實報告這個 anomaly——這是觀察到的平台行為，不是官方公告。

---

## Slide 14 — 5-Year Ecosystem Shift

這是我們的核心發現。

五年間，開發者從 700 萬成長到 1,060 萬，增加了 51%。但總事件量在 2025 年達到 3.76 億的高峰後，2026 年回落到 2.66 億。

更重要的是，每個 repo 的平均貢獻者數從 4.88 降到 2.84，下降了 42%。更多人在開發，但每個專案的參與者反而更少了。開發正在變「輕」——更多人、更小的 commit、更少的深度參與。

---

## Slide 15 — Bus Factor

我們也分析了貢獻者集中度。這張圖顯示各 repo 的 top-1 push ratio——也就是最大貢獻者佔全部 push 的比例。

紅色代表高風險：open-webui 的 top-1 ratio 高達 0.957，awesome 類 repo 也很高。但這裡要注意——concentration 不一定代表 fragility。tensorflow 的 ratio 是 0.99，但那是 CI bot，不是真人。要搭配 PR 貢獻者數一起看才有意義。

---

## Slide 16 — Bus Factor Evidence

為了證明這一點，我們直接截圖了 GitHub 的 contributor insight。

左邊是 open-webui：tjbck 一個人貢獻了 11,683 個 commit，第二名只有 479 個，差距 24 倍。這是真正的 bus factor 風險——如果這個人離開，整個專案可能停擺。

右邊是 tensorflow：第一名 tensorflower-gardener 有 57,761 個 commit，但它是 CI bot。看起來集中度很高，但這是自動化，不是真人依賴。

同樣的數字，完全不同的故事。

---

## Slide 17 — Weekend Gap Disappeared

這是我最喜歡的發現。我們計算了每個 era 的週末事件佔比。Q1 有大約 90 天，其中 26 天是週末，自然比例是 28.9%。

2022 年是 25.8%，低於自然比例 3.1 個百分點——因為人類週末會休息。2023 年更低，只有 23.8%。然後逐年回升，到 2026 年達到 29.7%，首次超過自然基線。

尤其是 Push 事件的週末比例從 24.8% 升到 29.4%——Push 代表實際在寫 code，這是最強的信號。2026 年的 GitHub 已經分不出星期幾了。Coding agent 是 24/7 在跑的。

---

## Slide 18 — Two Phases of the Agent Era

最後一個發現，也是整個故事的高潮。Agent 時代分成兩個階段。

2025 年是爆發期：每天 push 超過 50 次的帳號從 6,197 暴增到 12,743，翻了一倍多。這是早期 agent 使用者以超人類的頻率在推 code。

但到 2026 年，這些高頻帳號反而降回 6,575——因為 GitHub 清理了 bot 帳號。同時，push 的總人數從 695 萬增加到 871 萬，成長 25%。也就是說，bot 的高峰過去了，取而代之的是更廣泛但更輕量的參與。

更多人在 push，但每個人 push 得更少了。這就是 coding agent 時代的特徵。

---

## Slide 19 — Lessons Learned

三個 lesson learned。

第一，design for the constraint——HDFS quota 和權限限制反而逼出了更好的架構設計，rolling pipeline 比無限空間的版本更 crash-safe。

第二，never trust a live API schema。2026 的 schema 變更沒有大張旗鼓地宣布，Job 8 默默地算出 merged PR 等於 0，直到我們手動檢查才發現。以後一定要 assert column count、null ratio、key value range。

第三，validate the unexpected with outside sources。當 2026 年的 Watch 和 Fork 暴跌看起來像 bug 的時候，是外部論文幫我們確認這是真實的平台行為。異常值需要外部佐證。

---

## Slide 20 — Summary & Acknowledgements

總結。Coding agent 時代帶來三個根本性的轉變：

第一，Development got lighter——開發者多了 51%，但每個 repo 的貢獻者少了 42%，single-commit push 從 87% 升到 94%。

第二，Weekends disappeared——2026 年週末活動比例達到 29.7%，agent 是 24/7 不休息的。

第三，Two-phase adoption——2025 年爆發、2026 年正常化，從少數人高強度使用變成多數人輕量參與。

Implication 是：在 agent 時代，stars 和 fork 數這些傳統指標越來越不可靠，我們需要多維度的衡量方式。

感謝 NYU HPC 提供的 Dataproc 叢集，以及 GH Archive、Hugging Face、PyPI 的公開數據。謝謝大家。
