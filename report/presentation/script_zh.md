# OSS Pulse — 簡報逐字稿（中文版）

Group 9 | Jhe Chen Li (jl17797) + Bo Yu (by2566)
預估時長：15 分鐘

---

## Slide 1 — Title

大家好，我們是 Group 9。專案叫 OSS Pulse，研究 coding agent 如何改變開源開發。我們用了 14.6 億筆 GitHub 事件，橫跨五年。

---

## Slide 2 — Abstract & Platform

先講結論。Coding agent 時代帶來三個轉變：

第一，開發變輕了——開發者增加了，但每個 repo 的貢獻者反而變少。

第二，週末消失了——活動量在每天之間變得均勻。

第三，agent 採用分兩階段——2025 年爆發，2026 年正常化。

我們用 Spark 跑在 NYU 的 Dataproc 叢集上，930 GB 原始資料，清洗後約 88 GB，10 個分析 job。

---

## Slide 3 — Motivation

為什麼做這個？因為傳統指標在誤導人。

DeepSeek-R1 星星數排第二，但只有 5 個人在推 code。Star 多不代表健康。

而且時間點剛好——我們有五個 snapshot，從 2022 年 ChatGPT 之前，到 2026 年 agent 飽和，下面有 timeline。

---

## Slide 4 — Goodness

數據品質。四個核心欄位在五個 era 都零 null。

2026 年我們額外做了 re-validation，重新下載全部原始檔重新清洗，差異小於 0.4%。580 個損壞檔案用 ignoreCorruptFiles 處理。

2026 有 schema 變更，Slide 9 會講。

---

## Slide 5 — Data Sources

三個資料來源。GitHub Archive 的事件日誌，14.6 億筆。Hugging Face Hub 的模型 metadata，280 萬個模型。PyPI BigQuery 的下載量，46 個 AI library。

---

## Slide 6 — Data Samples

這頁展示各來源的 schema 和樣本。快速帶過。

---

## Slide 7 — Design Diagram

Pipeline 架構。三個來源進 Spark 清洗，然後分成 10 個分析 job——從健康分數到跨年代比較到開發節奏分析。

---

## Slide 8 — Code Challenge: Jhe Chen Li

第一個挑戰。需要 700 GB 但 quota 只有 500 GB。解法：rolling pipeline——下載、清洗、刪原始檔、重複。全程不超標。

第二個問題：五年聯集太大，executor 記憶體不夠，YARN 直接 kill。改成 per-era 處理——一次算一個 era，寫中介檔，釋放記憶體。Crash-safe 而且可以 resume。

---

## Slide 9 — Code Challenge: 2026 Schema Change

第二個挑戰。Job 8 算出 2026 年 merged PR 是 0，以為是 bug。

結果是 GitHub 在 2025 年底改了 API schema。merged 欄位變 NULL，改用新的 action 值。表格裡有 diff。

我們寫了向後相容的 OR 條件修復，然後重新驗證，差異小於 0.4%。

---

## Slide 10 — Code Challenge: Bo Yu

第三個挑戰：billion-record shuffle。對 10 億筆事件按 repo 分組，shuffle 83 GB，跑了三小時多。

我們調了 partition 數、加 cache、換掉 Python UDF。如果重來會在 ingest 階段就 bucket by repo name——直接省掉整個 shuffle。

---

## Slide 11 — Stars ≠ Health

進入 findings。先講背景觀察：stars 不等於健康。

三個典型。Transformers 全面領先。Sentence-transformers 星星很少但下載量巨大，是隱形冠軍。Ollama 星星和 PyPI 都很高但 HF 幾乎沒有——因為它用自己的 model registry。

沒有單一指標能衡量所有專案，需要三角驗證。

---

## Slide 12 — Event Composition

五年事件組成。重點：2026 總事件量比 2025 高峰掉了約 30%，但主因是 GitHub 清除了不真實帳號。

Popularity signal 受創最重——star 掉 63%、fork 掉 67%。但 push 的佔比反而從 71% 升到 85%。Engineering activity 受影響最小。

---

## Slide 13 — Paper Reference

為什麼確定是帳號清除？這篇 arXiv 論文發現約 600 萬個假星號，90% 被標記的 repo 後來被刪除，刪除率是正常的 16 倍。

我們的 2026 數據跟他們的發現完全吻合。

---

## Slide 14 — 5-Year Ecosystem Shift

核心發現。

開發者增加超過 50%。但總事件量在 2025 達到高峰後回落。每個 repo 的貢獻者減少超過 40%。

更多人在開發，但每個專案得到的關注更少。開發正在變輕。

---

## Slide 15 — Bus Factor

貢獻者集中度分析。這張圖顯示每個 repo 最大貢獻者佔全部 push 的比例。

紅色代表高集中度。但集中度不一定代表風險——tensorflow 的最大 pusher 是 CI bot，不是真人。要搭配 PR 貢獻者數一起看。

---

## Slide 16 — Bus Factor Evidence

GitHub contributor 頁面的截圖。

左邊 open-webui：一個人的 commit 量是第二名的 24 倍。真正的 bus factor 風險。

右邊 tensorflow：第一名是 CI bot。一樣的高集中度，完全不同的故事。

---

## Slide 17 — Weekend Gap Disappeared

我最喜歡的發現。週末活動的自然比例大約是 29%。2022 年是 26%——人類週末會休息。到 2026 年達到近 30%——差距消失了。

Push 的週末比例也是同樣趨勢。2026 年的 GitHub 已經分不出星期幾了。Agent 是 24/7 在跑的。

---

## Slide 18 — Two Phases of the Agent Era

最後一個發現。Agent 時代分兩階段。

2025 是爆發期——高頻 push 帳號翻了一倍多。早期使用者用 agent 以超人類速度推 code。

2026 是正常化——高頻帳號降回來了，因為 GitHub 清了 bot。但總 push 人數持續成長。更多人在 push，但每個人 push 得更少。

這就是 coding agent 時代的特徵。

---

## Slide 19 — Lessons Learned

三個教訓。

第一，design for the constraint。Quota 限制反而逼出了更好的架構。

第二，never trust a live API schema。2026 的變更默默地讓 query 壞掉——一定要 assert 你的假設。

第三，用外部來源驗證異常值。arXiv 論文幫我們確認 2026 數據不是 bug。

---

## Slide 20 — Summary & Acknowledgements

三個結論。

開發變輕了——開發者更多，每個 repo 貢獻者更少。

週末消失了——agent 24/7 不休息。

兩階段採用——2025 爆發，2026 正常化。

傳統指標如 stars 越來越不可靠，我們需要多維度的衡量。

感謝 NYU HPC、GitHub Archive、Hugging Face、PyPI。謝謝大家。
