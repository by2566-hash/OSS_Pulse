# OSS Pulse — EDA Progress Log
_Updated: 2026-05-01_

---

## GH Archive EDA（本地 3hr sample，2025-01-16）

| 指標 | 數值 |
|------|------|
| Records (3hr) | 651,244 |
| Core events / total | 510,346 / 651,244 (78.4%) |
| Distinct repos | 214,854 |
| Distinct actors | 109,014 |
| Null 欄位 | `org` 73% null（正常） |

**Event 分佈：** PushEvent 65% > PullRequestEvent 6.6% > WatchEvent 3.7% > IssuesEvent 1.6% > ForkEvent 0.9%

**⚠️ 問題：** Top repos 全是 spam/bot（commit inflation、NFT、hourly bot），需要 seed list 過濾。

---

## PyPI 資料

- 46 個 library，552 筆月下載量（2025 全年）
- 本機 + HDFS 同步完成

---

## HF Models（HDFS）

- 2,815,064 筆 cleaned，路徑 `/user/jl17797_nyu_edu/oss_pulse/cleaned/huggingface_hub`

---

## 下一步（依優先順序）

- [ ] by2566 開 ACL → 存取 GH Archive 全量
- [ ] 建 AI repo seed list（過濾 spam）
- [ ] Spark SQL `repo_daily_metrics`
- [ ] HF ↔ GH Join（用 model card GitHub URL）
- [ ] 三方合併：GH + HF + PyPI

---

## Seed List（2026-05-01）

- 93 個 AI/ML repos，14 個類別
- 39 個有 HF library 對應（可 join PyPI）
- 本機：`data/seed_repos.json`，HDFS：`source/seed_repos.json`

**下一步更新：**
- [x] 建 AI repo seed list
- [ ] by2566 開 ACL → Spark 過濾 GH Archive 只留 seed repos
- [ ] Spark SQL `repo_daily_metrics`
- [ ] HF ↔ GH ↔ PyPI 三方 Join

---

## repo_daily_metrics（2026-05-01）

- **24,001 筆**，82 個 repo，最多 334 天
- 欄位：`repo_name`, `event_date`, `stars`, `forks`, `pushes`, `prs`, `issues`, `distinct_actors`, `total_events`
- HDFS：`/user/jl17797_nyu_edu/oss_pulse/analytics/repo_daily_metrics`

**Top 5 by Stars 2025：** ollama (44K) > ComfyUI (24K) > unsloth (23K) > vllm (22K) > LLaMA-Factory (19K)

**下一步（依優先順序）：**
- [x] 建 AI repo seed list
- [x] Spark SQL `repo_daily_metrics`
- [ ] HF ↔ GH Join（HF 下載量 + GitHub stars 交叉分析）
- [ ] PyPI ↔ GH Join（library 下載量 + repo 活躍度）
- [ ] Health Score 計算
- [ ] Streaming Demo

---

## HF ↔ GH Join（2026-05-02）

- 36 個 repo 有完整三方資料（HF + GH + seed category）
- HDFS：`analytics/hf_gh_join`，本機：`data/hf_gh_join.csv`

**關鍵發現（Hype vs Real Adoption）：**
- `sentence-transformers`：519M HF下載 vs 只有 1,588 GH stars → **被低估**
- `pyannote-audio`：44M HF下載 vs 1,506 GH stars → **被低估（專業使用）**
- `ollama`：44,520 GH stars vs 241 HF下載 → **純知名度，非 HF 生態**
- `vllm`：22,697 GH stars vs 11M HF下載 → **兩者都強**

**下一步：**
- [x] HF ↔ GH Join
- [ ] 加入 PyPI 下載量 → 三方合併
- [ ] Health Score 計算
- [ ] Streaming Demo

---

## 三方合併 + Health Score（2026-05-02）

- 36 個 repo，欄位：HF + GH + PyPI + health_score
- HDFS：`analytics/health_score`，本機：`data/health_score.csv`

**Health Score 權重：**
- HF 下載量 30%、PyPI 下載量 20%、GH stars 15%、GH pushes 15%、GH PRs 10%、active days 10%

**Top 5 Health Score：**
1. huggingface/transformers (14.68) — 全方位最強
2. ultralytics/ultralytics (12.77)
3. vllm-project/vllm (12.74)
4. ukplab/sentence-transformers (12.73) — GH低估
5. huggingface/diffusers (12.57)

**有趣發現：**
- `ollama` (44K GH stars) health score 只有 9.3，低於 sentence-transformers (12.7) → 純知名度
- `scikit-learn` PyPI 14億下載，但 HF 幾乎沒有存在感 → 傳統 ML 生態

**下一步：**
- [x] PyPI ↔ HF ↔ GH 三方合併
- [x] Health Score 計算
- [ ] Streaming Demo
- [ ] Frontend 規劃
