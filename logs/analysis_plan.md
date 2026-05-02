# OSS Pulse — 分析計畫
_Group 9 | jl17797 (Jhe Chen Li) + by2566 (Bo Yu)_
_Last updated: 2026-05-02_

---

## 一、現有資料欄位總覽

### `data/health_score.csv`（36 AI repos，三方合併）
| 欄位 | 說明 |
|------|------|
| `repo_name` | GitHub repo 名稱 |
| `category` | AI 類別（framework / vision / llm / audio...）|
| `library_name` | HF library 對應名稱 |
| `hf_model_count` | HF 上基於此 library 的模型數量 |
| `hf_total_downloads` | HF 30 天下載量（April 2026 snapshot）|
| `hf_total_likes` | HF 累積 likes |
| `gh_stars_2025` | 2025 年 GitHub star 數（WatchEvent）|
| `gh_forks_2025` | 2025 年 fork 數 |
| `gh_pushes_2025` | 2025 年 push 次數 |
| `gh_prs_2025` | 2025 年 PR 數 |
| `gh_issues_2025` | 2025 年 issue 數 |
| `gh_active_days` | 2025 年有 event 的天數 |
| `pypi_downloads_2025` | 2025 年 PyPI 總下載量 |
| `pypi_months_tracked` | 有資料的月份數 |
| `health_score` | 綜合健康分數（log 標準化加權）|

### GH Archive cleaned（HDFS，330 天，億級 events）
| 欄位 | 說明 |
|------|------|
| `event_type` | WatchEvent / ForkEvent / PushEvent / PullRequestEvent / IssuesEvent |
| `event_date` | 事件日期 |
| `actor_login` | 執行者帳號 |
| `repo_name` | repo 名稱 |
| `push_size` | 該次 push 的 commit 數 |
| `push_distinct_size` | 不重複 commit 數 |
| `commit_count` | payload 內 commits 陣列長度 |
| `pr_merged` | PR 是否被 merge |
| `issue_state` | issue 狀態（open / closed）|

### PyPI（46 libraries，2025 月度）
| 欄位 | 說明 |
|------|------|
| `project` | library 名稱 |
| `month` | 年月（YYYY-MM-01）|
| `downloads` | 當月下載次數 |

### HF Hub（2,815,064 models，April 2026 snapshot）
| 欄位 | 說明 |
|------|------|
| `model_id` | 模型 ID |
| `author` | 上傳者 |
| `library_name` | 使用的 framework |
| `pipeline_tag` | 任務類型（text-generation / image-classification...）|
| `downloads` | 30 天下載量 |
| `likes` | 累積 likes |
| `created_at` | 建立時間 |
| `parameter_count` | 參數量 |
| `has_safetensors` | 是否有 safetensors 格式 |

### Analytics Jobs 輸出（Jobs 05/06/07，跑完後可下載）
| 檔案 | 欄位 |
|------|------|
| `ai_vs_general_csv` | repo_name, is_ai, stars, forks, pushes, prs, distinct_actors, active_days |
| `star_growth_hype_csv` | repo_name, total_stars, avg_monthly_stars, peak_stars, peak_ratio, months_active, is_ai |
| `contributor_health_csv` | repo_name, total_contributors, push_contributors, top1_push_ratio, pr_contributors, is_ai |

---

## 二、研究方向

### RQ1：AI repo 與一般 repo 的生態健康度差異
- AI repo 的平均 star、fork、contributor 是否顯著高於一般 repo？
- AI repo 的活躍天數分佈是否更集中（少數明星專案 vs 長尾）？
- **資料：** `ai_vs_general_csv`（Job 05）

### RQ2：Hype 與實質健康的關係
- star 暴增（peak_ratio 高）的 repo，health_score 是否也高？
- 還是只是短暫爆紅、工程採用低（PyPI 下載少）？
- **資料：** `star_growth_hype_csv`（Job 06）+ `health_score.csv`

### RQ3：三維健康度一致性
- HF 下載量（社群使用）、PyPI 下載量（工程採用）、GH 活躍度三者是否正相關？
- 哪些 repo 在某維度突出但其他落後（outlier）？
- **資料：** `health_score.csv`

### RQ4：Contributor 集中度與維護風險
- AI repo 是否比一般 repo 更依賴單一貢獻者（top1_push_ratio 高）？
- bus factor 低的 repo 有哪些共同特徵？
- **資料：** `contributor_health_csv`（Job 07）

### RQ5：時間趨勢
- 2025 年哪個月份 AI repo 活動量最高？
- star 爆發是否對應特定事件（模型發布、論文）？
- **資料：** GH Archive 月維度聚合

### RQ6（Optional）：Coding Agent 對 OSS 生態的影響
- 2022 Q1（ChatGPT 前）vs 2025 Q1：全生態 commit 粒度、PR/push 比值、active repo 數是否有結構性位移？
- `push_distinct_size = 1` 的比例是否在 2025 顯著更高（agent-style 小 commit）？
- PR merge rate 是否上升（agent PR 品質更一致）？
- **資料：** `cleaned/gharchive_2022q1/`（下載中）+ by2566 2025 Q1
- **狀態：** 🔄 下載中（PID 1600750），完成後跑 Job 00d → 08

---

## 三、視覺化設計

### 圖1 — AI vs General：Bar chart（grouped）
- x：指標（avg_stars / avg_forks / avg_pushes / avg_contributors）
- y：數值
- 分組：AI repo（橘）vs General（灰）
- **來源：** `ai_vs_general_summary_csv`

### 圖2 — AI vs General：Box plot
- x：AI / General
- y：stars 或 active_days
- 顯示離散程度，看是否集中在少數明星專案
- **來源：** `ai_vs_general_csv`

### 圖3 — Hype vs Health：Scatter plot（最有故事性）
- x：peak_ratio（hype 程度）
- y：health_score（實質健康）
- 點大小：hf_total_downloads
- 顏色：AI / General
- 標記：前 10 個 repo 名稱
- **來源：** `star_growth_hype_csv` + `health_score.csv`

### 圖4 — 三維健康度：Bubble chart
- x：gh_stars_2025（GH 關注度）
- y：pypi_downloads_2025（工程採用）
- 點大小：hf_total_downloads（HF 使用量）
- 顏色：category
- **來源：** `health_score.csv`

### 圖5 — 各維度強弱：Heatmap
- 列：36 repos
- 欄：hf_downloads / pypi_downloads / gh_stars / gh_pushes / gh_prs / active_days（normalize 後）
- **來源：** `health_score.csv`

### 圖6 — Contributor 風險：Scatter plot
- x：top1_push_ratio（單人依賴程度）
- y：total_contributors（貢獻者總數）
- 顏色：AI / General
- **來源：** `contributor_health_csv`

### 圖7 — 月度趨勢：Line chart
- x：月份（2025-01 ~ 2025-11）
- y：monthly_stars
- 線條：前 5 AI repo vs 前 5 general repo
- **來源：** GH Archive 月聚合

### 圖8 — 月度活躍度：Heatmap
- 列：top 20 repos
- 欄：月份
- 顏色深度：monthly_stars
- **來源：** `star_growth_hype_csv`

---

## 四、工具與分工建議

| 工具 | 用途 |
|------|------|
| Tableau | 圖1~8 互動式視覺化（主要展示）|
| Python / matplotlib | 補充靜態圖（報告用）|
| Jupyter notebook | EDA 探索（`pipeline/gharchive/eda.ipynb`）|

**Tableau 資料來源（直接拖入）：**
- `data/health_score.csv` → 圖3、4、5（現在就可以做）
- `data/source/pypi_monthly_downloads.csv` → 圖7 PyPI 趨勢
- Jobs 05/06/07 跑完後下載的 CSV → 圖1、2、6、8

---

## 五、Pipeline 執行狀態

| Job | 內容 | 狀態 |
|-----|------|------|
| 01 | AI repos 每日指標 | ✅ 完成 |
| 02 | HF + GH join | ✅ 完成 |
| 03 | 三方 health score | ✅ 完成 |
| 04 | 全生態 top 1000 | 🔄 進行中 |
| 05 | AI vs General | ⬜ 待跑（需 04）|
| 06 | Star hype 偵測 | ⬜ 待跑（需 04）|
| 07 | Contributor 健康 | ⬜ 待跑（需 04）|
| 00a | 補充資料下載（2025-12~2026-04）| 🔄 進行中 |
| 00b | 補充資料清理 | ⬜ 待跑（需 00a）|

> **注意：** Job 04 完成後，需依序跑 05 → 06 → 07。若之後補充資料就緒，需重跑 04 → 05 → 06 → 07 確保資料一致。
