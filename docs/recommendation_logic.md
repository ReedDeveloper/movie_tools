# 双场景逻辑说明

## 一、两种场景对比

| 维度 | 场景一：批量查询（batch） | 场景二：定时推荐（scheduled） |
|------|--------------------------|-------------------------------|
| 触发方式 | Web 手动 / CLI / Actions dispatch | Web 手动 / CLI / Actions cron/dispatch |
| 候选抓取 | 最近 1~5 年，每年调用豆瓣列表接口 | 当年 + 上一年，按月份时间窗决策 |
| 时间筛选 | 按年份范围（宽松，取 year 字段） | 严格月/日级日期落在 [window_start, today] |
| 评分下限 | 默认 5.0 | 默认 6.0 |
| 推荐数量 | 10~20（默认 15） | 1~10（默认 5） |
| 日期展示 | 仅年份，不强调具体日期 | 具体月/日；无则回退到年份 |
| 去重/重复 | 可选（默认允许重复） | 可配置（allow_repeat）；默认不重复 |
| 日期补全 | 可选 TMDB / Playwright | 执行后补全月/日级日期，无则按年显示 |
| 推送 | 可选 | 支持 Webhook/Server 酱/Bark，1 月或 2 周周期 |
| 数据存储 | 共享 SQLite（digest_type=batch） | 共享 SQLite（digest_type=scheduled） |

---

## 二、整体流程

### 场景一：批量查询

```
DoubanSpider.collect_candidate_pool_by_years(years_window)
    ↓ 每年 get_top_movies_by_year，合并去重
MetadataEnricher.enrich()
    ↓ 可选 TMDB 补全（类型/时长）
DecisionEngine._select_batch()
    ↓ 按年份范围过滤 + 评分排序 + 取前 N
DigestBuilder.build()  → 仅年份、无时间窗
StateStore.create_digest(digest_type="batch")
```

### 场景二：定时推荐

```
DoubanSpider.collect_candidate_pool(months_window)
    ↓ 当年 + 上一年列表
MetadataEnricher.enrich()
    ↓ TMDB 补全 + 可选 Playwright 补偿缺月份日期
DecisionEngine._select_scheduled()
    ↓ 严格 is_in_strict_window + only_unseen/allow_repeat + 评分打分
DigestBuilder.build()  → 展示时间窗 + 具体日期（或年份）
StateStore.create_digest(digest_type="scheduled", time_window_start, time_window_end)
    ↓ 可选推送（Webhook/Server 酱/Bark）
```

---

## 三、关键设计细节

### 3.1 时间窗判定

| 函数 | 用途 | 仅年份时 |
|------|------|----------|
| `is_recent_release(value, months_window)` | 宽松判断（原批量查询历史逻辑） | 只要年份在窗内即通过 |
| `is_in_strict_window(value, months_window)` | 严格判断（定时推荐） | 长度 < 7 直接返回 False |
| `is_in_year_range(value, years_window)` | 按年范围判断（批量查询） | 取前 4 位作年份判断 |

严格判断的意义：避免仅有年份（如 `"2026"`）的影片被错误地纳入「最近 1 个月」的推荐。定时推荐中，仅年份的影片会进入 `repair_queue`，通过 Playwright/TMDB 补全后写回 DB，下次运行（或二次筛选）才可能进入推荐。

### 3.2 候选采集

- **批量查询**：`collect_candidate_pool_by_years(years_window)`——遍历 `current_year` 到 `current_year - (years_window - 1)`，每年请求豆瓣列表接口；以 `movie_key=douban:{id}` 去重。
- **定时推荐**：`collect_candidate_pool(months_window)`——固定当年 + 上一年，`months_window` 参数仅用于决策阶段时间窗判定，不影响抓取范围。

### 3.3 决策打分（定时推荐）

```
decision_score = rating * 10 + freshness_bonus + confidence_bonus + vote_bonus
```

- `freshness_bonus`：严格在窗内 +30，否则 0
- `confidence_bonus`：日期置信度 high/medium/low 分别 +10/+6/+2
- `vote_bonus`：`tmdb_votes / 1000`

批量查询仅按 `rating * 10` 排序，不加时效奖励。

### 3.4 重复推送策略

| 场景 | only_unseen 逻辑 | 说明 |
|------|-----------------|------|
| 批量查询 | 默认 `allow_repeat=True`，`only_unseen=False` | 每次按评分展示；可选排除已展示 |
| 定时推荐 | 默认 `allow_repeat=False`，`only_unseen=True` | 历史已推送的 scheduled digest 中出现过的影片会被排除；勾选「允许重复推送」后不排除，始终选当前最高分 |

`get_sent_movie_keys(digest_type="scheduled")` 只读取 `digest_type=scheduled` 的历史推送，不影响批量查询的历史。

### 3.5 日期补全时机

- **批量查询**：和旧逻辑一致，TMDB 补全类型/时长/日期（可选）；Playwright 按需开启，补全结果写入 DB。海报展示只取年份。
- **定时推荐**：`MetadataEnricher` 补全后，若仍缺月/日级日期，进入 `repair_queue`；`use_playwright_repair=True` 时在流水线末尾触发 Playwright，补全结果写回 DB；展示时调用 `display_release_date(release_date, fallback_year=year)`，有具体日期则展示，无则展示年份。

---

## 四、数据库字段说明（digests 表）

| 字段 | 说明 |
|------|------|
| `digest_type` | `batch` 或 `scheduled` |
| `months_window` | 定时推荐的月份时间窗（scheduled 填写，batch 为 NULL） |
| `years_window` | 批量查询的年份范围（batch 填写，scheduled 为 NULL） |
| `time_window_start` | 定时推荐本次时间窗起始日期（YYYY-MM-DD） |
| `time_window_end` | 定时推荐本次时间窗结束日期（YYYY-MM-DD） |
| `push_interval` | 推送周期（`1month` / `2weeks`，scheduled 专用） |

旧记录（重构前生成）`digest_type` 字段为 NULL，代码视为 `scheduled` 以保持向后兼容。

---

## 五、推送与 Webhook

定时推荐执行后，若 `push_enabled=True`，调用 `PushService.send()` 向配置的通道发送 Markdown 摘要，内容包含：
- 时间窗口区间
- 最低评分
- 逐条影片：标题、评分、上映日期（具体或年份）、类型、地区、简介、豆瓣链接

Webhook 端接收到的 JSON body：`{"title": "...", "content": "...（Markdown）"}`。
