# 电影工具 (movie_tools)

基于豆瓣与 TMDB 的自动化工具，支持两种独立场景：**批量查询高分片单** 与 **定时推荐**，共享数据库与海报展示。

---

## 功能概览

| 场景 | 说明 |
|------|------|
| **批量查询** | 按年份范围（1~5 年）抓取高分电影，评分>5，最多 10~20 部，海报展示（仅年份，不强调具体上映日期） |
| **定时推荐** | 严格月份时间窗（最近 1~5 个月），最低 6 分，推荐 1~10 部，记录时间区间，支持 1 月/2 周推送周期，可配置 Webhook |

- **数据源**：豆瓣（列表 JSON 接口）、TMDB（补全上映日期/类型/时长）。
- **上映日期**：多源策略（豆瓣抽象 / 豆瓣详情 / TMDB），支持 Playwright 补偿缺月份日期。
  - **批量查询**：日期补全可选，海报仅展示年份。
  - **定时推荐**：执行后强制补全，确保月/日级日期；无具体日期则按年份展示。
- **推送**：文件、控制台、Webhook、Server 酱、Bark；定时推荐支持配置推送周期。
- **存储**：SQLite，两场景共享（movies_enriched、digests、digest_items、user_feedback）。
- **Web 界面**：Streamlit，侧栏切换场景，海报卡片展示，支持用户反馈（想看/已看/跳过）。

---

## 项目结构

```
movie_tools/
├── main.py                    # CLI 入口（digest batch/scheduled / catalog）
├── app.py                     # Streamlit Web 入口
├── models.py                  # 配置与结果数据模型
├── spiders/
│   ├── douban.py              # 豆瓣列表/抽象 API（含限流与熔断）
│   └── playwright_spider.py  # Playwright 详情页日期修复（可选）
├── services/
│   ├── pipeline.py            # 双场景流水线（MovieDigestService）
│   ├── tmdb.py                # TMDB API 与缓存
│   └── push.py                # 多通道推送
├── utils/
│   ├── processor.py           # 数据清洗与日期多源合并
│   ├── storage.py             # CSV/Excel/Markdown 导出
│   ├── state_store.py         # SQLite 状态与审计
│   ├── cache.py               # HTTP 响应文件缓存
│   ├── date_utils.py          # 日期解析、宽松/严格时间窗
│   └── history.py             # 历史摘要读取
├── docs/
│   └── recommendation_logic.md
├── .github/workflows/
│   └── movie_bot.yml          # GitHub Actions 定时/手动触发
├── .env.example               # 环境变量模板
└── requirements.txt
```

---

## 安装与配置

### 1. 依赖

```bash
pip install -r requirements.txt
```

### 2. 环境变量

复制 `.env.example` 为 `.env`，按需填写：

```ini
TMDB_API_KEY=your_tmdb_api_key_here
MOVIE_PUSH_WEBHOOK_URL=    # Webhook 推送 URL（可选）
SERVERCHAN_SENDKEY=        # Server 酱 sendkey（可选）
BARK_PUSH_URL=             # Bark 推送 URL（可选）
```

TMDB API Key 可在 [themoviedb.org](https://www.themoviedb.org/documentation/api) 申请。

### 3. Playwright（可选，定时推荐补偿缺失月份日期）

```bash
pip install playwright
playwright install chromium
```

未安装时，流水线会跳过 Playwright 步骤并打日志提示，不影响主流程。

---

## 使用方式

### 命令行

#### 场景一：批量查询

```bash
# 最近 2 年、评分 ≥5、最多 15 部
python main.py digest --mode batch

# 最近 3 年、评分 ≥6、最多 20 部
python main.py digest --mode batch --years-window 3 --min-rating 6.0 --max-candidates 20
```

**参数说明（batch 模式）**：

| 参数 | 说明 | 默认 |
|------|------|------|
| `--years-window` | 最近 N 年（1~5） | 2 |
| `--min-rating` | 最低豆瓣评分 | 5.0 |
| `--max-candidates` | 推荐数量（10~20） | 15 |
| `--allow-repeat` | 允许展示已推送过的影片 | 否 |
| `--per-year-limit` | 每年豆瓣抓取条数 | 60 |
| `--region-scope` | 地区筛选，all 不限 | all |

#### 场景二：定时推荐

```bash
# 最近 1 个月、评分 ≥6、最多 5 部，推送到 webhook
python main.py digest --mode scheduled --push-channel webhook --push

# 最近 2 个月、评分 ≥7、最多 8 部、2 周周期、允许重复推送
python main.py digest --mode scheduled --months-window 2 --min-rating 7.0 \
  --max-candidates 8 --push-interval 2weeks --allow-repeat

# 启用 Playwright 补偿缺失月份日期
python main.py digest --mode scheduled --playwright-repair
```

**参数说明（scheduled 模式）**：

| 参数 | 说明 | 默认 |
|------|------|------|
| `--months-window` | 最近 N 个月（1~5） | 1 |
| `--min-rating` | 最低豆瓣评分（≥6） | 6.0 |
| `--max-candidates` | 推荐数量（1~10） | 5 |
| `--push-interval` | 推送周期：`2weeks` / `1month` | 1month |
| `--push-channel` | 推送通道：file/console/webhook/serverchan/bark | file |
| `--push` | 执行推送 | 否 |
| `--playwright-repair` | 对缺月份日期的影片用 Playwright 补全 | 否 |
| `--allow-repeat` | 允许重复推送（始终选最高分） | 否 |
| `--per-year-limit` | 每年豆瓣抓取条数 | 60 |

### Web 界面

```bash
streamlit run app.py
```

侧栏顶部选择「批量查询」或「定时推荐」，配置参数后点击生成按钮。

- **批量查询**：配置年份范围（1~5 年）、最低评分、数量；海报卡片展示年份。
- **定时推荐**：配置月份时间窗（1~5 月）、最低评分、推荐数量、推送周期、Webhook；海报展示具体日期（无则年份）。
- **历史记录**：可按类型（批量/定时）筛选，展示对应的时间窗或年份范围。
- **影片库**：所有已缓存影片，支持按片名搜索与最低评分过滤。

---

## 反爬与限流策略

- **主链路不请求豆瓣详情页 HTML**：仅用列表接口与抽象 API + TMDB，减少触发 429。
- **豆瓣 HTML 请求**：已实现熔断、指数退避、BID Cookie 轮换与 sec.douban.com 检测；即便如此，requests 抓详情仍易被限流。
- **Playwright 补偿**：仅对「仍缺月份级日期」的少量影片启用，真实浏览器执行 JS、通过 sec.douban.com 校验，结果缓存 30 天，避免重复请求。

---

## 自动化（GitHub Actions）

`.github/workflows/movie_bot.yml` 支持两种触发方式：

**定时触发（cron）**：每月 1 日和每月 15 日自动运行定时推荐，并推送到配置的 Webhook。

**手动触发（workflow_dispatch）**：在 GitHub Actions 页面手动运行，可选：
- `mode`：`scheduled`（定时推荐）或 `batch`（批量查询）
- `months_window` / `years_window`：时间范围
- `min_rating` / `max_candidates`：评分与数量
- `push_interval`：`1month` 或 `2weeks`
- `push`：是否执行推送

**Secrets 配置**（仓库 Settings → Secrets and variables → Actions）：

| Secret | 说明 |
|--------|------|
| `TMDB_API_KEY` | TMDB API 密钥（必须） |
| `MOVIE_PUSH_WEBHOOK_URL` | Webhook 推送地址（可选） |
| `SERVERCHAN_SENDKEY` | Server 酱 sendkey（可选） |
| `BARK_PUSH_URL` | Bark 推送地址（可选） |

---

## 清空历史数据与缓存

```powershell
# PowerShell
Remove-Item -Path "output\movie_tools.db" -Force -ErrorAction SilentlyContinue
Remove-Item -Path "output\cache" -Recurse -Force -ErrorAction SilentlyContinue
```

```bash
# Bash
rm -f output/movie_tools.db
rm -rf output/cache
```

或使用脚本：

```bash
python scripts/clear_storage.py        # 仅清数据库与缓存
python scripts/clear_storage.py --all  # 同时删除 output/ 下导出文件
```
