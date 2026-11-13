# AIS 船位连续归档与港区进出事件系统

港口数据工程场景：本地**模拟岸站**持续产生带全局连续序号的 AIS 船位，
**采集归档服务**通过 SSE 实时流 + HTTP 历史分页两条通道完整归档，
并基于**部署配置的固定港区矩形**，按同船相邻船位的港内/港外状态切换
增量计算**进港/出港事件**，为靠离泊分析复核提供无缺口轨迹与进出港时段。
FastAPI + SQLite，Docker Compose 一键启动。

## 1. 架构与核心语义

```
                 ┌──────────────────────┐        SSE /stream（只推新记录，不补发）
                 │  模拟岸站 station:8000 │◄──────────────┐
                 │  seq 从 1 全局连续递增  │               │
                 │  记录只增不改           │        HTTP /history 分页补采
                 └──────────────────────┘◄─────────────┐│
                                                       ││
                                            ┌──────────▼▼───────────┐
                                            │ 采集器 collector:8001  │
                                            │  ┌──────────────────┐ │
                                            │  │ SSE 实时循环      │ │  断流指数退避重连
                                            │  │ 历史补采循环      │ │  翻页失败重试、互不影响
                                            │  └────────┬─────────┘ │
                                            │     INSERT OR IGNORE  │  seq 主键去重（重叠只入一次）
                                            │  连续水位 contiguous_seq │  仅 1..w 全在库才推进
                                            │  ┌──────────────────┐ │
                                            │  │ 港区事件引擎(后台) │ │  只消费连续水位以内船位
                                            │  │ 外→内=进 内→外=出  │ │  分批小事务，不占长写事务
                                            │  │ 事件+船态+进度持久化│ │  重启接续，不漏不重
                                            │  └──────────────────┘ │
                                            │  SQLite (WAL, 持久卷)   │
                                            └───────────────────────┘
```

### 1.1 采集归档保证

| 需求 | 实现机制 |
|---|---|
| 重叠记录只入库一次 | `positions.seq` 主键 + `INSERT OR IGNORE`，SSE/历史任一路径先到先得 |
| 补采期间新船位不漏 | 两个循环并发独立运行；SSE 不补发的部分由历史循环按持久化游标补齐 |
| 断流不丢数据 | SSE 循环断线指数退避重连；断线期间船位由历史循环补回 |
| 翻页失败不影响实时 | 历史循环仅记录错误并重试，SSE 循环完全独立 |
| 连续进度严谨 | 每次写入后从水位逐序号探测，遇缺口即停；水位持久化、单调不减 |
| 轨迹不越过缺口 | 查询以 `contiguous_seq` 为可见上界，缺口之后的记录一律不返回 |
| 重启自动接续 | 船位、水位、历史游标、错误全部在 SQLite；启动即恢复并继续补采 |
| 岸站重置可自愈 | 检测到序号代际回退时自动把补采游标回 0 重新补采（记录为已解决告警） |

### 1.2 港区进出事件语义

- **港区是部署配置的固定经纬度矩形**（`PORT_LAT_MIN/MAX`、`PORT_LON_MIN/MAX`），
  **边界算内部**（闭区间判定），矩形**不跨 180°** 经线；启动即校验配置合法性。
- 按 **seq 比较同一 MMSI 的相邻船位**：港外→港内记 **进港 `enter`**，
  港内→港外记 **出港 `exit`**；状态相同不产生事件，每艘船处理到的
  **第一个船位只建立衔接状态、不记事件**。
- 事件记录方向以及跨越边界的**前后船位序号与各自 UTC 时刻**
  （`before_seq/after_seq`、`before_utc_ts/after_utc_ts`）。
- **后台先补建历史、随后只处理新增连续船位**：事件引擎只使用连续进度
  `contiguous_seq` 以内的数据，处理进度 `event_processed_seq` 持久化并对外返回；
  历史扫描按 `EVENT_BATCH_SIZE` 分批小事务提交，**不占用长写事务**，
  期间采集与查询持续可用（SQLite WAL）。
- 事件（`port_events`）、同船衔接状态（`vessel_port_state`）、处理进度
  （meta 中 `event_processed_seq`）**共同持久化**，失败或重启自动接续，
  **不漏记、不重复**（事件 `after_seq` 全局唯一 + 状态 upsert 幂等）。
- **缺口演练**：回退连续进度后，越过新进度的事件在同一事务内立即删除、
  查询立刻不可见；epoch 护栏使**在途旧计算不能写回**；补齐后事件引擎从
  回退点重建——船舶衔接状态被**截断重建**为水位内最后船位的内/外状态，
  保证跨越回退边界的相邻船位仍被正确比较，自动恢复正确事件。
- **同次查询一致**：事件查询在请求开始时固定连续水位与处理进度快照，
  本次返回的事件锁定在该快照内，并在响应体内回带同一份 `progress`。
- **清空归档同时清空事件**：`/admin/reset` 在同一临界区清空船位、事件、
  衔接状态并推进 epoch，任何在途旧计算的写回都会被作废。

## 2. 快速启动（Docker Compose）

```bash
docker compose up --build
```

- 岸站 API 文档（含故障注入接口）：http://localhost:8000/docs
- 采集器 API 文档（状态/错误/轨迹/港区事件/管理）：http://localhost:8001/docs
- SQLite 数据位于命名卷 `collector-data`，容器重建不丢数据。

停止与清理：

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 停止并删除归档数据
```

> 采集器通过健康检查等待岸站就绪后再启动。

## 3. API 一览

### 采集器（端口 8001，演示主入口）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/status` | 已接收数、连续进度 `contiguous_seq`、事件处理进度、真实缺口、两通道计数、游标 |
| GET | `/errors?limit=&include_resolved=` | 错误查询（默认仅未恢复） |
| GET | `/vessels` | 已接收船舶清单及各自序号范围 |
| GET | `/trajectories/{mmsi}?limit=` | 按 MMSI 读取轨迹（仅返回连续水位内记录，按 seq 升序） |
| GET | `/port-events/progress` | 港区事件处理进度（连续水位、已处理序号、事件总数、是否追平、港区矩形） |
| GET | `/port-events/{mmsi}?after_seq=&limit=&direction=` | 按 MMSI 分页查询进出港事件，响应内嵌同次一致的 `progress` |
| POST | `/admin/reset` | 清空归档（含事件与衔接状态）并重置进度（演示用） |
| POST | `/admin/inject-gap` | 故障注入：删除指定序号，连续水位与事件进度一起回退，演示缺口/事件截断语义 |
| POST | `/admin/rewind-backfill` | 历史游标回退到水位，重新补采缺口区间，演示补齐后事件自动恢复 |

事件查询参数：

- `after_seq`：分页游标，只返回 `after_seq` 大于该值的事件，首页传 0；
  响应的 `next_after_seq` 作为下一页游标，`has_more=false` 表示取完。
- `limit`：每页条数（1~500，默认 100）。
- `direction`：可选，`enter` 只看进港、`exit` 只看出港；缺省返回全部。
- 未知 MMSI（连续进度内无该船）返回 404；非法 `direction` 返回 422。

### 模拟岸站（端口 8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/stream` | SSE 实时流（`text/event-stream`，只推连接后的新船位） |
| GET | `/history?cursor=&limit=` | 历史分页，返回 `seq > cursor` 的记录 |
| GET | `/station/status` | 岸站生成状态 |
| POST | `/station/stream-stop` / `stream-resume` | 模拟实时流中断 / 恢复 |
| POST | `/station/history-fail-rate` | 设置翻页失败率 `{"rate": 0.5}`（0~1） |
| POST | `/station/reset` | 岸站清空并重新生成（注意见第 6 节） |

## 4. 在 Swagger UI 中的演示流程

打开 http://localhost:8001/docs ，按顺序操作（均可点 **Try it out** 执行）：

1. **观察初始采集与事件补建**：GET `/status`
   - `received_total`、`contiguous_seq` 增长并紧随，`complete=true`；
   - `event_processed_seq` 追平水位，`event_caught_up=true`，`event_total` 持续增长。
2. **查看处理进度与港区矩形**：GET `/port-events/progress`
   - 返回 `contiguous_seq`、`event_processed_seq`、`event_total`、`caught_up`、`port_rect`。
3. **查询进出港事件**：先 GET `/vessels` 取一个 MMSI（如 `413000002`），
   再 GET `/port-events/{mmsi}`：
   - 每个事件含 `direction`（enter/exit）、`before_seq/after_seq`、
     `before_utc_ts/after_utc_ts`；响应内 `progress` 与事件同次一致；
   - 用 `direction=enter`（或 `exit`）过滤方向；用响应的 `next_after_seq`
     作为下一页 `after_seq` 游标翻页。
4. **演练缺口与事件截断（核心）**：
   - 先记录当前事件；POST `/admin/inject-gap`，body 例如 `{"seqs": [50]}`；
   - 响应中 `contiguous_seq` 回退到 49，`event_rollback.deleted_events`
     给出被清理的越界事件数，`event_processed_seq` 同步回退；
   - 立即再查事件：所有事件 `after_seq <= 49`，越过新进度的事件已不可见，
     且等待数秒也不会被在途旧计算重新发布；
   - POST `/admin/rewind-backfill` 补齐缺口，水位恢复后，
     GET `/port-events/progress` 重新 `caught_up=true`，
     事件自动恢复且与轨迹重算结果完全一致（不漏不重）。
5. **演练清空归档**：先 POST 岸站 `/station/reset`，再 POST 采集器 `/admin/reset`；
   立即 GET `/port-events/progress` 可见事件与进度全部归零，随后随采集自动重建。
6. **采集容错演练（保持原能力）**：翻页失败率、断流/重连等流程见下，
   事件引擎与采集循环相互独立，故障期间事件处理只是等待水位，不产生错误事件。

> 历史翻页失败、SSE 断流重连等采集侧演练步骤与原系统一致：
> 在 8000 文档调 `/station/history-fail-rate`、`/station/stream-stop|resume`，
> 在 8001 用 `/status`、`/errors` 观察恢复。

## 5. 中断重启演示

```bash
docker compose restart collector        # 重启采集器，卷中数据保留
# 或本地：
# tests/restart_test.py 会真正杀掉进程再拉起，断言进度保留与自动接续
```

重启后 `/status` 中 `received_total`、`contiguous_seq`、`history_cursor`、
`event_processed_seq` 均为重启前的值；事件、同船衔接状态在 SQLite 中，
引擎从 `event_processed_seq` 继续，已生成事件不丢失、不重复。

## 6. 关于“重置”的正确姿势

- 重新开始一轮干净演练时，**两侧都要重置**（端到端测试即如此）：
  先 POST 采集器 `/admin/reset`，再 POST 岸站 `/station/reset`。
- 若只重置岸站：采集器能检测到序号代际回退并自动回退补采游标（不会卡死），
  但旧代际船位因 `seq` 主键冲突会保留在库中。这是去重主键的预期安全行为；
  需要全新数据集请使用 `/admin/reset` 清空归档（事件会一并清空）。

## 7. 本地开发（无 Docker 时）

```bash
python3 -m pip install -r requirements.txt   # 建议虚拟环境
# 终端 1：岸站
uvicorn shore_station.app:app --port 8000
# 终端 2：采集器
STATION_URL=http://127.0.0.1:8000 DB_PATH=./data/collector.db \
  uvicorn collector.app:app --port 8001
```

## 8. 自动化测试

测试只通过 HTTP 驱动，对 Docker 环境与本地进程同样适用：

```bash
# 基础容错与缺口语义
python3 tests/e2e_test.py
# 港区进出事件：正确性、分页/过滤、缺口截断与恢复、清空重建
python3 tests/event_test.py
# 真正杀进程重启的接续测试
python3 tests/restart_test.py
```

`event_test.py` 覆盖：事件与按轨迹独立重算逐字段一致、首点不记事件、
处理进度不越水位、同次查询事件与进度自洽、分页与方向过滤、
缺口回退后越界事件立即不可见且旧计算不写回、补齐后不漏不重、
清空归档同时清空事件并自动重建。

## 9. 目录结构

```
.
├── docker-compose.yml        # station + collector 两服务，SQLite 命名卷
├── Dockerfile                # 共用镜像，compose 中以不同 command 启动
├── requirements.txt
├── shore_station/app.py      # 模拟岸站：SSE 流、历史分页、断流/失败注入
├── collector/
│   ├── db.py                 # SQLite：去重写入、连续水位、错误、事件/船态/进度
│   ├── ingest.py             # SSE 循环 + 历史补采循环、退避重试、代际检测
│   ├── events.py             # 港区矩形判定 + 事件引擎（增量、回退护栏、重启接续）
│   └── app.py                # FastAPI：状态/错误/船舶/轨迹/港区事件/管理接口
└── tests/
    ├── e2e_test.py
    ├── event_test.py
    └── restart_test.py
```

## 10. 主要环境变量

| 变量 | 默认 | 作用 |
|---|---|---|
| `GEN_INTERVAL` | 0.25 | 岸站船位生成间隔（秒） |
| `INITIAL_COUNT` | 60 | 岸站启动时预生成历史条数 |
| `HISTORY_PAGE_LIMIT` | 100 | 岸站历史分页上限 |
| `STATION_URL` | http://station:8000 | 采集器访问的岸站基址 |
| `DB_PATH` | /data/collector.db | SQLite 文件路径 |
| `HISTORY_PAGE_SIZE` | 50 | 采集器补采分页大小 |
| `SSE_READ_TIMEOUT` | 60 | SSE 读超时（秒），超时判定断流并重连 |
| `SSE_RECONNECT_MAX` / `HISTORY_RETRY_MAX` | 30 / 30 | 两类重试的最大退避（秒） |
| `PORT_LAT_MIN` / `PORT_LAT_MAX` | 31.208 / 31.252 | 港区矩形纬度下界/上界（边界算内部） |
| `PORT_LON_MIN` / `PORT_LON_MAX` | 121.448 / 121.492 | 港区矩形经度下界/上界（不跨 180°） |
| `EVENT_BATCH_SIZE` | 200 | 事件引擎单批处理船位数（小事务提交） |
| `EVENT_IDLE_INTERVAL` | 0.3 | 无新增连续船位时的轮询间隔（秒） |
