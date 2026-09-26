# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 制品元数据：工作者回执时提交网格文件、日志摘要、校验清单等制品的相对路径、大小、sha256 摘要和用途；服务逐项校验本地文件后在同一事务中与结果版本原子绑定，校验失败任务保持运行态。
- 回执幂等：回执键（缺省为内容摘要）对重复回执去重，相同回执重放不制造第二份结果版本或制品记录，相同回执键对应不同内容会被拒绝。
- 结果生命周期：结果版本按候选、已发布、已撤回流转，发布与撤回均记录人工干预；已成功任务可人工重试重算，产生新的结果版本，被取代的候选版本成为临时制品。
- 下载授权：仅向任务提交人或活跃管理员签发授权，且只覆盖仍在保留期、未被撤回的版本，授权包含制品清单与过期时间。
- 清理计划：按临时、候选、已发布、被撤回四类汇总制品，已发布结果引用的制品永远阻止删除，保留期内的候选与撤回制品同样受保护。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。制品根目录默认 `./data/artifacts`，可用 `TOWNSHIP_ARTIFACT_ROOT` 调整；结果版本默认保留 30 天（`TOWNSHIP_ARTIFACT_RETENTION_DAYS`），下载授权默认 900 秒有效（`TOWNSHIP_DOWNLOAD_GRANT_SECONDS`）。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。制品与结果版本相关接口：

- `POST /api/compute/tasks/{id}/complete`：回执可携带 `artifacts`（相对路径、大小、sha256、用途）、`receipt_key` 和 `retention_days`。
- `GET /api/compute/tasks/{id}/artifacts?version=N`：查看任务或指定版本的制品清单。
- `POST /api/compute/tasks/{id}/results/{version}/publish?actor=...`：发布候选版本。
- `POST /api/compute/tasks/{id}/results/{version}/withdraw`：撤回版本，需说明原因。
- `POST /api/compute/downloads/authorize`：按任务、版本和请求人签发下载授权。
- `GET /api/compute/cleanup/plan`：输出四类制品的清理计划与阻止原因，只规划不删除。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、制品校验与原子绑定、回执幂等、发布与撤回、下载授权、清理计划分类、取消、人工重试、批量操作和租约恢复，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交计算任务、工作者领取、携带制品清单回执并签发下载授权，用于快速确认核心运营链路。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本、制品元数据和人工干预
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
