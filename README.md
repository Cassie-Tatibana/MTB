## MTB - MySQL To Bitable（仅支持多维表格 Bitable）

本项目（MTB）提供「从 MySQL 读取数据 → 导出为 Excel → 调用 XTF 引擎写入飞书多维表格（Bitable）」的一体化解决方案，内置轻量任务管理与调度（Flask + APScheduler），并支持失败告警（飞书群机器人 Webhook）。
A lightweight data synchronization tool from MySQL to Feishu Bitable (target_type: bitable).

本仓库由两部分构成：
- 外层调度与包装代码（本仓库原创）
- 上游项目 XTF（第三方，GPL-3.0，未修改），作为核心写入引擎被调用

### 功能特性
- 同步模式：full / incremental / overwrite / clone
- 索引列去重与幂等：基于 `index_column` 进行记录匹配
- 字段类型策略：raw / base / auto / intelligence（基于 XTF 引擎）
- 任务管理：Web 页面创建、编辑、启用、手动执行、定时执行（Cron）
- 结果可观测：数据库 `sync_logs` 明细、最近 20 条 UI 展示、失败飞书群通知
- 时区安全：UI 和日志统一按北京时间展示

### 目录结构（关键文件）
- `main.py`：Flask Web 与调度入口
- `models.py`：SQLAlchemy 数据模型与建表
- `sync_runner.py`：任务执行器（读库→导表→写飞书→记日志→告警）
- `mysql_to_bitable.py`：独立 CLI，同步一次（从 YAML 读取 MySQL 与飞书配置）
- `_tmp_xtf_config.yaml`：CLI 用 YAML 示例（包含 source 与 XTF 运行参数）
- `runs/task_*.xlsx|yaml`：各任务导出的 Excel 与 XTF 运行 YAML
- `templates/*.html`：任务列表与表单页面
- `XTF-main/`：XTF 引擎（真正与飞书 API 交互）

### 环境要求
- Python 3.9+（推荐 3.10/3.11）
- MySQL 5.7+ / 8.0+
- 依赖（示例）：
  - Flask, APScheduler, SQLAlchemy, PyMySQL
  - pandas, openpyxl, PyYAML, requests

安装依赖（示例）：
```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install flask apscheduler sqlalchemy pymysql pandas openpyxl pyyaml requests
# XTF 引擎依赖（在其目录下）
pip install -r XTF-main/requirements.txt
```

### 快速开始（Web 调度）
1) 配置 MySQL / 飞书凭据  
   编辑 `config.py` 中的 `MySQLConfig` 与 `FeishuConfig`，配置飞书机器人 `webhook_url` 与 `webhook_secret`（如需失败告警）。

2) 初始化与启动
```bash
python main.py
# 访问 http://127.0.0.1:8000
```

3) Web 页面操作
- 新建任务：填写任务名、MySQL 查询 SQL、飞书多维表链接（形如 `/base/<app_token>?table=<tbl...>`）
- 设置同步模式、索引列、字段策略
- 保存后即可「立即执行」或设置 Cron 定时（北京时间）

4) 查看结果
- 页面底部展示最近 20 条日志
- 全量日志存储在数据库表 `sync_logs`
- 失败时会推送到飞书群（如配置 Webhook）

### 单次同步（CLI）
若你只想用一次性的 YAML 配置进行同步（无需 Web 界面）：
```bash
python mysql_to_bitable.py --config _tmp_xtf_config.yaml
```
说明：
- 程序读取 `_tmp_xtf_config.yaml` 的 `source.mysql` 配置连接 MySQL，执行 `source.sql` 或 `source.table` 生成 Excel
- 临时覆盖 YAML 中的 `file_path` 并移除 `source`，随后调用 `XTF-main/XTF.py` 执行同步

### 同步模式与索引列
- `full`：存在则更新，不存在则新增（推荐）
- `incremental`：只新增，不更新已存在的记录
- `overwrite`：删除已存在记录，再导入（危险）
- `clone`：清空远端表后导入（危险）
- `index_column`：用于唯一匹配（如 `id`、`sku_code`），务必保证数据唯一

### 时区与日志
- 调度统一以北京时间（Asia/Shanghai）触发
- `sync_logs.start_time/end_time` 为北京时间（naive datetime）
- 页面显示使用 `cn_time` 过滤器渲染

### 常见问题
- 91403 或 forbidden：当前应用对目标多维表缺少「字段创建」权限，请将机器人加入目标多维表并授予相应权限
- app secret invalid / 获取访问令牌失败：检查 `FeishuConfig` 的 `app_id/app_secret`
- 结果为空：检查 SQL 是否正确、连接是否有效
- Excel 写入失败：确保已安装 `openpyxl`

### 安全建议
- 限制飞书应用权限到最小集合

### 许可证
本项目示例代码遵循与仓库上游一致的开源许可（如有）。如用于生产，请审阅并补充企业内合规与安全策略。

### 上游项目与许可合规（重要）
- 上游仓库：BlueSkyXN/XTF（GPL-3.0）  
  固定提交：4a7113f  
  链接：[`BlueSkyXN/XTF@4a7113f`](https://github.com/BlueSkyXN/XTF/tree/4a7113faa0f8258f8aba043730d6e42ab4e0a478)
- 本项目未修改 `XTF-main/` 源码，保留其 LICENSE/README/版权声明；外层代码为自研，与 XTF 以子进程方式解耦，属于“同一仓库内的聚合分发”

### 拉取与初始化（含子模块）
- 初次克隆（推荐连同子模块）
```bash
git clone --recurse-submodules <your-repo-url>
cd 飞书mysql同步
```
- 若已克隆但未带子模块：
```bash
git submodule update --init --recursive
```

### 运行时配置注入（给二次使用者）
- 方式A（推荐）：环境变量（或复制 `example.env` 为 `.env`）
  - `.env` 支持字段：`MYSQL_HOST/PORT/USERNAME/PASSWORD/DATABASE`、`FEISHU_APP_ID/FEISHU_APP_SECRET/FEISHU_BASE_APP_TOKEN`、`FEISHU_WEBHOOK_URL/FEISHU_WEBHOOK_SECRET`、`RUNTIME_DIR`
  - 已内置对 `python-dotenv` 的自动加载支持（安装后自动读取 `.env`）
- 方式B（CLI）：复制 `_tmp_xtf_config.example.yaml` 为 `_tmp_xtf_config.yaml` 并填入真实值，然后
```bash
python mysql_to_bitable.py --config _tmp_xtf_config.yaml
```



### 许可（License）
- 上游项目 XTF：GPL-3.0（见其仓库与 `XTF-main/` 内 LICENSE）
- 外层调度与包装代码：MIT（见根目录 LICENSE）
  - 免责声明：本项目与飞书（Lark）、字节跳动无从属与背书关系；请遵守官方公开 API 与品牌使用规范


