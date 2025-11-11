#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict

import requests
import time
import hmac
import hashlib
import base64

from config import CONFIG
from models import SessionLocal, SyncTask, SyncLog
from mysql_to_bitable import (
    read_mysql_to_df,
    write_temp_excel,
    run_xtf_with_config,
)
import yaml
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
import re


def _now_cn_naive():
    """
    返回北京时间的“naive” datetime（无tzinfo），便于直接落库为本地时间。
    """
    if ZoneInfo:
        tz_cn = ZoneInfo("Asia/Shanghai")
        return datetime.now(tz=tz_cn).replace(tzinfo=None)
    return datetime.now()

def _parse_feishu_link(link: str) -> dict:
    """
    解析飞书多维表链接（仅支持 bitable）:
    - https://.../base/<app_token>?table=<tbl...>&view=...
    返回: { 'target_type': 'bitable', 'app_token':..., 'table_id':... }
    """
    if not link:
        return {}
    link = link.strip()
    # bitable
    m_base = re.search(r"/base/([A-Za-z0-9]+)", link)
    if m_base:
        app_token = m_base.group(1)
        m_table = re.search(r"[?&]table=(tbl[0-9A-Za-z]+)", link)
        table_id = m_table.group(1) if m_table else ""
        return {"target_type": "bitable", "app_token": app_token, "table_id": table_id}
    return {}


def _send_webhook(message: str) -> tuple:
    """
    发送飞书Webhook消息。
    Returns: (ok: bool, info: str)
    """
    if not CONFIG.webhook_url:
        return False, "webhook_url empty"
    payload = {
        "msg_type": "text",
        "content": {"text": message}
    }
    headers = {"Content-Type": "application/json; charset=utf-8"}
    # 若开启签名校验，计算签名（与飞书文档一致：HMAC 的 key=timestamp+'\n'+secret，消息体为空）
    if getattr(CONFIG, "webhook_secret", ""):
        ts_int = int(time.time())
        timestamp = str(ts_int)
        string_to_sign = f"{timestamp}\n{CONFIG.webhook_secret}"
        sign = base64.b64encode(
            hmac.new(string_to_sign.encode("utf-8"), b"", digestmod=hashlib.sha256).digest()
        ).decode("utf-8")
        # 按官方推荐：将 timestamp 与 sign 放入请求体
        payload.update({"timestamp": timestamp, "sign": sign})
        url = CONFIG.webhook_url
        # 记录便于排查的时间信息（北京时区）
        try:
            if ZoneInfo:
                cn_time = datetime.fromtimestamp(ts_int, ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
            else:
                cn_time = datetime.fromtimestamp(ts_int).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            cn_time = "n/a"
        debug_suffix = f" (ts={timestamp}, beijing_time={cn_time})"
    else:
        url = CONFIG.webhook_url
        debug_suffix = ""
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        txt = resp.text
        if resp.status_code != 200:
            return False, f"http {resp.status_code}: {txt[:200]}{debug_suffix}"
        # 尝试解析V2机器人返回结构
        try:
            data = resp.json()
            code = data.get("StatusCode", data.get("code", 0))
            msg = data.get("StatusMessage", data.get("msg", ""))
            if code == 0:
                return True, "ok"
            else:
                return False, f"feishu code={code}, msg={msg}{debug_suffix}"
        except Exception:
            # 非标准JSON也视为已送达
            return True, "ok"
    except Exception as e:
        return False, f"exception: {e}{debug_suffix}"


def _build_xtf_yaml_dict(task: SyncTask, excel_path: Path) -> Dict:
    cfg = {
        "file_path": str(excel_path),
        "app_id": CONFIG.feishu.app_id,
        "app_secret": CONFIG.feishu.app_secret,
        "sync_mode": task.sync_mode,
        "index_column": task.index_column,
        "batch_size": 1000,
        "rate_limit_delay": 0.5,
        "max_retries": 3,
        "log_level": "INFO",
        "field_type_strategy": task.field_type_strategy,
        "create_missing_fields": bool(task.create_missing_fields),
    }
    # 解析链接（仅支持 bitable）
    parsed = _parse_feishu_link(task.feishu_link or "")
    if parsed.get("target_type") == "bitable":
        cfg.update({
            "target_type": "bitable",
            "app_token": parsed.get("app_token") or task.app_token,
            "table_id": parsed.get("table_id") or task.table_id,
        })
    else:
        # 回退到任务原配置（兼容老任务）
        cfg.update({"target_type": task.target_type})
        if task.target_type == "bitable":
            cfg.update({
                "app_token": getattr(task, "app_token", "") or "",
                "table_id": getattr(task, "table_id", "") or "",
            })
        else:
            # 强制为 bitable（无效链接将由 XTF 报错提示）
            cfg.update({"target_type": "bitable"})
    return cfg


def _display_target_from_link(link: str) -> str:
    parsed = _parse_feishu_link(link or "")
    if parsed.get("target_type") == "bitable":
        at = parsed.get("app_token", "")
        tid = parsed.get("table_id", "")
        return f"bitable:{at}/{tid}" if (at or tid) else "bitable"
    if parsed.get("target_type") == "sheet":
        st = parsed.get("spreadsheet_token", "")
        sid = parsed.get("sheet_id", "")
        return f"sheet:{st}/{sid}" if (st or sid) else "sheet"
    return link or "unknown"


def run_task(task_id: int) -> None:
    session = SessionLocal()
    task = session.query(SyncTask).get(task_id)
    if not task:
        return

    # 记录日志开始
    log = SyncLog(task_id=task.id, task_name=task.name, start_time=_now_cn_naive(), status="running", message="start")
    session.add(log)
    session.commit()
    log_id = log.id

    run_dir = Path(CONFIG.runtime_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    # 改为每个任务复用固定产物文件，避免每次生成新的 YAML/Excel
    excel_path = run_dir / f"task_{task.id}.xlsx"
    yaml_path = run_dir / f"task_{task.id}.yaml"

    try:
        # 1) 读 MySQL（全局写死配置），使用任务的 SQL
        mysql_uri = f"mysql+pymysql://{CONFIG.mysql.username}:{CONFIG.mysql.password}@{CONFIG.mysql.host}:{CONFIG.mysql.port}/{CONFIG.mysql.database}?charset=utf8mb4"
        df = read_mysql_to_df(mysql_uri, CONFIG.mysql.database, table=None, sql=str(task.sql_text))
        # 2) 导出 Excel
        write_temp_excel(df, excel_path)
        # 3) 生成 XTF 配置 YAML（不含 source，基于链接解析）
        xtf_cfg = _build_xtf_yaml_dict(task, excel_path)
        yaml_path.write_text(yaml.safe_dump(xtf_cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
        # 4) 调用 XTF 执行
        rc, ok, output = run_xtf_with_config(yaml_path)
        if ok:
            status = "success"
            message = f"同步成功: 行数={len(df)}, 模式={task.sync_mode}, 目标={_display_target_from_link(task.feishu_link)}"
        else:
            status = "fail"
            # 返回部分关键输出帮助定位
            tail = (output or "").splitlines()[-10:]
            tail_text = " | ".join(tail) if tail else "失败，详见控制台/日志"
            # 特定错误的人性化摘要（权限类）
            lower_out = (output or "").lower()
            out_text = output or ""
            permission_error = (
                ("91403" in out_text)
                or ("1254302" in out_text)
                or ("forbidden" in lower_out)
                or ("no permissions" in lower_out)
                or ("the role has no permissions" in lower_out)
            )
            if permission_error:
                human_summary = "暂无权限——当前应用/机器人对目标多维表没有足够权限（读取/创建字段）。请在多维表中邀请该机器人并授予编辑或管理员权限"
                message = f"{human_summary} | XTF 返回码 {rc}; {tail_text}"
            else:
                message = f"XTF 返回码 {rc}; {tail_text}"
    except Exception as e:
        status = "fail"
        message = f"异常: {e}\n{traceback.format_exc()}"

    # 更新日志与任务状态
    log = session.query(SyncLog).get(log_id)
    log.end_time = _now_cn_naive()
    log.status = status
    log.message = message
    task.last_run_status = status
    task.updated_at = _now_cn_naive()
    session.commit()

    # 失败告警（带回执）
    if status != "success":
        ok_webhook, info_webhook = _send_webhook(f"[DataSync] 任务失败: {task.name} - {message}")
        if not ok_webhook:
            # 追加Webhook失败信息到日志，便于定位
            log = session.query(SyncLog).get(log_id)
            log.message = (log.message or "") + f" | Webhook推送失败: {info_webhook}"
            session.commit()


