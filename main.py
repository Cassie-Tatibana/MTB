#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Dict
import os

from flask import Flask, request, redirect, url_for, render_template, flash
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
try:
    # Python 3.9+ 标准库
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from models import init_db, SessionLocal, SyncTask, SyncLog
from sync_runner import run_task

app = Flask(__name__)
app.secret_key = "dev-secret"
# 以北京时区运行调度（确保 cron 在北京时间触发）
if ZoneInfo:
    scheduler = BackgroundScheduler(
        timezone=ZoneInfo("Asia/Shanghai"),
        job_defaults={"max_instances": 1, "coalesce": True},
    )
else:
    scheduler = BackgroundScheduler(
        job_defaults={"max_instances": 1, "coalesce": True},
    )


def parse_cron_expr(expr: str) -> Dict:
    # 支持标准5段: "m h dom mon dow"
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError("cron_expr 需为5段表达式，如: 0 3 * * *")
    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }


def upsert_job(task: SyncTask):
    job_id = f"task_{task.id}"
    # 先移除旧任务
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    # 未启用则不注册
    if not bool(getattr(task, "enabled", True)):
        return
    # 注册新任务
    trigger_kwargs = parse_cron_expr(task.cron_expr)
    if ZoneInfo:
        scheduler.add_job(
            run_task,
            CronTrigger(timezone=ZoneInfo("Asia/Shanghai"), **trigger_kwargs),
            id=job_id,
            args=[task.id],
            replace_existing=True,
        )
    else:
        scheduler.add_job(run_task, CronTrigger(**trigger_kwargs), id=job_id, args=[task.id], replace_existing=True)


def cn_time(dt: datetime) -> str:
    """
    将 datetime 渲染为北京时区字符串：
    - 若是 naive（无 tzinfo），视为已是本地时间，直接格式化（避免二次换算）
    - 若是 tz-aware，则转换到 Asia/Shanghai 后再格式化
    """
    if not dt:
        return "-"
    try:
        if ZoneInfo and getattr(dt, "tzinfo", None) is not None:
            tz_cn = ZoneInfo("Asia/Shanghai")
            cn_dt = dt.astimezone(tz_cn)
            return cn_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # naive 或无 zoneinfo：直接按本地格式输出（数据库里存的即为北京时间）
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)

# 注册到 Jinja 过滤器
app.jinja_env.filters["cn_time"] = cn_time


@app.route("/")
def index():
    session = SessionLocal()
    tasks = session.query(SyncTask).order_by(SyncTask.id.desc()).all()
    logs = session.query(SyncLog).order_by(SyncLog.id.desc()).limit(20).all()
    session.close()
    return render_template("tasks_list.html", tasks=tasks, logs=logs)


@app.route("/tasks/new", methods=["GET", "POST"])
def new_task():
    if request.method == "POST":
        session = SessionLocal()
        task = SyncTask(
            name=request.form.get("name") or "未命名任务",
            sql_text=request.form.get("sql_text") or "SELECT 1",
            feishu_link=request.form.get("feishu_link") or "",
            target_type="bitable",
            sync_mode=request.form.get("sync_mode") or "full",
            index_column=request.form.get("index_column") or "id",
            field_type_strategy=request.form.get("field_type_strategy") or "base",
            create_missing_fields=bool(request.form.get("create_missing_fields", "true") == "true"),
            enabled=True,
            cron_expr=request.form.get("cron_expr") or "0 3 * * *",
            last_run_status=None,
        )
        session.add(task)
        session.commit()
        upsert_job(task)
        flash("任务已创建并注册调度", "success")
        return redirect(url_for("index"))
    return render_template("task_form.html", task=None)


@app.route("/tasks/<int:task_id>/edit", methods=["GET", "POST"])
def edit_task(task_id: int):
    session = SessionLocal()
    task = session.query(SyncTask).get(task_id)
    if not task:
        session.close()
        return "Task not found", 404
    if request.method == "POST":
        task.name = request.form.get("name") or task.name
        task.sql_text = request.form.get("sql_text") or task.sql_text
        task.feishu_link = request.form.get("feishu_link") or task.feishu_link
        task.sync_mode = request.form.get("sync_mode") or task.sync_mode
        task.index_column = request.form.get("index_column") or task.index_column
        task.field_type_strategy = request.form.get("field_type_strategy") or task.field_type_strategy
        task.create_missing_fields = bool(request.form.get("create_missing_fields", "true") == "true")
        task.cron_expr = request.form.get("cron_expr") or task.cron_expr
        session.commit()
        upsert_job(task)
        session.close()
        flash("任务已更新并重新注册调度", "success")
        return redirect(url_for("index"))
    session.close()
    return render_template("task_form.html", task=task)


@app.route("/tasks/<int:task_id>/run", methods=["POST"])
def run_now(task_id: int):
    scheduler.add_job(run_task, id=f"run_once_{task_id}_{datetime.utcnow().timestamp()}", args=[task_id], replace_existing=False)
    flash("已触发后台执行", "info")
    return redirect(url_for("index"))


@app.route("/tasks/<int:task_id>/toggle", methods=["POST"])
def toggle_task(task_id: int):
    session = SessionLocal()
    task = session.query(SyncTask).get(task_id)
    if not task:
        session.close()
        flash("任务不存在", "warning")
        return redirect(url_for("index"))
    task.enabled = not bool(getattr(task, "enabled", True))
    session.commit()
    # 更新调度
    if task.enabled:
        upsert_job(task)
        flash("任务已启用并注册调度", "success")
    else:
        try:
            scheduler.remove_job(f"task_{task.id}")
        except Exception:
            pass
        flash("任务已禁用并移除调度", "info")
    session.close()
    return redirect(url_for("index"))


@app.route("/tasks/<int:task_id>/delete", methods=["POST"])
def delete_task(task_id: int):
    session = SessionLocal()
    task = session.query(SyncTask).get(task_id)
    if not task:
        session.close()
        flash("任务不存在", "warning")
        return redirect(url_for("index"))
    # 移除调度
    try:
        scheduler.remove_job(f"task_{task.id}")
    except Exception:
        pass
    session.delete(task)
    session.commit()
    session.close()
    flash("任务已删除", "success")
    return redirect(url_for("index"))


def bootstrap():
    init_db()
    # 启动时加载现有任务
    session = SessionLocal()
    for task in session.query(SyncTask).all():
        try:
            upsert_job(task)
        except Exception as e:
            # 忽略单个任务的调度错误，便于系统整体启动
            pass
    session.close()


if __name__ == "__main__":
    bootstrap()
    # 仅在主进程中启动调度器，避免 Flask debug 模式下的双进程重复调度
    should_start_scheduler = (not app.debug) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if should_start_scheduler:
        try:
            scheduler.start()
        except Exception:
            pass
    app.run(host="0.0.0.0", port=8000, debug=True)


