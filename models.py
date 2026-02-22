#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, Enum, create_engine, inspect, text as sa_text
)
from sqlalchemy.orm import declarative_base, sessionmaker

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None

from config import CONFIG

Base = declarative_base()


def now_cn_naive():
    """返回北京时间的 naive datetime（无时区信息），用于数据库存储"""
    if ZoneInfo:
        tz_cn = ZoneInfo("Asia/Shanghai")
        return datetime.now(tz=tz_cn).replace(tzinfo=None)
    # Fallback: 假定服务器时间已是合理时间，或者接受 UTC 偏差
    return datetime.now()


class SyncTask(Base):
    __tablename__ = "sync_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    sql_text = Column(Text, nullable=False)
    # 新增：飞书链接（多维表 base 链接或电子表格链接）
    feishu_link = Column(Text, nullable=True)

    target_type = Column(String(16), nullable=False, default="bitable")  # bitable/sheet
    sync_mode = Column(String(16), nullable=False, default="full")       # full/incremental/overwrite/clone
    index_column = Column(String(64), nullable=False, default="id")
    field_type_strategy = Column(String(16), nullable=False, default="base")  # raw/base/auto/intelligence
    create_missing_fields = Column(Boolean, nullable=False, default=True)
    enabled = Column(Boolean, nullable=False, default=True)  # 是否启用

    # 目标表信息改为通过 feishu_link 解析，不再强制存储 app_token/table_id

    cron_expr = Column(String(64), nullable=False, default="0 3 * * *")
    last_run_status = Column(String(32), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=now_cn_naive, onupdate=now_cn_naive)


class SyncLog(Base):
    __tablename__ = "sync_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, nullable=False, index=True)
    task_name = Column(String(64), nullable=True)
    start_time = Column(DateTime, nullable=False, default=now_cn_naive)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(16), nullable=True)  # success/fail
    message = Column(Text, nullable=True)


def create_db_engine():
    return create_engine(CONFIG.mysql.sqlalchemy_url(), pool_pre_ping=True, pool_recycle=1800)


ENGINE = create_db_engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False)


def init_db():
    Base.metadata.create_all(bind=ENGINE)
    # 轻量自适应：若缺少 task_name 字段则自动补齐
    try:
        inspector = inspect(ENGINE)
        cols = [c.get("name") for c in inspector.get_columns("sync_logs")]
        if "task_name" not in cols:
            with ENGINE.begin() as conn:
                conn.execute(sa_text("ALTER TABLE sync_logs ADD COLUMN task_name VARCHAR(64)"))
    except Exception:
        # 避免因权限或版本问题影响主流程
        pass
    # 轻量自适应：若缺少 feishu_link 字段则自动补齐
    try:
        inspector = inspect(ENGINE)
        cols = [c.get("name") for c in inspector.get_columns("sync_tasks")]
        if "feishu_link" not in cols:
            with ENGINE.begin() as conn:
                conn.execute(sa_text("ALTER TABLE sync_tasks ADD COLUMN feishu_link TEXT"))
        if "enabled" not in cols:
            with ENGINE.begin() as conn:
                conn.execute(sa_text("ALTER TABLE sync_tasks ADD COLUMN enabled TINYINT(1) NOT NULL DEFAULT 1"))
        # 兼容旧库：若存在已废弃的 app_token/table_id 且为 NOT NULL，则放宽为可空，避免插入报错
        if "app_token" in cols:
            try:
                with ENGINE.begin() as conn:
                    conn.execute(sa_text("ALTER TABLE sync_tasks MODIFY COLUMN app_token VARCHAR(64) NULL DEFAULT NULL"))
            except Exception:
                pass
        if "table_id" in cols:
            try:
                with ENGINE.begin() as conn:
                    conn.execute(sa_text("ALTER TABLE sync_tasks MODIFY COLUMN table_id VARCHAR(64) NULL DEFAULT NULL"))
            except Exception:
                pass
    except Exception:
        pass


