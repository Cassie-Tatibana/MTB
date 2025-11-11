#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import os
# 可选：自动加载项目根目录的 .env（若已安装 python-dotenv）
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


@dataclass
class MySQLConfig:
    host: str = os.getenv("MYSQL_HOST", "127.0.0.1")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    username: str = os.getenv("MYSQL_USERNAME", "")
    password: str = os.getenv("MYSQL_PASSWORD", "")
    database: str = os.getenv("MYSQL_DATABASE", "mysql_to_bitable")

    def sqlalchemy_url(self) -> str:
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4"


@dataclass
class FeishuConfig:
    app_id: str = os.getenv("FEISHU_APP_ID", "")
    app_secret: str = os.getenv("FEISHU_APP_SECRET", "")
    # Base AppToken（多维表 base 的 app_token），可选
    app_token: str = os.getenv("FEISHU_BASE_APP_TOKEN", "")


@dataclass
class AppConfig:
    mysql: MySQLConfig = MySQLConfig()
    feishu: FeishuConfig = FeishuConfig()
    webhook_url: str = os.getenv("FEISHU_WEBHOOK_URL", "")  # 飞书群机器人Webhook
    webhook_secret: str = os.getenv("FEISHU_WEBHOOK_SECRET", "")  # 若开启“签名校验”，填写机器人密钥；否则留空
    runtime_dir: str = os.getenv("RUNTIME_DIR", "runs")  # 运行期产物（excel/yaml/log）的目录


CONFIG = AppConfig()


