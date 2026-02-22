#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import subprocess
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
import re
try:
    import yaml
except Exception:
    yaml = None


def build_mysql_uri(host: str, port: int, username: str, password: str, database: str) -> str:
    return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4"

def _normalize_sql(sql_text: str) -> str:
    """
    规范化 SQL：
    - 去掉注释（--、#、/* ... */）
    - 去掉反斜杠续行（\\ + 换行）
    - 去除回车符
    - 合并多余空白
    - 去掉末尾分号
    - 将 Python 字符串转义的 %% 转换为单个 %（用于 date_format 等函数）
    """
    def strip_sql_comments(sql: str) -> str:
        """
        删除 SQL 注释。支持：
        - 单行注释：-- ... 与 # ...
        - 块注释：/* ... */
        保留字符串字面量内的注释符号。
        """
        result_chars = []
        i = 0
        n = len(sql)
        in_single = False
        in_double = False
        in_block_comment = False
        while i < n:
            ch = sql[i]
            nxt = sql[i + 1] if i + 1 < n else ""
            # 处理块注释结束
            if in_block_comment:
                if ch == "*" and nxt == "/":
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            # 处理字符串进入/退出
            if not in_single and not in_double:
                # 块注释开始
                if ch == "/" and nxt == "*":
                    in_block_comment = True
                    i += 2
                    continue
                # 单行注释 -- 和 #
                if ch == "-" and nxt == "-":
                    # 跳过到行尾
                    while i < n and sql[i] != "\n":
                        i += 1
                    continue
                if ch == "#":
                    while i < n and sql[i] != "\n":
                        i += 1
                    continue
                # 字符串开始
                if ch == "'":
                    in_single = True
                    result_chars.append(ch)
                    i += 1
                    continue
                if ch == "\"":
                    in_double = True
                    result_chars.append(ch)
                    i += 1
                    continue
                # 常规字符
                result_chars.append(ch)
                i += 1
                continue
            else:
                # 字符串内，处理转义与结束
                result_chars.append(ch)
                if in_single:
                    if ch == "\\" and (i + 1) < n:
                        # 逃逸下一个字符
                        result_chars.append(sql[i + 1])
                        i += 2
                        continue
                    if ch == "'":
                        in_single = False
                        i += 1
                        continue
                elif in_double:
                    if ch == "\\" and (i + 1) < n:
                        result_chars.append(sql[i + 1])
                        i += 2
                        continue
                    if ch == "\"":
                        in_double = False
                        i += 1
                        continue
                i += 1
        return "".join(result_chars)

    # 1) 统一换行风格并去掉回车
    s = sql_text.replace("\r", "")
    # 2) 先去除注释（保留换行，便于正确识别单行注释）
    s = strip_sql_comments(s)
    # 3) 去掉行续接的反斜杠
    s = re.sub(r"\\\s*\n", " ", s)
    # 4) 将 %% 转换为 %
    s = s.replace("%%", "%")
    # 5) 合并多余空白
    s = re.sub(r"\s+", " ", s).strip()
    # 6) 去掉末尾分号
    if s.endswith(";"):
        s = s[:-1].strip()
    return s


def read_mysql_to_df(uri: str, database: str, table: str = None, sql: str = None) -> pd.DataFrame:
    engine = create_engine(uri)
    if sql and sql.strip():
        query = _normalize_sql(sql)
    else:
        # 保守引用库与表名
        query = f"SELECT * FROM `{database}`.`{table}`"
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def write_temp_excel(df: pd.DataFrame, excel_path: Path) -> None:
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(excel_path, index=False, engine='openpyxl')


def load_yaml_config(config_path: Path) -> dict:
    if yaml is None:
        raise RuntimeError("缺少 pyyaml，请先: pip install pyyaml")
    if not config_path.exists():
        raise FileNotFoundError(f"未找到配置文件: {config_path}")
    data = yaml.safe_load(config_path.read_text(encoding='utf-8')) or {}
    return data


def build_clean_config_text(cfg: dict, excel_path: Path) -> str:
    """构造清洗后的 YAML 文本：更新 file_path，移除 source。"""
    if yaml is None:
        raise RuntimeError("缺少 pyyaml，请先: pip install pyyaml")
    cleaned = dict(cfg or {})
    cleaned['file_path'] = str(excel_path)
    if 'source' in cleaned:
        cleaned.pop('source', None)
    return yaml.safe_dump(cleaned, allow_unicode=True, sort_keys=False)


def run_xtf_with_config(config_path: Path):
    # 使用相对于当前脚本的路径定位 XTF.py
    # 当前文件在项目根目录，XTF.py 在 XTF-main/XTF.py
    current_file = Path(__file__).resolve()
    project_root = current_file.parent
    xtf_main = project_root / "XTF-main" / "XTF.py"
    if not xtf_main.exists():
        print(f"❌ 未找到 XTF 主程序: {xtf_main}")
        return 1, False, "XTF.py not found"
    # 固定为 bitable
    cmd = [sys.executable, str(xtf_main), "--target-type", "bitable", "--config", str(config_path)]
    print("运行命令:")
    print(" ", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    output = stdout + ("\n" + stderr if stderr else "")
    # 成功信号：包含“同步完成”或“批量创建完成”
    success_cues = ["同步完成", "🎉 批量创建完成", "批量创建完成"]
    ok = any(cue in output for cue in success_cues)
    # 失败判定（白名单放行“record not found/1254043”等可忽略错误）
    lower_output = output.lower()
    hard_fail_indicators = [
        "同步出错",
        "程序异常",
        "traceback",
        "获取访问令牌失败",
        "app secret invalid",
        " - error - ",
    ]
    # 可忽略错误白名单（删除不存在记录导致的失败不视为整体失败）
    whitelist_markers = [
        "record not found",
        "错误码 1254043",
        "1254043",
    ]
    # 若出现除“- ERROR -”之外的致命错误关键字，则失败
    if any(ind in lower_output for ind in ["同步出错", "程序异常", "traceback", "获取访问令牌失败", "app secret invalid"]):
        ok = False
    # 仅当包含 “- ERROR -” 且不属于白名单语境时，判定失败
    elif " - error - " in lower_output and not any(w in lower_output for w in whitelist_markers):
        ok = False
    return proc.returncode, ok, output


def parse_args():
    parser = argparse.ArgumentParser(description="MySQL → Excel → Feishu Bitable 单表导入（从 YAML 读取全部配置）")
    parser.add_argument("--config", default="_tmp_xtf_config.yaml", help="XTF 配置文件路径，内含 source/mysql 与 feishu 配置")
    return parser.parse_args()


def main():
    args = parse_args()

    # 0) 读取 YAML 配置
    cfg_path = Path(args.config).expanduser().resolve()
    print(f"🧩 读取配置: {cfg_path}")
    cfg = load_yaml_config(cfg_path)

    # 1) 解析 MySQL 源配置
    source = cfg.get('source', {}) if isinstance(cfg.get('source', {}), dict) else {}
    host = source.get('host')
    port = int(source.get('port', 3306))
    username = source.get('username')
    password = source.get('password')
    database = source.get('database')
    table = source.get('table')
    sql_text = source.get('sql')

    # 2) 校验
    missing = [k for k, v in {
        'host': host,
        'username': username,
        'password': password,
        'database': database,
    }.items() if v in (None, '')]
    if missing:
        print(f"❌ 配置缺失: {missing}")
        sys.exit(1)

    # 3) 读取 MySQL
    print("📥 正在从 MySQL 读取数据...")
    uri = build_mysql_uri(str(host), int(port), str(username), str(password), str(database))
    df = read_mysql_to_df(uri, str(database), table=str(table) if table else None, sql=sql_text)
    print(f"✅ 读取完成: {len(df)} 行 × {len(df.columns)} 列")
    if df.empty:
        print("⚠️ 查询结果为空，已退出")
        sys.exit(0)

    # 4) 导出 Excel 到 YAML 指定路径（若未设置则使用默认路径）
    file_path_cfg = cfg.get('file_path') or "_tmp_mysql_export.xlsx"
    excel_path = Path(file_path_cfg).expanduser().resolve()
    print(f"📄 导出 Excel: {excel_path}")
    write_temp_excel(df, excel_path)

    # 5) 临时覆盖原 YAML（去除 source，更新 file_path），执行完毕后恢复
    original_text = cfg_path.read_text(encoding='utf-8')
    cleaned_text = build_clean_config_text(cfg, excel_path)
    try:
        cfg_path.write_text(cleaned_text, encoding='utf-8')
        # 6) 调用 XTF 引擎执行同步
        print("🚀 调用 XTF 引擎执行同步...")
        rc, ok, output = run_xtf_with_config(cfg_path)
    finally:
        # 恢复原配置（包含 source 段）
        try:
            cfg_path.write_text(original_text, encoding='utf-8')
        except Exception:
            pass
    if ok:
        print("\n✅ 同步流程结束 (返回码 0)")
    else:
        print(f"\n❌ 同步流程失败 (返回码 {rc})")
        tail = (output or "").splitlines()[-30:]
        if tail:
            print("\n".join(tail))
    sys.exit(0 if ok else (rc or 1))


if __name__ == "__main__":
    main()


