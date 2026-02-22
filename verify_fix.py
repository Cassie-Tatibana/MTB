import sys
import os
from datetime import datetime, timedelta

# 确保能导入当前目录的模块
sys.path.append(os.getcwd())

try:
    from models import now_cn_naive
    print("✅ 成功从 models 导入 now_cn_naive")
except ImportError as e:
    print(f"❌ 导入失败，请检查是否已应用 models.py 的修改: {e}")
    sys.exit(1)

print("\n--- 时区修复验证 ---")
cn_time = now_cn_naive()
utc_time = datetime.utcnow()
local_time = datetime.now()

print(f"生成的北京时间 (models): {cn_time}")
print(f"标准 UTC 时间:          {utc_time}")
print(f"系统本地时间:           {local_time}")

# 验证逻辑：北京时间应该比 UTC 时间大约快 8 小时
# 允许 1 分钟的误差
diff = cn_time - utc_time
target_diff = timedelta(hours=8)
margin = timedelta(minutes=1)

print(f"\n时间差 (北京 - UTC): {diff}")

if target_diff - margin < diff < target_diff + margin:
    print("✅ 验证通过：生成的时间是正确的北京时间（UTC+8）")
else:
    print("❌ 验证失败：时间差不是 8 小时，请检查服务器时区设置或 ZoneInfo 支持情况")