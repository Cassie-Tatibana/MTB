# NOTICE（项目组成与来源）

本仓库包含两部分：

1) 外层调度与包装代码（原创）
- 文件：`main.py`、`models.py`、`sync_runner.py`、`mysql_to_bitable.py`、`templates/*`、`docs/*`、`README.md` 等
- 版权：© 2025 Cassie Tatibana (猫猫)
- 许可：MIT（详见仓库根目录 LICENSE）；并附带免责声明：本项目与飞书（Lark）、字节跳动无从属与背书关系

2) 上游开源项目 XTF（第三方，未修改原始代码）
- 路径：`XTF-main/`
- 仓库：BlueSkyXN/XTF
- 提交：4a7113faa0f8258f8aba043730d6e42ab4e0a478
- 许可：GPL-3.0
- 链接：<https://github.com/BlueSkyXN/XTF/tree/4a7113faa0f8258f8aba043730d6e42ab4e0a478>

说明与合规：
- 本仓库将 XTF 作为能力引擎以子进程方式调用，属于同一仓库内的“聚合分发”；请保留 `XTF-main/` 目录内的 LICENSE/README/版权声明，不得删除或修改
- 若后续对 `XTF-main/` 做出修改并发布，需按 GPL-3.0 开源这些修改并保留原始声明
- 为降低合规复杂度，推荐在公开发布时将 `XTF-main/` 改为 Git 子模块或安装时下载的外部依赖，并在文档中注明固定提交版本与链接