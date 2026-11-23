# 农产品残留限量版本与复检裁决

面向农产品检测站复核员，处理采信、判定、核算、复核与归档。

## 启动

```bash
docker compose up --build -d
```

访问 http://localhost:18254 。初次启动自动运行脏样例，可切换 clean.csv 与 legacy.csv。顶部“运行批次”会重新计算，同源输入复用批次。

## 业务模块

- 报告采信：`/m1_ingest`、`/api/m1_ingest`
- 项目映射：`/m2_mapping`、`/api/m2_mapping`
- 限量选版：`/m3_versions`、`/api/m3_versions`
- 检出量换算：`/m4_segments`、`/api/m4_segments`
- 复检裁决：`/m5_calculate`、`/api/m5_calculate`
- 批次判级：`/m6_reconcile`、`/api/m6_reconcile`
- 报告归档：`/m7_archive`、`/api/m7_archive`
- 封存保留：`/m8_retention`、`/api/m8_retention`

## 核心规则

按采样日期选择有效限量，低于检出限保留区间；复检不得越过样本关联与方法适用范围。完整数值、边界和异常定义见 `docs/DATA-DICTIONARY.md`。

## 封存保留账

封存空间告急时，由 `/m8_retention` 裁决本次可压缩范围，并形成可复核、可下载的保留账：

- **可压缩范围**：仅“已出证”（复核通过 + 对账平 + 归档就绪）且距出证已达保留天数的批次；
- **必须保留原件**：仍被未完结复检引用、或所属批次仍在复核的记录不能压缩，账内逐条列出阻塞对象与原因；
- **可还原证据**：压缩只丢弃重复原始副本与源文件明文，保留摘要（`source_sha256`）、记录级摘要、复检引用、隔离原因与按批次/样本/复检关系组织的证据清单；`/api/runs/<id>/restore` 可还原原件；
- **归档放行只认这一版**：归档导出仅在当前保留账“复核通过”且未过期时放行，否则返回 409；
- **失败回滚**：写入单事务，压缩失败整体回滚，旧记录与旧保留账原样恢复；
- **同源不重复出账**：同一来源（运行集合与复核状态未变）再次处理返回同一保留版本；
- **复核版本控制**：保留账复核需提交当前版本，陈旧复核返回 409，与批次复核一致。

保留在账内不落库的副本可回收；容量按 `ARCHIVE_CAPACITY_BYTES`（默认 2MB）、保留期按 `RETENTION_DAYS`（默认 0 天）配置。保留账可下载为单份 CSV：`/api/m8_retention/<id>/export.csv`。

## 验证

```bash
docker compose exec -T app pytest -q
docker compose config --quiet
```

原始样例在 `sample_data/`。测试在 `tests/`。封存文件是单份CSV，含运行清单、计算证据、隔离原因与对账。复核需提交当前版本，陈旧复核返回409。压缩后的批次仍可由源摘要还原，证据包校验与压缩前一致。

## 数据与限制

SQLite保存在records命名卷，普通重启与构建不会清除。确认可丢弃本项目数据后，运行 `docker compose down -v` 再启动可重置。单文件上限2MB；不接入外部业务系统；历史规则由本项目样例定义，不代表实际行业标准。封存空间与保留期通过环境变量 `ARCHIVE_CAPACITY_BYTES`、`RETENTION_DAYS` 配置。
