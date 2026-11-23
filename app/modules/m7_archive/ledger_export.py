"""保留账导出：单份 CSV，含摘要、可压缩范围、可还原证据、阻塞对象与隔离原因。"""
import csv
import hashlib
import io
import json


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def bundle(ledger):
    out = io.StringIO(newline='')
    writer = csv.writer(out)
    writer.writerow(['记录类型', '批次', '样本', '业务ID', '行', '原因/动作', '内容'])
    identity = ledger.get('ledger_id') if ledger.get('ledger_id') is not None else 'preview'
    summary = {key: ledger.get(key) for key in
               ('policy', 'rule_version', 'fingerprint', 'source_key', 'generated_at',
                'retention_days', 'version', 'decision', 'status', 'totals', 'usage',
                'archive_release', 'digests')}
    writer.writerow(['保留摘要', '', '', identity, '', ledger.get('decision', ''),
                     _json(summary)])
    for batch in ledger.get('batches', []):
        writer.writerow(['批次', batch.get('batch'), '', batch.get('run_id'), '',
                         batch.get('action'),
                         _json({key: batch.get(key) for key in
                                ('decision', 'issued_at', 'expired', 'archive_ready', 'compressed',
                                 'samples', 'records', 'compressible', 'retained')})])
    for group in ledger.get('restorable', []):
        for record in group.get('records', []):
            writer.writerow(['可还原证据', group.get('batch'), group.get('sample_id'),
                             record.get('id'), record.get('line'), record.get('action'),
                             _json(record)])
    for blocker in ledger.get('blockers', []):
        writer.writerow(['阻塞对象', blocker.get('batch'), blocker.get('sample_id'),
                         blocker.get('record_id'), blocker.get('line'), blocker.get('type'),
                         _json(blocker)])
    for item in ledger.get('quarantine', []):
        writer.writerow(['隔离原因', '', '', item.get('id'), item.get('line'), item.get('code'),
                         _json(item)])
    return '﻿' + out.getvalue()


def checksum(ledger):
    return hashlib.sha256(bundle(ledger).encode('utf-8')).hexdigest()
