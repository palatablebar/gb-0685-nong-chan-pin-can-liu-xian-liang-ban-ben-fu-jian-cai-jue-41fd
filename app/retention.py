"""封存区保留裁决（保留账）。

把“已出证批次能否压缩原件”收敛为一版可复核、可下载、可回滚的保留账：

* 只有已出证（复核通过 + 对账平 + 归档就绪）且到期的批次进入压缩候选；
* 仍被未完结复检引用、或所属批次仍在复核时必须保留原件，并列出阻塞对象；
* 压缩丢弃重复原始副本与源文件明文，摘要、隔离原因、复检引用与可还原证据留在账内；
* 同一来源（运行集合与复核状态未变）再次处理返回同一保留版本，不重复出账；
* 写入在单事务内完成，任何一步失败整体回滚，旧记录与旧保留账原样恢复。
"""
import base64
import hashlib
import json
import os
import zlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone

POLICY = 'retention-v1'
DEFAULT_RETENTION_DAYS = 0
DEFAULT_CAPACITY_BYTES = 2_000_000

EVIDENCE_KEYS = ['id', 'sample_id', 'batch', 'product', 'analyte', 'method', 'version',
                 'sampled', 'reported', 'result', 'unit', 'lod', 'relation', 'standard_id',
                 'limit', 'lower', 'upper', 'upper_exclusive', 'code', 'note']
SUMMARY_KEYS = ['manifest', 'archive', 'reconcile', 'calculations', 'business', 'quarantine',
                'input_count', 'blank_count', 'header_count', 'duplicate_count', 'encoding',
                'parameters', 'run_id']


def settings():
    def _int(name, default):
        try:
            return int(os.environ.get(name, default))
        except (TypeError, ValueError):
            return default
    return {'policy': POLICY,
            'retention_days': _int('RETENTION_DAYS', DEFAULT_RETENTION_DAYS),
            'capacity_bytes': _int('ARCHIVE_CAPACITY_BYTES', DEFAULT_CAPACITY_BYTES)}


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def _iso(value):
    return value.astimezone(timezone.utc).isoformat() if value else None


def _parse(value):
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _rule_version():
    from app.config import RULE_VERSION
    return RULE_VERSION


def is_compressed(body):
    return body.get('retention', {}).get('state') == 'compressed'


def source_of(body):
    """返回运行原件字节：压缩运行由 source_z 解压，未压缩直接用 source_b64。"""
    if body.get('source_b64'):
        return base64.b64decode(body['source_b64'])
    if body.get('source_z'):
        return zlib.decompress(base64.b64decode(body['source_z']))
    return None


def _record(item):
    record = {key: item.get(key) for key in EVIDENCE_KEYS if key in item}
    record['line'] = item.get('_line', item.get('line'))
    record['relation'] = item.get('relation') or ''
    record['record_sha256'] = digest(item)
    return record


def expand(body):
    """把压缩运行展开为完整运行，保留原摘要、对账、计算与隔离记录。

    若无法取得原件（旧库缺失 source），退回既有 measurement/evidence，不伪造数据。
    """
    raw = source_of(body)
    if raw is None:
        return deepcopy(body)
    from app.workflow import run as execute
    try:
        state = execute(raw, dict(body.get('manifest', {}).get('parameters', {})))
    except Exception:
        return deepcopy(body)
    for key in SUMMARY_KEYS:
        if key in body:
            state[key] = deepcopy(body[key])
    state.pop('retention', None)
    state.pop('evidence', None)
    state.pop('source_z', None)
    return state


def records_of(body):
    """记录视图：压缩运行读 evidence，未压缩运行读 measurements；均带 conclusion 与引用。"""
    calc_lines = {row.get('line') for row in body.get('calculations', [])}
    if is_compressed(body):
        records = []
        for entry in body.get('evidence', []):
            record = dict(entry)
            record['adopted'] = record.get('line') in calc_lines
            records.append(record)
        return records
    records = []
    for item in body.get('measurements', []):
        record = _record(item)
        record['adopted'] = record.get('line') in calc_lines
        records.append(record)
    return records


def evidence_records(body, keep_lines):
    """按保留行号生成压缩后的证据清单（与 compact 写入的一致）。"""
    keep = set(keep_lines)
    evidence = []
    measurements = body.get('measurements', [])
    if not measurements and body.get('evidence'):
        for entry in body['evidence']:
            record = dict(entry)
            record['retained'] = record.get('line') in keep
            evidence.append(record)
    else:
        for item in measurements:
            record = _record(item)
            record['retained'] = record.get('line') in keep
            evidence.append(record)
    return evidence


def compact(body, keep_lines):
    """压缩运行：只保留需要保留记录的原件，其余仅留摘要与引用。"""
    keep = set(keep_lines)
    evidence = evidence_records(body, keep_lines)
    kept = [item for item, record in zip(body.get('measurements', []), evidence) if record['retained']]
    out = {key: deepcopy(body[key]) for key in SUMMARY_KEYS if key in body}
    out['measurements'] = kept
    out['rows'] = [row for row in body.get('rows', []) if row.get('_line') in keep]
    out['reports'] = [row for row in body.get('reports', []) if row.get('_line') in keep]
    out['evidence'] = evidence
    source = source_of(body)
    if source is not None:
        out['source_z'] = base64.b64encode(zlib.compress(source, 9)).decode('ascii')
    out['retention'] = {'policy': POLICY, 'state': 'compressed', 'kept_lines': sorted(keep),
                        'record_count': len(evidence),
                        'source_sha256': hashlib.sha256(source).hexdigest() if source else None,
                        'evidence_sha256': digest(evidence)}
    return out


def _is_issued(view):
    body = view['body']
    return (view['decision'] == '通过' and bool(body.get('reconcile', {}).get('balanced'))
            and bool(body.get('archive', {}).get('ready')))


def _issued_at(view):
    return _parse(view.get('issued_at')) or _parse(view['body'].get('manifest', {}).get('generated_at'))


def _reference_index(views):
    """收集各运行内的复检引用。

    复检引用在来源运行内解析；跨运行只在来源运行自身缺少该目标ID时才可能指向
    其他运行，保留 own_ids 以区分“运行内复检”与“悬空/跨运行复检”。
    """
    refs = []
    for view in views:
        calc_lines = {row.get('line') for row in view['body'].get('calculations', [])}
        records = records_of(view['body'])
        own_ids = {record.get('id') for record in records}
        for record in records:
            if not record.get('relation'):
                continue
            refs.append({'run_id': view['id'], 'decision': view['decision'],
                         'record_id': record.get('id'), 'target_id': record['relation'],
                         'sample_id': record.get('sample_id'), 'batch': record.get('batch'),
                         'line': record.get('line'), 'own_ids': own_ids,
                         'active': record.get('line') in calc_lines})
    return refs


def _reference_blockers(record, refs, view_id):
    """列出指向该记录、且未完结的复检引用。

    “未完结”指引用该记录的复检所属运行尚未复核通过；已通过运行内的复检无论被
    采纳还是被隔离，结论均已定，不再阻塞原件。跨运行引用仅在来源运行自身没有
    该目标ID（悬空复检指向其他来源）时生效，避免独立来源因 ID 重号互相阻塞。
    """
    blocked = []
    for ref in refs:
        if ref['decision'] == '通过':
            continue
        if ref['target_id'] != record.get('id') or ref['sample_id'] != record.get('sample_id'):
            continue
        if record.get('batch') and ref.get('batch') and ref['batch'] != record.get('batch'):
            continue
        if ref['run_id'] != view_id and record.get('id') in ref.get('own_ids', set()):
            continue
        blocked.append({'type': '未完结复检', 'run_id': ref['run_id'],
                        'record_id': ref['record_id'],
                        'reason': '复检批次仍在复核（' + ref['decision'] + '）'})
    unique = {canonical(item): item for item in blocked}
    return list(unique.values())


def source_key(views, policy):
    parts = [{'id': view['id'], 'version': view['version'], 'decision': view['decision'],
              'issued_at': view.get('issued_at'),
              'source_sha256': view['body'].get('manifest', {}).get('sha256')} for view in views]
    return digest({'policy': policy, 'runs': parts})


def build_plan(views, now=None):
    config = settings()
    now = now or datetime.now(timezone.utc)
    days, capacity = config['retention_days'], config['capacity_bytes']
    refs = _reference_index(views)

    batches, restorable, quarantine = [], [], []
    scope = {'run_ids': [], 'batches': [], 'records': [], 'retained_records': []}
    updates, snapshots = {}, {}
    totals = {'runs': len(views), 'issued': 0, 'expired': 0, 'batches': 0, 'records': 0,
              'compressible': 0, 'retained': 0, 'blocked': 0, 'restored': 0}
    stored_bytes = reclaimable = 0
    evidence_parts, blocker_flat = [], []

    for view in views:
        body = view['body']
        stored_bytes += len(canonical(body))
        was_compressed = is_compressed(body)
        records = records_of(body)
        totals['records'] += len(records)
        issued, issued_at = _is_issued(view), _issued_at(view)
        expired = bool(issued and issued_at and issued_at + timedelta(days=days) <= now)
        totals['issued'] += int(issued)
        totals['expired'] += int(expired)

        run_blockers = []
        if view['decision'] != '通过':
            run_blockers.append({'type': '仍在复核', 'run_id': view['id'], 'record_id': '',
                                 'reason': '批次复核状态：' + view['decision']})
        elif not issued:
            run_blockers.append({'type': '证据未就绪', 'run_id': view['id'], 'record_id': '',
                                 'reason': '对账未平或归档校验未通过'})
        elif not expired:
            run_blockers.append({'type': '未到期', 'run_id': view['id'], 'record_id': '',
                                 'reason': '出证时间 ' + str(_iso(issued_at))})

        decided = []
        for record in records:
            blockers = list(run_blockers)
            if issued and expired:
                blockers += _reference_blockers(record, refs, view['id'])
            compressible = issued and expired and not blockers
            decided.append({'record': record, 'blockers': blockers, 'compress': compressible})
            if compressible:
                totals['compressible'] += 1
                scope['records'].append({'run_id': view['id'], 'line': record.get('line'),
                                         'id': record.get('id'), 'batch': record.get('batch'),
                                         'sample_id': record.get('sample_id')})
            else:
                totals['retained'] += 1
                if blockers:
                    totals['blocked'] += 1
                    scope['retained_records'].append({'run_id': view['id'], 'line': record.get('line'),
                                                      'id': record.get('id'), 'batch': record.get('batch'),
                                                      'sample_id': record.get('sample_id'),
                                                      'blockers': blockers})
                    blocker_flat += [dict(b, id=record.get('id'), line=record.get('line'),
                                          batch=record.get('batch'), sample_id=record.get('sample_id'))
                                     for b in blockers]

        keep_lines = sorted(item['record'].get('line') for item in decided if not item['compress'])
        any_compressible = any(item['compress'] for item in decided)
        projected_evidence = evidence_records(expand(body) if was_compressed else body, keep_lines)
        evidence_parts.append({'run_id': view['id'], 'sha256': digest(projected_evidence)})
        if was_compressed:
            current_keep = set(body.get('retention', {}).get('kept_lines', []))
            if set(keep_lines) == current_keep:
                state_action = '已压缩'
            elif any_compressible:
                snapshots[view['id']] = deepcopy(body)
                updates[view['id']] = compact(expand(body), keep_lines)
                state_action = '压缩'
            else:
                snapshots[view['id']] = deepcopy(body)
                updates[view['id']] = expand(body)
                state_action = '恢复'
                totals['restored'] += 1
        elif any_compressible:
            snapshots[view['id']] = deepcopy(body)
            updates[view['id']] = compact(body, keep_lines)
            state_action = '压缩'
        else:
            state_action = '保留'
        if view['id'] in updates:
            reclaimable += max(len(canonical(body)) - len(canonical(updates[view['id']])), 0)
            if state_action == '压缩':
                scope['run_ids'].append(view['id'])

        grouped = {}
        for item in decided:
            grouped.setdefault(item['record'].get('batch') or '', []).append(item)
        for batch, items in sorted(grouped.items()):
            if state_action == '压缩':
                action = '压缩' if all(item['compress'] for item in items) else '保留'
            else:
                action = state_action
            if action == '压缩':
                scope['batches'].append({'run_id': view['id'], 'batch': batch})
            batch_blockers = [dict(b, batch=batch, id=item['record'].get('id'),
                                  line=item['record'].get('line'))
                              for item in items for b in item['blockers']]
            totals['batches'] += 1
            batches.append({'run_id': view['id'], 'batch': batch, 'decision': view['decision'],
                            'issued_at': _iso(issued_at), 'expired': expired,
                            'archive_ready': issued, 'compressed': was_compressed,
                            'action': action,
                            'samples': len({i['record'].get('sample_id') for i in items}),
                            'records': len(items),
                            'compressible': [i['record'].get('id') for i in items if i['compress']],
                            'retained': [i['record'].get('id') for i in items if not i['compress']],
                            'blockers': batch_blockers})
            samples = {}
            for item in items:
                record = item['record']
                samples.setdefault(record.get('sample_id'), []).append({
                    'id': record.get('id'), 'line': record.get('line'),
                    'retest_of': record.get('relation') or '', 'verdict': record.get('code'),
                    'standard': record.get('standard_id'), 'limit': record.get('limit'),
                    'lower': record.get('lower'), 'upper': record.get('upper'),
                    'record_sha256': record.get('record_sha256'),
                    'action': '压缩' if item['compress'] else '保留',
                    'blockers': item['blockers']})
            for sample, rows in sorted(samples.items(), key=lambda pair: str(pair[0])):
                restorable.append({'run_id': view['id'], 'batch': batch, 'sample_id': sample,
                                   'records': rows})
        for entry in body.get('quarantine', []):
            quarantine.append({'run_id': view['id'], 'line': entry.get('line'),
                               'id': entry.get('id'), 'code': entry.get('code'),
                               'raw_sha256': digest(entry.get('raw'))})

    projected = max(stored_bytes - reclaimable, 0)
    release_ok = projected <= capacity
    key = source_key(views, config['policy'])
    if updates:
        state = '恢复' if totals['restored'] else '压缩'
    else:
        state = '已压缩' if any(is_compressed(v['body']) for v in views) else '保留'
    ledger = {
        'policy': config['policy'], 'rule_version': _rule_version(), 'generated_at': _iso(now),
        'retention_days': days, 'source_key': key,
        'fingerprint': digest({'source': key, 'policy': config['policy'],
                               'rule': _rule_version()}),
        'usage': {'stored_bytes': stored_bytes, 'reclaimable_bytes': reclaimable,
                  'projected_bytes': projected},
        'archive_release': {'allowed': release_ok, 'status': '放行' if release_ok else '拒绝',
                            'capacity_bytes': capacity, 'projected_bytes': projected,
                            'reason': '压缩后可放行新批次' if release_ok else '容量不足，需先解除保留阻塞'},
        'state': state,
        'totals': totals,
        'scope': scope,
        'batches': batches,
        'restorable': restorable,
        'blockers': blocker_flat,
        'quarantine': quarantine,
        'digests': {'source': digest([{'id': v['id'],
                                       'source': v['body'].get('manifest', {}).get('sha256')}
                                      for v in views]),
                    'evidence': digest([{'run_id': part['run_id'], 'sha256': part['sha256']}
                                        for part in evidence_parts]),
                    'evidence_by_run': {part['run_id']: part['sha256'] for part in evidence_parts},
                    'quarantine': digest(quarantine)},
        'status': '已裁决',
    }
    return ledger, updates, snapshots


def preview(views):
    ledger, _, _ = build_plan(views)
    ledger.update(ledger_id=None, version=None, decision='预览', applied=False,
                  created_at=None, reviewed_at=None)
    ledger['archive_release'] = dict(ledger['archive_release'], status='预览')
    return ledger


def adjudicate(views, failpoint=None, now=None):
    """裁决并落账；单事务，失败回滚旧版。同源重复处理返回既有版本。"""
    from app import store
    config = settings()
    key = source_key(views, config['policy'])
    existing = store.find_ledger(config['policy'], key)
    if existing:
        return existing
    ledger, updates, snapshots = build_plan(views, now=now)
    ledger['source_key'] = key
    return store.commit_ledger(config['policy'], key, ledger, updates, snapshots, failpoint=failpoint)


def release_status(ledger, views=None):
    """归档放行只认当前保留账：缺账、未复核、退回或来源已变都不放行。"""
    if not ledger:
        return {'allowed': False, 'status': '缺少保留账',
                'reason': '尚未裁决保留账，不能放行归档'}
    if views is not None and ledger.get('source_key'):
        if ledger['source_key'] != source_key(views, ledger.get('policy', POLICY)):
            return {'allowed': False, 'status': '保留账已过期',
                    'reason': '批次或复核状态已变，请重新裁决保留账'}
    if ledger.get('decision') != '通过':
        return {'allowed': False, 'status': ledger.get('decision') or '待复核',
                'reason': '保留账尚未复核通过'}
    return ledger.get('archive_release', {'allowed': False, 'status': '拒绝'})


def restore_run(views, run_id):
    """按保留账还原单一批次原件，用于复核与审计。"""
    for view in views:
        if view['id'] == run_id:
            return expand(view['body']) if is_compressed(view['body']) else deepcopy(view['body'])
    from app.errors import DomainError
    raise DomainError('NOT_FOUND', '批次不存在', 404)
