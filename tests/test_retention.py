import csv
import io
import json

import pytest

from app import store, retention
from app.errors import DomainError
from app.workflow import load, run
from app.modules.m7_archive.ledger_export import bundle as ledger_bundle


def seed(tmp_path, monkeypatch, decision=None):
    monkeypatch.setenv('DB_PATH', str(tmp_path / 'r.db'))
    state = run(load('dirty.csv'), {})
    store.save(state['run_id'], state)
    if decision:
        store.review(state['run_id'], 1, decision)
    return state['run_id']


def test_pending_review_blocks_everything(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch)
    plan = retention.preview(store.views())
    assert plan['totals']['compressible'] == 0
    assert plan['totals']['retained'] == plan['totals']['records']
    assert {b['type'] for b in plan['blockers']} == {'仍在复核'}
    assert retention.release_status(store.current_ledger(), store.views())['allowed'] is False


def test_expired_issued_batch_is_compressible(tmp_path, monkeypatch):
    seed(tmp_path, monkeypatch, '通过')
    plan = retention.preview(store.views())
    assert plan['totals']['compressible'] == plan['totals']['records'] > 0
    assert plan['totals']['blocked'] == 0
    assert plan['archive_release']['allowed'] is True
    assert all(b['action'] == '压缩' for b in plan['batches'])


def test_not_due_retention_keeps_originals(tmp_path, monkeypatch):
    monkeypatch.setenv('RETENTION_DAYS', '30')
    seed(tmp_path, monkeypatch, '通过')
    plan = retention.preview(store.views())
    assert plan['totals']['expired'] == 0
    assert plan['totals']['compressible'] == 0
    assert {b['type'] for b in plan['blockers']} == {'未到期'}


def test_unfinished_retest_keeps_only_referenced_record(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    header = 'id,sample_id,batch,product,analyte,method,version,sampled,reported,result,unit,lod,relation,note\n'
    body = '00002,S001,B001,苹果,P01,LC-MS/MS,2,2026-08-01,2026-08-03,0.004,mg/kg,0.001,00001,复检\n'
    pending = run((header + body).encode(), {})
    store.save(pending['run_id'], pending)
    plan = retention.preview(store.views())
    blocked = [b for b in plan['batches'] if b['run_id'] == run_id and b['batch'] == 'B001'][0]
    assert blocked['action'] == '保留'
    assert blocked['retained'] == ['00001']
    assert blocked['blockers'][0]['type'] == '未完结复检'
    assert any(k['id'] == '00001' for k in plan['blockers'])


def test_compression_preserves_digest_and_reference(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    ledger = retention.adjudicate(store.views())
    body = store.get(run_id)
    assert retention.is_compressed(body)
    assert body['retention']['source_sha256'] == body['manifest']['sha256']
    evidence = {row['id']: row for row in body['evidence']}
    assert evidence['00002']['relation'] == '00001'
    assert evidence['00002']['record_sha256']
    assert all('code' in row for row in body['quarantine'])
    assert ledger['digests']['evidence_by_run'][run_id] == body['retention']['evidence_sha256']


def test_quarantine_and_summary_survive_compression(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    before = store.get(run_id)
    ledger = retention.adjudicate(store.views())
    after = store.get(run_id)
    assert [q['code'] for q in after['quarantine']] == [q['code'] for q in before['quarantine']]
    assert after['reconcile'] == before['reconcile']
    assert after['calculations'] == before['calculations']
    assert after['archive'] == before['archive']
    assert ledger['digests']['quarantine']


def test_restore_reconstructs_full_records(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    original = store.get(run_id)
    retention.adjudicate(store.views())
    restored = retention.restore_run(store.views(), run_id)
    assert restored['calculations'] == original['calculations']
    assert restored['quarantine'] == original['quarantine']
    assert len(restored['measurements']) == len(original['measurements'])
    assert retention.source_of(store.get(run_id)) == retention.source_of(original)


def test_same_source_does_not_duplicate_ledger(tmp_path, monkeypatch):
    seed(tmp_path, monkeypatch, '通过')
    first = retention.adjudicate(store.views())
    second = retention.adjudicate(store.views())
    assert first['ledger_id'] == second['ledger_id']
    assert len(store.ledgers()) == 1


def test_failed_commit_rolls_back_old_version(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    before = json.dumps(store.get(run_id), sort_keys=True, ensure_ascii=False)

    def boom(db):
        raise RuntimeError('磁盘写入失败')

    with pytest.raises(RuntimeError):
        retention.adjudicate(store.views(), failpoint=boom)
    after = json.dumps(store.get(run_id), sort_keys=True, ensure_ascii=False)
    assert after == before
    assert store.ledgers() == []
    assert not retention.is_compressed(store.get(run_id))


def test_reopen_restores_originals(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    retention.adjudicate(store.views())
    assert retention.is_compressed(store.get(run_id))
    store.review(run_id, 2, '退回')
    ledger = retention.adjudicate(store.views())
    assert ledger['state'] == '恢复'
    assert not retention.is_compressed(store.get(run_id))
    assert len(store.get(run_id)['measurements']) == 36


def test_ledger_export_contains_required_sections(tmp_path, monkeypatch):
    seed(tmp_path, monkeypatch, '通过')
    ledger = retention.adjudicate(store.views())
    rows = list(csv.reader(io.StringIO(ledger_bundle(ledger).lstrip('﻿'))))
    kinds = {row[0] for row in rows[1:]}
    assert {'保留摘要', '批次', '可还原证据', '隔离原因'} <= kinds
    assert rows[0][0] == '记录类型'


def test_ledger_review_requires_matching_version(tmp_path, monkeypatch):
    seed(tmp_path, monkeypatch, '通过')
    ledger = retention.adjudicate(store.views())
    approved = store.review_ledger(ledger['ledger_id'], ledger['version'], '通过')
    assert approved['version'] == ledger['version'] + 1
    with pytest.raises(DomainError) as error:
        store.review_ledger(ledger['ledger_id'], ledger['version'], '退回')
    assert error.value.status == 409


def test_archive_release_requires_reviewed_ledger(tmp_path, monkeypatch):
    seed(tmp_path, monkeypatch, '通过')
    assert retention.release_status(store.current_ledger(), store.views())['allowed'] is False
    ledger = retention.adjudicate(store.views())
    pending = retention.release_status(store.current_ledger(), store.views())
    assert pending['allowed'] is False and pending['status'] == '待复核'
    store.review_ledger(ledger['ledger_id'], ledger['version'], '通过')
    approved = retention.release_status(store.current_ledger(), store.views())
    assert approved['allowed'] is True


def test_archive_release_rejects_stale_ledger(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    ledger = retention.adjudicate(store.views())
    store.review_ledger(ledger['ledger_id'], ledger['version'], '通过')
    store.review(run_id, 2, '退回')
    stale = retention.release_status(store.current_ledger(), store.views())
    assert stale['allowed'] is False and stale['status'] == '保留账已过期'


def test_capacity_shortage_blocks_release(tmp_path, monkeypatch):
    seed(tmp_path, monkeypatch, '通过')
    monkeypatch.setenv('ARCHIVE_CAPACITY_BYTES', '1000')
    ledger = retention.adjudicate(store.views())
    assert ledger['archive_release']['allowed'] is False
    assert ledger['archive_release']['status'] == '拒绝'
    assert ledger['usage']['projected_bytes'] > ledger['archive_release']['capacity_bytes']


def test_compression_shrinks_stored_body(tmp_path, monkeypatch):
    run_id = seed(tmp_path, monkeypatch, '通过')
    before = len(json.dumps(store.get(run_id), ensure_ascii=False))
    ledger = retention.adjudicate(store.views())
    after = len(json.dumps(store.get(run_id), ensure_ascii=False))
    assert after < before
    assert ledger['usage']['reclaimable_bytes'] > 0


def test_ledger_is_reviewable_and_downloadable(client):
    preview = client.get('/api/m8_retention').get_json()
    assert preview['decision'] == '预览' and preview['ledger_id'] is None  # GET never mutates
    ledger = client.post('/api/m8_retention', json={}).get_json()
    export = client.get(f"/api/m8_retention/{ledger['ledger_id']}/export.csv")
    assert export.status_code == 200
    assert export.headers['Content-Disposition'].endswith('retention-ledger.csv')
