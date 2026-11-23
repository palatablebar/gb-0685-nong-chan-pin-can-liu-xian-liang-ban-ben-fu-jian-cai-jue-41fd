import sqlite3,json,os
from datetime import datetime,timezone
from pathlib import Path
from app.errors import DomainError

def _now():return datetime.now(timezone.utc).isoformat()

def connection():
    path=Path(os.environ.get('DB_PATH','data/records.db'));path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path,timeout=10);db.row_factory=sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, body TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, decision TEXT NOT NULL DEFAULT "待复核")')
    db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS retention_ledgers (id INTEGER PRIMARY KEY AUTOINCREMENT,policy TEXT NOT NULL,source_key TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 1,decision TEXT NOT NULL DEFAULT "待复核",body TEXT NOT NULL,created_at TEXT NOT NULL,reviewed_at TEXT,UNIQUE(policy,source_key))')
    columns={row['name'] for row in db.execute('PRAGMA table_info(runs)')}
    if 'issued_at' not in columns:db.execute('ALTER TABLE runs ADD COLUMN issued_at TEXT')
    return db

def save(run_id, body):
    with connection() as db:
        db.execute('INSERT OR IGNORE INTO runs(id,body) VALUES (?,?)',(run_id,json.dumps(body,ensure_ascii=False)))
        db.execute("INSERT INTO meta(key,value) VALUES ('latest',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(run_id,))
    return get(run_id)

def get(run_id):
    with connection() as db:r=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
    if not r:raise DomainError('NOT_FOUND','批次不存在',404)
    body=json.loads(r['body']);body.update(run_id=r['id'],review_version=r['version'],decision=r['decision']);body['issued_at']=r['issued_at'];return body

def history():
    with connection() as db:rows=db.execute("SELECT id,version,decision,issued_at FROM runs ORDER BY CASE WHEN id=(SELECT value FROM meta WHERE key='latest') THEN 0 ELSE 1 END,rowid DESC LIMIT 100").fetchall()
    return [dict(r) for r in rows]

def views():
    """全部运行及其完整正文，供保留裁决构建证据面。"""
    with connection() as db:rows=db.execute('SELECT id,version,decision,issued_at,body FROM runs ORDER BY rowid').fetchall()
    return [{'id':r['id'],'version':r['version'],'decision':r['decision'],'issued_at':r['issued_at'],'body':json.loads(r['body'])} for r in rows]

def review(run_id, version, decision):
    if decision not in ('通过','退回'):raise DomainError('DECISION','复核状态无效')
    with connection() as db:
        issued=_now() if decision=='通过' else None
        cur=db.execute('UPDATE runs SET version=version+1,decision=?,issued_at=? WHERE id=? AND version=?',(decision,issued,run_id,version))
        if cur.rowcount!=1:
            if not db.execute('SELECT 1 FROM runs WHERE id=?',(run_id,)).fetchone():raise DomainError('NOT_FOUND','批次不存在',404)
            raise DomainError('STALE','复核版本已更新，请刷新',409)
    return get(run_id)

def find_ledger(policy, source_key):
    with connection() as db:r=db.execute('SELECT * FROM retention_ledgers WHERE policy=? AND source_key=?',(policy,source_key)).fetchone()
    return _ledger_view(r) if r else None

def current_ledger():
    with connection() as db:r=db.execute('SELECT * FROM retention_ledgers ORDER BY id DESC LIMIT 1').fetchone()
    return _ledger_view(r) if r else None

def ledger(ledger_id):
    with connection() as db:r=db.execute('SELECT * FROM retention_ledgers WHERE id=?',(ledger_id,)).fetchone()
    if not r:raise DomainError('NOT_FOUND','保留账版本不存在',404)
    return _ledger_view(r)

def ledgers():
    with connection() as db:rows=db.execute('SELECT id,version,decision,created_at,reviewed_at FROM retention_ledgers ORDER BY id DESC LIMIT 100').fetchall()
    return [dict(r) for r in rows]

def _ledger_view(row):
    body=json.loads(row['body'])
    body.update(ledger_id=row['id'],version=row['version'],decision=row['decision'],
                created_at=row['created_at'],reviewed_at=row['reviewed_at'])
    return body

def commit_ledger(policy, source_key, body, updates, snapshots=None, failpoint=None):
    """单事务落账：写入压缩记录与保留账，任一步失败整体回滚到旧版。"""
    db=connection()
    db.isolation_level=None
    try:
        db.execute('BEGIN IMMEDIATE')
        existing=db.execute('SELECT * FROM retention_ledgers WHERE policy=? AND source_key=?',(policy,source_key)).fetchone()
        if existing:
            db.rollback()
            return _ledger_view(existing)
        for run_id,old_body in (snapshots or {}).items():
            before=db.execute('SELECT body FROM runs WHERE id=?',(run_id,)).fetchone()
            if before is None:raise RuntimeError('压缩目标批次不存在：'+run_id)
            if json.loads(before['body'])!=old_body:raise RuntimeError('压缩目标已被改写：'+run_id)
        for run_id,new_body in updates.items():
            cur=db.execute('UPDATE runs SET body=? WHERE id=?',(json.dumps(new_body,ensure_ascii=False),run_id))
            if cur.rowcount!=1:raise RuntimeError('压缩目标批次不存在：'+run_id)
        if callable(failpoint):failpoint(db)
        number=db.execute('SELECT COALESCE(MAX(id),0)+1 FROM retention_ledgers').fetchone()[0]
        body=dict(body,ledger_id=number,version=1,decision='待复核',created_at=_now(),reviewed_at=None)
        db.execute('INSERT INTO retention_ledgers(id,policy,source_key,version,decision,body,created_at) VALUES (?,?,?,?,?,?,?)',
                   (number,policy,source_key,1,'待复核',json.dumps(body,ensure_ascii=False),_now()))
        db.commit()
        return body
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def review_ledger(ledger_id, version, decision):
    if decision not in ('通过','退回'):raise DomainError('DECISION','复核状态无效')
    db=connection()
    db.isolation_level=None
    try:
        db.execute('BEGIN IMMEDIATE')
        row=db.execute('SELECT * FROM retention_ledgers WHERE id=?',(ledger_id,)).fetchone()
        if not row:
            db.rollback();raise DomainError('NOT_FOUND','保留账版本不存在',404)
        if row['version']!=version:
            db.rollback();raise DomainError('STALE','保留账版本已更新，请刷新',409)
        body=json.loads(row['body'])
        body.update(ledger_id=row['id'],version=version+1,decision=decision,
                    created_at=row['created_at'],reviewed_at=_now())
        cur=db.execute('UPDATE retention_ledgers SET version=version+1,decision=?,body=?,reviewed_at=? WHERE id=? AND version=?',
                       (decision,json.dumps(body,ensure_ascii=False),_now(),ledger_id,version))
        if cur.rowcount!=1:
            db.rollback();raise DomainError('STALE','保留账版本已更新，请刷新',409)
        db.commit()
        return body
    except Exception:
        if db.in_transaction:db.rollback()
        raise
    finally:
        db.close()
