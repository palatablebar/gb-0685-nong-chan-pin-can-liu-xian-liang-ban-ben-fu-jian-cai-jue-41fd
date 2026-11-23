from flask import request,render_template,jsonify
from app.errors import DomainError
from app.config import TITLE,NAV,SHORT
from app.workflow import run,load
from app import store,retention

def payload():
    data=request.get_json(silent=True) or {}
    if not isinstance(data,dict):raise DomainError('PAYLOAD','请求必须是对象')
    if 'csv' in data and not isinstance(data['csv'],str):raise DomainError('CSV','CSV内容必须是文本')
    sample=data.get('sample',request.args.get('sample','dirty.csv'))
    content=data.get('csv')
    return (content.encode() if isinstance(content,str) else load(sample)),data.get('parameters',{})

def api(stage):
    content,parameters=payload();return jsonify(run(content,parameters,stage))

def page(stage,template):
    latest=store.history();state=store.get(latest[0]['id']) if latest else run(load('dirty.csv'),{})
    if 'sample' in request.args:source=load(request.args['sample'])
    else:source=retention.source_of(state) or load('dirty.csv')
    data=run(source,state.get('manifest',{}).get('parameters',{}),stage)
    ledger=store.current_ledger()
    return render_template(template,title=TITLE,short=SHORT,modules=NAV,current=stage,data=data,
        run=state,history=latest,ledger=ledger,ledger_history=store.ledgers(),
        release=retention.release_status(ledger,store.views()))

def retention_page():
    stored=store.current_ledger()
    views=store.views()
    release=retention.release_status(stored,views)
    stale=bool(stored) and release.get('status')=='保留账已过期'
    preview=retention.preview(views)
    ledger=stored or preview
    needs=stored is None or stale
    data={'run_id':ledger.get('ledger_id') if ledger.get('ledger_id') is not None else 'preview',
          'manifest':{'rule_version':ledger.get('rule_version'),
                      'generated_at':ledger.get('generated_at') or ''},
          'input_count':ledger.get('totals',{}).get('records',0),'rows':[],'quarantine':[],
          'business':None}
    return render_template('m8_retention.html',title=TITLE,short=SHORT,modules=NAV,current=8,data=data,
        run={'decision':ledger.get('decision') or '预览','review_version':ledger.get('version') or 0},
        history=store.history(),ledger=ledger,ledger_history=store.ledgers(),release=release,
        needs_adjudication=needs,stale=stale,preview=preview)
