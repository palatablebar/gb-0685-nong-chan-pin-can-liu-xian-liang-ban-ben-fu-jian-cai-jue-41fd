import importlib,os
from flask import Flask,jsonify,request,redirect,Response
from app.config import MODULES,NAV
from app.errors import DomainError
from app.workflow import run,load
from app import store,retention
from app.web import payload,retention_page
from app.modules.m7_archive.export_out import bundle
from app.modules.m7_archive.ledger_export import bundle as ledger_bundle

def create_app():
    app=Flask(__name__);app.config['MAX_CONTENT_LENGTH']=2_000_000
    for entry in MODULES:app.register_blueprint(importlib.import_module('app.modules.'+entry[0]+'.handler').bp)
    @app.errorhandler(DomainError)
    def domain_error(error):return jsonify(code=error.code,message=error.message),error.status
    @app.errorhandler(404)
    def missing(error):return jsonify(code='NOT_FOUND',message='入口不存在'),404
    @app.get('/')
    def index():return redirect('/'+MODULES[0][0])
    @app.get('/healthz')
    def health():return jsonify(status='ok')
    @app.post('/api/runs')
    def submit():
        content,params=payload();state=run(content,params);return jsonify(store.save(state['run_id'],state))
    @app.get('/api/runs')
    def list_runs():return jsonify(store.history())
    @app.get('/api/runs/<run_id>')
    def get_run(run_id):return jsonify(store.get(run_id))
    @app.get('/api/runs/<run_id>/restore')
    def restore_run(run_id):return jsonify(retention.restore_run(store.views(),run_id))
    @app.post('/api/runs/<run_id>/review')
    def review(run_id):
        data=request.get_json(silent=True) or {}
        if not isinstance(data.get('version'),int):raise DomainError('VERSION','请提交复核版本')
        return jsonify(store.review(run_id,data['version'],data.get('decision')))
    @app.get('/api/archive/<run_id>/export.csv')
    def download(run_id):
        state=store.get(run_id)
        if not state.get('reconcile',{}).get('balanced'):raise DomainError('UNBALANCED','对账未平')
        ledger=store.current_ledger()
        if ledger is not None:
            release=retention.release_status(ledger,store.views())
            if not release.get('allowed'):raise DomainError('RETENTION',release.get('reason','保留账未放行'),409)
        return Response(bundle(state),mimetype='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename=evidence.csv'})
    @app.get('/m8_retention')
    def retention_screen():return retention_page()
    @app.get('/api/m8_retention')
    def retention_preview():return jsonify(retention.preview(store.views()))
    @app.post('/api/m8_retention')
    def retention_adjudicate():
        data=request.get_json(silent=True) or {}
        if not isinstance(data,dict):raise DomainError('PAYLOAD','请求必须是对象')
        try:return jsonify(retention.adjudicate(store.views()))
        except RuntimeError as error:raise DomainError('COMPACT','压缩失败，已恢复旧版：'+str(error),500)
    @app.post('/api/m8_retention/<int:ledger_id>/review')
    def retention_review(ledger_id):
        data=request.get_json(silent=True) or {}
        if not isinstance(data.get('version'),int):raise DomainError('VERSION','请提交复核版本')
        return jsonify(store.review_ledger(ledger_id,data['version'],data.get('decision')))
    @app.get('/api/m8_retention/<int:ledger_id>/export.csv')
    def retention_download(ledger_id):
        record=store.ledger(ledger_id)
        return Response(ledger_bundle(record),mimetype='text/csv; charset=utf-8',
                        headers={'Content-Disposition':'attachment; filename=retention-ledger.csv'})
    with app.app_context():
        if not store.history():
            state=run(load('dirty.csv'),{});store.save(state['run_id'],state)
    return app

app=create_app()
if __name__=='__main__':app.run(host='0.0.0.0',port=8000)
