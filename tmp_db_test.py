from importlib import import_module
try:
    mod = import_module('app.db')
    print('import ok', [k for k in dir(mod) if not k.startswith('_')][:40])
    import app.db as db
    db.init_db()
    print('init_db ok')
except Exception as e:
    print('ERROR', e)
    raise
