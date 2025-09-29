from pathlib import Path
import importlib.util
import types
import sys
import sqlite3


repo = Path.cwd()
core_dir = repo / 'core'
app_dir = repo / 'app'

# Create minimal package entries to avoid executing package __init__ files
core_pkg = types.ModuleType('core')
core_pkg.__path__ = [str(core_dir)]
sys.modules['core'] = core_pkg
app_pkg = types.ModuleType('app')
app_pkg.__path__ = [str(app_dir)]
sys.modules['app'] = app_pkg


def load_mod(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Preload core support modules used by scrapers to avoid triggering core.__init__
load_mod('core.models', core_dir / 'models.py')
load_mod('core.logger', core_dir / 'logger.py')
# core.client is used by Scrape (CarSensorClient)
load_mod('core.client', core_dir / 'client.py')

# Now load core.scrape and run
scrape_mod = load_mod('core.scrape', core_dir / 'scrape.py')
Scrape = getattr(scrape_mod, 'Scrape')
print('Starting Scrape.run(2, {})...')
s = Scrape()
try:
    out_path = s.run(2, {})
    print('SCRAPE_DONE', out_path)
except Exception as e:
    print('SCRAPE_ERROR', e)

# Report DB counts and sample rows
db_path = repo / 'database.db'
print('DB path:', db_path)
if not db_path.exists():
    print('DB not found')
else:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    def count_table(name):
        try:
            cur.execute(f"select count(1) from {name}")
            return cur.fetchone()[0]
        except Exception as e:
            return f"ERR:{e}"
    
    print('goo_count', count_table('goo'))
    print('car_count', count_table('car'))
    try:
        cur.execute('select id,name,price,year,url from car limit 5')
        for r in cur.fetchall():
            print('CAR_SAMPLE', r)
    except Exception as e:
        print('CAR_SAMPLE_ERR', e)
    conn.close()
