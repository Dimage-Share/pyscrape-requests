"""Quick MySQL sanity test script for workspace.

Runs: import app.db.mysql, init_db(), insert a test CarRecord into goo and car,
then queries counts. Exits with non-zero code on exception.
"""
import sys
from importlib import import_module
try:
    mysql = import_module('app.db.mysql')
    print('imported app.db.mysql')
    mysql.init_db()
    print('init_db done')
    from core.models import CarRecord
    rec = CarRecord(id='SANITY_TEST_1', manufacturer='テスト社', name='テスト車', price=12345, year=2020, rd=1, engine=660, color='白', mission1='AT', mission2=None, bodytype='ハッチバック', repair=None, location='東京', option=None, wd=None, seat=None, door=None, fuel=None, handle=None, jc08=None, category='テスト', source='unittest', url='http://example.local/test', raw={})
    mysql.truncate_goo()
    n = mysql.bulk_insert_goo([rec])
    print('bulk_insert_goo inserted', n)
    mysql.upsert_car(rec)
    print('upsert_car done')
    # Query back via pymysql to ensure rows exist
    import pymysql
    conn = pymysql.connect(host='127.0.0.1', port=33062, user='pyscrape', password='pyscrape_pwd', database='pyscrape', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM goo")
        print('goo count:', cur.fetchone())
        cur.execute("SELECT COUNT(*) AS cnt FROM car WHERE id=%s", ('SANITY_TEST_1', ))
        print('car SANITY_TEST_1 count:', cur.fetchone())
    conn.close()
except Exception as e:
    print('ERROR', type(e).__name__, e)
    sys.exit(2)
print('SANITY OK')
