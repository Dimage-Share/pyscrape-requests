from app.scrapers.goonet import parser
from app.scrapers.goonet import models
from app.db import init_db, bulk_upsert_cars, get_connection


SAMPLE_HTML = '''<html><body>
<div id="tr_12345">
  <div id="td_12345">
    <div>
      <h3><a href="/car/12345">TestCar</a></h3>
    </div>
  </div>
  <div>
    <div>
      <div>
        <div>
          <div>
            <p>Price: <em>288.4万</em></p>
          </div>
        </div>
      </div>
      <div>
        <div>
          <div>
            <ul>
              <li>2020年</li>
              <li>3.4万km</li>
              <li>---</li>
              <li>2.0L</li>
              <li>修復歴なし</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
</body></html>'''


def main():
    init_db()
    recs = parser.parse_cars(SAMPLE_HTML)
    print('parsed', len(recs))
    n = bulk_upsert_cars(recs)
    print('written', n)
    conn = get_connection()
    cur = conn.execute('SELECT id,name,price,year,rd,engine,repair,url FROM car')
    rows = cur.fetchall()
    print('rows in car:', len(rows))
    for r in rows:
        print(r)
    conn.close()


if __name__ == '__main__':
    main()
