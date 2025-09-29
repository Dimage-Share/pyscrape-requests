from app.scrapers.goonet import parser

# Minimal synthetic HTML with a tr_ element resembling goo-net structure
html = '''
<div id="tr_12345">
  <div>
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
</div>
'''

records = parser.parse_cars(html)
print('parsed', len(records))
for r in records:
    print(r)
