import json
from app.scrapers.goonet.models import CarRecord


def test_to_db_row_happy_path():
    rec = CarRecord(id='T1', name='TestCar', price=123, year=2020, rd=4567, engine=2000, repair='なし', location='Tokyo', raw={
        'k': 'v'
    }, color='red', mission='AT', bodytype='sedan', source='test', url='http://example.com')
    row = rec.to_db_row()
    # required keys
    for key in ('id', 'manufacturer', 'name', 'price', 'year', 'rd', 'engine', 'color', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'option', 'wd', 'seat', 'door', 'fuel', 'handle', 'jc08', 'category', 'source', 'url', 'raw_json'):
        assert key in row
    # check mission1 populated from mission
    assert row['mission1'] == 'AT'
    # raw_json is JSON string and contains 'k'
    raw = json.loads(row['raw_json'])
    assert raw['k'] == 'v'


def test_to_db_row_missing_mission_and_some_fields():
    rec = CarRecord(id='T2', name=None, price=None, year=None, rd=None, engine=None, repair=None, location=None, raw={}, color=None, mission=None, bodytype=None, source=None, url=None)
    row = rec.to_db_row()
    # required keys exist and are None where appropriate
    assert row['id'] == 'T2'
    assert row['name'] is None
    assert row['mission1'] is None
    # raw_json should be '{}'
    assert json.loads(row['raw_json']) == {}
