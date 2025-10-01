from goo_net_scrape.normalize import normalize_bodytype, normalize_record_fields


def test_normalize_bodytype_variant():
    nb, suspicious = normalize_bodytype('ミニバン・ワンボックス')
    assert nb == 'ミニバン'
    assert suspicious is False


def test_normalize_bodytype_mojibake():
    nb, suspicious = normalize_bodytype('�ܥǥXYZ')
    assert suspicious is True


def test_normalize_record_fields_sets_flag():
    rec = {
        'id': 'x1',
        'name': None,
        'price': None,
        'year': None,
        'rd': None,
        'engine': None,
        'color': None,
        'mission': None,
        'bodytype': '�FOOBAR',
        'repair': None,
        'location': None,
        'source': None,
        'url': None,
        'raw': {}
    }
    out = normalize_record_fields(rec)
    assert out['raw'].get('suspicious_bodytype') is True


def test_carsensor_bodytype_mapping():
    nb, suspicious = normalize_bodytype('SUV・クロスカントリー')
    assert nb == 'SUV'
    assert suspicious is False
