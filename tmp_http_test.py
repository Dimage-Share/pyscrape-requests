import requests

try:
    r = requests.get('http://127.0.0.1:5000/', timeout=5)
    print('status:', r.status_code)
    txt = r.text or ''
    print('len:', len(txt))
    print('head:', txt[:400].replace('\n', '\\n'))
except Exception as e:
    print('error:', type(e).__name__, e)
