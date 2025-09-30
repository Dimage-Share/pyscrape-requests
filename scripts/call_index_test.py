import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask

from app.views import bp as main_bp


app = Flask(__name__)
app.register_blueprint(main_bp)

with app.test_client() as c:
    resp = c.get('/')
    print('GET / status:', resp.status_code)
    # print snippet of content length
    print('response length:', len(resp.data or b''))
