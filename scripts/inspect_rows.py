import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Import the search module and monkeypatch its render_template before registering blueprint
import app.views.search as search
from flask import Flask


captured = {}


def fake_render_template(template_name, **kwargs):
    rows = kwargs.get('rows')
    print('template:', template_name)
    if rows is None:
        print('no rows')
    else:
        print('rows count:', len(rows))
        for i, r in enumerate(rows[:10]):
            # try attribute and mapping access
            val = None
            try:
                val = r.get('price') if isinstance(r, dict) else getattr(r, 'price', None)
            except Exception as e:
                val = f'error: {e}'
            print(f'row[{i}] price type={type(val).__name__} repr={repr(val)}')
    # return minimal HTML
    return '<html>OK</html>'


# patch module-level render_template used by the view
search.render_template = fake_render_template

# Now register blueprint and call
app = Flask(__name__)
from app.views import bp as main_bp


app.register_blueprint(main_bp)

with app.test_client() as c:
    resp = c.get('/')
    print('GET status:', resp.status_code)
    print('resp data len:', len(resp.data))
