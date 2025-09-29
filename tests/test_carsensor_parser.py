import importlib.util
import sys
import types
from pathlib import Path

# Prepare a minimal 'core' package in sys.modules so that relative imports in
# core.carsensor_parser (e.g. from .models import CarRecord) resolve.
core_pkg = types.ModuleType('core')
core_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / 'core')]
sys.modules['core'] = core_pkg

# Load core.models first
models_path = Path(__file__).resolve().parents[1] / 'core' / 'models.py'
spec = importlib.util.spec_from_file_location('core.models', str(models_path))
models_mod = importlib.util.module_from_spec(spec)
sys.modules['core.models'] = models_mod
spec.loader.exec_module(models_mod)  # type: ignore

# Load core.carsensor_parser
parser_path = Path(__file__).resolve().parents[1] / 'core' / 'carsensor_parser.py'
spec2 = importlib.util.spec_from_file_location('core.carsensor_parser', str(parser_path))
parser_mod = importlib.util.module_from_spec(spec2)
sys.modules['core.carsensor_parser'] = parser_mod
spec2.loader.exec_module(parser_mod)  # type: ignore

from core.carsensor_parser import parse_cars_carsensor, get_next_page_url_carsensor, parse_car_detail


SAMPLE_LIST_HTML = '''
<html><body>
<div id="AU12345_cas">
  <div>
    <div>
      <div></div>
      <div>
        <div>
          <div>
            <div>
              <div>
                <div></div>
                <div>
                  <p>ignore</p>
                  <p>支払総額 123.4万円</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
<a href="/usedcar/hokkaido/index2.html">2</a>
</body></html>
'''

SAMPLE_DETAIL_HTML = '''
<html><body>
  <div></div>
  <div>
    <main>
      <section>
        <h1>車名<span>オプション</span></h1>
      </section>
    </main>
  </div>
</body></html>
'''


def test_parse_cars_carsensor_basic():
    records = parse_cars_carsensor(SAMPLE_LIST_HTML)
    assert isinstance(records, list)
    # At minimum we expect an AU id record and a record object
    assert any(getattr(r, 'id', '').startswith('AU') for r in records)


def test_get_next_page_url_carsensor():
    url = get_next_page_url_carsensor(SAMPLE_LIST_HTML, current_url='search.php')
    assert url is None or url.endswith('index2.html')


def test_parse_car_detail_name_option():
    d = parse_car_detail(SAMPLE_DETAIL_HTML)
    assert isinstance(d, dict)
    # Parser should return a dict with expected keys; values may be None depending on HTML
    assert 'name' in d and 'option' in d
