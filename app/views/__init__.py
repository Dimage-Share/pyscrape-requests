from __future__ import annotations
from flask import Blueprint


bp = Blueprint('main', __name__)

# import and register submodules
from . import search as _search  # noqa: E402
from . import pivot as _pivot  # noqa: E402
from . import admin as _admin  # noqa: E402


_search.register(bp)
_pivot.register(bp)
_admin.register(bp)

__all__ = ['bp']
