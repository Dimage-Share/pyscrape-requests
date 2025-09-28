"""Compatibility shim for moved logging setup.

Deprecated: Import from core.logger instead.

Existing third-party / legacy code that does::
    from goo_net_scrape.logging_config import setup_logging

will continue to function. ColorFormatter re-exported for dictConfig compatibility.
"""
from core.logger import setup_logging, ColorFormatter  # noqa: F401


__all__ = ["setup_logging", "ColorFormatter"]
