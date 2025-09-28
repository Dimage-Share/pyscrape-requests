from __future__ import annotations
"""Project-specific lightweight logger wrapper and logging setup utilities.

Merged components:
  - Lightweight Logger wrapper (static + bound instances)
  - ColorFormatter & setup_logging (previously in goo_net_scrape.logging_config)

Policy requirements:
  - Concise English messages
  - f-string style (callers pre-format strings)
  - Console: no timestamp
  - Provide static methods: info, debug, warn, error, fatal, exception
  - Timing helpers (time_block / measure)
"""
import logging
import time
import sys
import json
from pathlib import Path
from logging.config import dictConfig
from typing import Callable, Any, Optional, Dict

try:  # optional colorama
    from colorama import init as colorama_init, Fore, Style  # type: ignore
except Exception:  # noqa: BLE001
    
    class _Dummy:  # type: ignore
        RESET_ALL = ""
    
    class _Fore(_Dummy):
        RED = YELLOW = GREEN = CYAN = MAGENTA = BLUE = WHITE = ""
    
    class _Style(_Dummy):
        BRIGHT = DIM = NORMAL = ""
    
    def colorama_init(*_, **__):  # type: ignore
        return None
    
    Fore = _Fore()  # type: ignore
    Style = _Style()  # type: ignore

LEVEL_STYLE: Dict[int, Dict[str, str]] = {
    logging.DEBUG: {
        "color": Fore.CYAN,
        "style": Style.DIM
    },
    logging.INFO: {
        "color": Fore.GREEN,
        "style": Style.NORMAL
    },
    logging.WARNING: {
        "color": Fore.YELLOW,
        "style": Style.NORMAL
    },
    logging.ERROR: {
        "color": Fore.RED,
        "style": Style.BRIGHT
    },
    logging.CRITICAL: {
        "color": Fore.RED,
        "style": Style.BRIGHT
    },
}


class ColorFormatter(logging.Formatter):
    
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        base = super().format(record)
        spec = LEVEL_STYLE.get(record.levelno)
        if not spec:
            return base
        return f"{spec['style']}{spec['color']}{base}{Style.RESET_ALL}"


def _apply_inline(level: int):
    """Fallback color setup without timestamp (policy)."""
    colorama_init()
    handler = logging.StreamHandler(sys.stdout)
    fmt = "[ %(levelname)5s ] %(name)s : %(message)s"  # no asctime
    handler.setFormatter(ColorFormatter(fmt=fmt))
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.setLevel(level)


def setup_logging(level: int = logging.INFO):
    """Initialize logging.

    Priority:
      1. log.config.json at CWD or project root (dictConfig)
      2. Inline color fallback
    """
    cfg_path_candidates = [
        Path.cwd() / 'log.config.json',
        Path(__file__).resolve().parent.parent / 'log.config.json',
    ]
    for p in cfg_path_candidates:
        if p.is_file():
            try:
                with p.open('r', encoding='utf-8') as f:
                    data = json.load(f)
                dictConfig(data)
                logging.getLogger().setLevel(level)  # override root level by CLI
                logging.getLogger(__name__).debug(f"{p} loaded.")
                return
            except Exception as e:  # noqa: BLE001
                print(f"[logging] config load fail {p}: {e}", file=sys.stderr)
                continue
    _apply_inline(level)
    logging.getLogger(__name__).info("fallback inline logging config active")


_LOGGER = logging.getLogger


class Logger:
    """Thin wrapper supporting both static global usage and bound instances.

    Static style (existing):
        Logger.info("message")

    Module-aware style:
        log = Logger.bind(__name__)
        log.info("message")
    """
    
    def __init__(self, name: Optional[str] = None):
        self._name = name or __name__
    
    # --- instance methods ---
    def debug(self, msg: str) -> None:
        _LOGGER(self._name).debug(msg)
    
    def info(self, msg: str) -> None:
        _LOGGER(self._name).info(msg)
    
    def warn(self, msg: str) -> None:  # noqa: D401
        _LOGGER(self._name).warning(msg)
    
    def error(self, msg: str) -> None:
        _LOGGER(self._name).error(msg)
    
    def fatal(self, msg: str) -> None:
        _LOGGER(self._name).fatal(msg)
    
    def exception(self, msg: str) -> None:
        _LOGGER(self._name).exception(msg)
    
    # --- static convenience (backward compatibility) ---
    @staticmethod
    def bind(name: str) -> "Logger":
        return Logger(name)
    
    @staticmethod
    def debug(msg: str) -> None:  # type: ignore[override]
        _LOGGER(__name__).debug(msg)
    
    @staticmethod
    def info(msg: str) -> None:  # type: ignore[override]
        _LOGGER(__name__).info(msg)
    
    @staticmethod
    def warn(msg: str) -> None:  # type: ignore[override]
        _LOGGER(__name__).warning(msg)
    
    @staticmethod
    def error(msg: str) -> None:  # type: ignore[override]
        _LOGGER(__name__).error(msg)
    
    @staticmethod
    def fatal(msg: str) -> None:  # type: ignore[override]
        _LOGGER(__name__).fatal(msg)
    
    @staticmethod
    def exception(msg: str) -> None:  # type: ignore[override]
        _LOGGER(__name__).exception(msg)
    
    # --- helpers ---
    @staticmethod
    def time_block(label: str) -> Callable[[], None]:
        """Return a closure that logs elapsed ms when invoked.

        Usage:
            done = Logger.time_block("first page fetch")
            ... work ...
            done()
        """
        start = time.perf_counter()
        
        def _end() -> None:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            Logger.debug(f"{label} {elapsed_ms:.1f}ms")
        
        return _end
    
    @staticmethod
    def measure(label: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            Logger.debug(f"{label} {elapsed_ms:.1f}ms")


__all__ = ["Logger", "setup_logging", "ColorFormatter"]
