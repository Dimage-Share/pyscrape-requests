"""Core scraping package (post-migration).

Public entrypoints:
  - Scrape: high-level orchestrator
  - GooNetClient, parse_summary, parse_cars

Legacy compatibility: external code should migrate imports from
`goo_net_scrape.*` to `core.*`. A thin compatibility layer will remain
until final removal.
"""

from .scrape import Scrape  # noqa: F401
from .client import GooNetClient  # noqa: F401
from .parser import parse_summary, parse_cars  # noqa: F401


__all__ = ["Scrape", "GooNetClient", "parse_summary", "parse_cars"]
