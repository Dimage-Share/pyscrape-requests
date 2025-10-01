"""Core scraping package (post-migration).

This package intentionally avoids importing submodules at package-import
time to prevent circular import problems in utility scripts. Import
submodules explicitly (for example ``from core.scrape import Scrape``)
when you need them.
"""

__all__ = ["Scrape", "GooNetClient", "parse_summary", "parse_cars"]
