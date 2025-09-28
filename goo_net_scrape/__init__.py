"""Legacy package providing backward compatibility.

The codebase has migrated to the `core` package. Importing from
`goo_net_scrape` will continue to work for a transition period.
Prefer updating imports to:

	from core import GooNetClient, parse_summary, parse_cars, Scrape
"""

from core import GooNetClient, parse_summary, parse_cars, Scrape  # type: ignore  # noqa: F401


__all__ = ["GooNetClient", "parse_summary", "parse_cars", "Scrape"]
