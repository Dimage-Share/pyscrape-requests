"""Legacy package providing backward compatibility.

The codebase has migrated to the `core` package. Importing from
`goo_net_scrape` will continue to work for a transition period.
Prefer updating imports to:

	from core import GooNetClient, parse_summary, parse_cars, Scrape
"""

__all__ = ["GooNetClient", "parse_summary", "parse_cars", "Scrape"]


def __getattr__(name: str):  # pragma: no cover - thin shim
    if name in __all__:
        # 局所 import で循環回避
        from goo_net_scrape.client import GooNetClient  # type: ignore
        from goo_net_scrape.parser import parse_summary  # type: ignore
        from goo_net_scrape.parser import parse_cars  # type: ignore
        from core.scrape import Scrape  # type: ignore
        mapping = {
            'GooNetClient': GooNetClient,
            'parse_summary': parse_summary,
            'parse_cars': parse_cars,
            'Scrape': Scrape,
        }
        return mapping[name]
    raise AttributeError(name)
