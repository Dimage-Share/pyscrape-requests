"""Entry point (no CLI args): runs scraper with interactive page prompt.

All former command line options removed per new requirement.
Behavior:
    1. Initialize logging (INFO level)
    2. Prompt user for last page number (q to quit)
    3. Remove previous summary*.md files
    4. Run scraping with default page_delay=1.0 and no extra params
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import List

from core import Scrape
from core.logger import Logger, setup_logging
from core.console import Console


DEFAULT_PAGE_DELAY = 1.0


class App:
    
    def __init__(self, page_delay: float = DEFAULT_PAGE_DELAY):
        self.page_delay = page_delay
    
    # --- interactive helpers ---
    def getPages(self) -> int:
        while True:
            pages = Console.input_int('scrape to [page]')
            if pages == 0: return 0
            if Console.confirm(f'Proceed?'): return pages
    
    def deleteSummaryFiles(self) -> None:
        for path in glob.glob('summary-*.md') + glob.glob('summary-all.md') + glob.glob('summary.md'):
            try:
                Path(path).unlink()
                Logger.debug(f"{path} deleted.")
            except Exception as e:  # noqa: BLE001
                Logger.warn(f"{path} delete failed with '{e}'")
    
    def run(self) -> int:
        setup_logging()  # default INFO
        pages = self.getPages()
        if pages <= 0:
            Logger.debug("Invalid page input, exit.")
            return 0
        self.deleteSummaryFiles()
        
        try:
            scraper = Scrape(page_delay=self.page_delay)
            scraper.run(pages, {})
        except Exception as e:  # noqa: BLE001
            Logger.exception(e)
            return 1
        return 0


def main() -> int:  # thin wrapper for legacy entrypoint
    return App().run()


if __name__ == "__main__":
    raise SystemExit(main())
