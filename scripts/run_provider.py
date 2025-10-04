from __future__ import annotations
"""Generic provider run script.

Usage (PowerShell):
  python .\scripts\run_provider.py --provider goonet --pages 5
  python .\scripts\run_provider.py --provider carsensor --pages 3
"""
import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from providers.base import provider_command, available_providers  # type: ignore
import json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--provider', required=True, help='Provider key (one of: ' + ','.join(available_providers()) + ')')
    ap.add_argument('--pages', type=int, default=1)
    ap.add_argument('--dump', action='store_true', help='Write simple dump artifact')
    ap.add_argument('--dump-dir', type=str, default='dumps_provider')
    args = ap.parse_args()
    count = provider_command(args.provider, pages=args.pages, dump=args.dump, dump_dir=args.dump_dir)
    print(json.dumps({
        'provider': args.provider,
        'pages': args.pages,
        'records': count
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
