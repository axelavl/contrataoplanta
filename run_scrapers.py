"""Wrapper de compatibilidad para el nuevo entrypoint scrapers.run_all."""

from __future__ import annotations

import asyncio
import sys

from scrapers.run_all import main


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
