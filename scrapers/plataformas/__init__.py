"""Scrapers genericos y adaptadores por plataforma."""

from .ffaa import FfaaScraper
from .generic_site import GenericSiteScraper
from .hiringroom import HiringRoomScraper
from .pdi import PdiScraper
from .playwright_scraper import PlaywrightScraper
from .wordpress import WordPressScraper

__all__ = [
    "FfaaScraper",
    "GenericSiteScraper",
    "HiringRoomScraper",
    "PdiScraper",
    "PlaywrightScraper",
    "WordPressScraper",
]
