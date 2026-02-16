"""Ohio AG scraper.

Ohio (ohioattorneygeneral.gov) uses Kentico CMS with .ohio-news listing items.
"""

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper


@register_scraper("ohio")
class OhioScraper(BaseScraper):
    """Scraper for the Ohio Attorney General's press releases."""
    pass
