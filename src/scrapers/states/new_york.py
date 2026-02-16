"""New York AG scraper.

New York (ag.ny.gov) uses Drupal 10 with a paginated press release archive.
Structure is similar to California: .views-row items with time elements for dates.
"""

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper


@register_scraper("new_york")
class NewYorkScraper(BaseScraper):
    """Scraper for the New York Attorney General's press releases."""
    pass
