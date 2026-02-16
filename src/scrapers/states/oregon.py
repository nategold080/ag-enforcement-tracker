"""Oregon DOJ scraper.

Oregon (doj.state.or.us) uses WordPress with a custom news listing page.
The .results-item elements contain links to press releases.
"""

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper


@register_scraper("oregon")
class OregonScraper(BaseScraper):
    """Scraper for the Oregon Department of Justice's press releases."""
    pass
