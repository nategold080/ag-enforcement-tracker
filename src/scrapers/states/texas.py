"""Texas AG scraper.

Texas (texasattorneygeneral.gov) uses Drupal 10 with a custom press release listing.
The listing uses div.m-b-3 items with h4 a links. No dates on the listing page.
Text contains soft hyphens (\\xad) that must be stripped.
"""

from __future__ import annotations

from selectolax.parser import HTMLParser

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper
from src.validation.schemas import PressReleaseListItem


@register_scraper("texas")
class TexasScraper(BaseScraper):
    """Scraper for the Texas Attorney General's press releases."""

    def _parse_listing_page(self, html: str) -> list[PressReleaseListItem]:
        """Override to handle TX's Drupal listing and strip soft hyphens."""
        tree = HTMLParser(html)
        items: list[PressReleaseListItem] = []

        for row in tree.css("div.m-b-3"):
            link = row.css_first("h4 a")
            if not link:
                continue

            href = link.attributes.get("href", "")
            if not href or "/news/releases/" not in href:
                continue

            # Strip soft hyphens from title text
            title = link.text(strip=True).replace("\xad", "")

            from urllib.parse import urljoin
            url = urljoin(self.base_url, href)

            items.append(PressReleaseListItem(
                title=title,
                url=url,
                date=None,  # No dates on TX listing page
                state=self.state_code,
            ))

        return items
