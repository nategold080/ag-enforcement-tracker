"""Virginia AG scraper.

Virginia (oag.state.va.us) uses Joomla with a table-based press release listing.
Custom scraper needed because:
- Listing is in a Joomla category table
- Need to filter out header rows and non-content rows
- Dates are extracted from the detail page or press release text
"""

from __future__ import annotations

import re
from datetime import date

from selectolax.parser import HTMLParser

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper
from src.validation.schemas import PressReleaseListItem


@register_scraper("virginia")
class VirginiaScraper(BaseScraper):
    """Scraper for the Virginia Attorney General's press releases."""

    def _parse_listing_page(self, html: str) -> list[PressReleaseListItem]:
        """Override to handle Joomla's table-based listing."""
        tree = HTMLParser(html)
        items: list[PressReleaseListItem] = []

        # Find all links within the category list table
        table = tree.css_first(".category-list") or tree.css_first("table.category")
        if not table:
            # Fallback: find all links in the main content area
            table = tree.css_first("#content") or tree.css_first("main") or tree

        for link in table.css("a"):
            href = link.attributes.get("href", "")
            title = link.text(strip=True)

            # Filter out non-press-release links
            if not href or not title:
                continue
            if len(title) < 15:  # Skip short nav links
                continue
            if "/media-center/news-releases/" not in href:
                continue
            if title.lower() in ("title", "date", "hits", "news releases"):
                continue

            from urllib.parse import urljoin
            url = urljoin(self.base_url, href)

            items.append(PressReleaseListItem(
                title=title,
                url=url,
                date=None,  # Dates parsed from detail page
                state=self.state_code,
            ))

        return items
