"""Pennsylvania AG scraper.

Pennsylvania (attorneygeneral.gov) uses WordPress with a custom 'Taking Action' post type.
Listing page has a unique card layout that needs custom parsing.
"""

from __future__ import annotations

import re
from datetime import date, datetime

from selectolax.parser import HTMLParser

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper
from src.validation.schemas import PressReleaseListItem


@register_scraper("pennsylvania")
class PennsylvaniaScraper(BaseScraper):
    """Scraper for the Pennsylvania Attorney General's press releases."""

    def _parse_listing_page(self, html: str) -> list[PressReleaseListItem]:
        """Override to handle PA's Taking Action card layout."""
        tree = HTMLParser(html)
        items: list[PressReleaseListItem] = []
        seen_urls: set[str] = set()

        # PA uses links with date text followed by title links
        # Pattern: date link → title link pairs pointing to same URL
        for link in tree.css("a"):
            href = link.attributes.get("href", "")
            text = link.text(strip=True)

            if not href or not text:
                continue
            if "/taking-action/" not in href:
                continue
            if href == "https://www.attorneygeneral.gov/taking-action/":
                continue
            # Skip pagination links like /taking-action/page/2/
            if re.search(r"/taking-action/page/\d+/?$", href):
                continue
            if href in seen_urls:
                continue

            # Try to detect if this is a date-only link (MM/DD/YYYY)
            date_match = re.match(r"^(\d{1,2}/\d{1,2}/\d{4})$", text)
            if date_match:
                continue  # Skip date-only links, we'll get the title link

            # This should be a title link
            from urllib.parse import urljoin
            url = urljoin(self.base_url, href)
            seen_urls.add(href)

            # Try to find the date for this item
            pr_date = self._find_date_for_url(tree, href)

            items.append(PressReleaseListItem(
                title=text,
                url=url,
                date=pr_date,
                state=self.state_code,
            ))

        return items

    def _find_date_for_url(self, tree: HTMLParser, href: str) -> date | None:
        """Find the date link that matches the same URL."""
        for link in tree.css("a"):
            if link.attributes.get("href", "") == href:
                text = link.text(strip=True)
                date_match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text)
                if date_match:
                    try:
                        return datetime.strptime(text, "%m/%d/%Y").date()
                    except ValueError:
                        continue
        return None
