"""California AG scraper.

California (oag.ca.gov) uses a Drupal/Views CMS with a clean, paginated
press release archive. The base config-driven scraper handles this well,
so this module only registers the state and adds minor CA-specific overrides.
"""

from __future__ import annotations

import logging

from src.scrapers.base import BaseScraper
from src.scrapers.registry import register_scraper

logger = logging.getLogger(__name__)


@register_scraper("california")
class CaliforniaScraper(BaseScraper):
    """Scraper for the California Attorney General's press releases.

    The CA AG site is the gold standard — well-structured, paginated,
    server-rendered HTML with clean body text. The base scraper config
    handles the common case. This subclass exists for any CA-specific
    adjustments if needed.
    """

    def _parse_listing_row(self, node):
        """Override to handle CA-specific listing format if needed."""
        # The base implementation works well for CA's Drupal Views format.
        # Override only if we discover edge cases.
        return super()._parse_listing_row(node)
