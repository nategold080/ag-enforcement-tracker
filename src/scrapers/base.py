"""Abstract base scraper with config-driven operation.

The base scraper handles:
- HTTP requests with rate limiting, retries, and exponential backoff
- Pagination (query_param, next_link, none)
- CSS selector-based extraction from listing pages
- Detail page fetching
- Date filtering (--since)

Subclasses only need to override methods when the AG website requires
non-standard handling (JS rendering, unusual pagination, etc.).
"""

from __future__ import annotations

import asyncio
import logging
import re
from abc import ABC
from datetime import date, datetime
from pathlib import Path
from typing import AsyncIterator, Optional
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

import dateparser
import httpx
from selectolax.parser import HTMLParser

from src.validation.schemas import PressRelease, PressReleaseListItem

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "AGEnforcementTracker/1.0 (research project; contact@example.com)"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
MAX_RETRIES = 3
BASE_TIMEOUT = 30.0


class BaseScraper(ABC):
    """Config-driven scraper that handles common AG website patterns.

    Most states can be scraped using only the YAML config.  Override methods
    for sites that deviate from the standard listing → detail page pattern.
    """

    def __init__(self, config: dict):
        self.config = config
        self.state_name: str = config["name"]
        self.state_code: str = config["code"]
        self.base_url: str = config["base_url"]
        self.press_release_url: str = config["press_release_url"]
        self.pagination: dict = config.get("pagination", {"type": "none"})
        self.selectors: dict = config.get("selectors", {})
        self.link_attribute: str = config.get("link_attribute", "href")
        self.date_format: str | None = config.get("date_format")
        self.rate_limit: float = config.get("rate_limit_seconds", 2.0)
        self.use_browser_ua: bool = config.get("use_browser_ua", False)

        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            ua = BROWSER_USER_AGENT if self.use_browser_ua else DEFAULT_USER_AGENT
            self._client = httpx.AsyncClient(
                headers={"User-Agent": ua},
                timeout=httpx.Timeout(BASE_TIMEOUT),
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def fetch(self, url: str) -> str:
        """Fetch a URL with retries and rate limiting. Returns HTML text."""
        client = await self._get_client()
        last_exc: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(self.rate_limit)
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "[%s] Fetch attempt %d/%d failed for %s: %s. Retrying in %ds.",
                    self.state_code, attempt, MAX_RETRIES, url, exc, wait,
                )
                await asyncio.sleep(wait)

        logger.error("[%s] All %d attempts failed for %s", self.state_code, MAX_RETRIES, url)
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def _build_page_url(self, page_num: int) -> str:
        """Build the URL for a given page number based on pagination config."""
        ptype = self.pagination.get("type", "none")
        if ptype == "none":
            return self.press_release_url

        if ptype == "query_param":
            param = self.pagination["param"]
            parsed = urlparse(self.press_release_url)
            qs = parse_qs(parsed.query)
            qs[param] = [str(page_num)]
            new_query = urlencode(qs, doseq=True)
            return urlunparse(parsed._replace(query=new_query))

        raise ValueError(f"Unknown pagination type: {ptype}")

    # ------------------------------------------------------------------
    # Listing page parsing
    # ------------------------------------------------------------------

    def _parse_listing_page(self, html: str) -> list[PressReleaseListItem]:
        """Parse a listing page and return press release list items."""
        tree = HTMLParser(html)
        items: list[PressReleaseListItem] = []

        listing_sel = self.selectors.get("listing_item", ".views-row")
        rows = tree.css(listing_sel)

        for row in rows:
            try:
                item = self._parse_listing_row(row)
                if item:
                    items.append(item)
            except Exception:
                logger.debug("[%s] Failed to parse listing row", self.state_code, exc_info=True)

        return items

    def _parse_listing_row(self, node) -> PressReleaseListItem | None:
        """Parse a single row from the listing page."""
        # Title
        title_sel = self.selectors.get("title", "h3 a")
        title_node = node.css_first(title_sel)
        if not title_node:
            return None
        title = title_node.text(strip=True)

        # Link
        link_sel = self.selectors.get("link", "h3 a")
        link_node = node.css_first(link_sel)
        if not link_node:
            return None
        href = link_node.attributes.get(self.link_attribute, "")
        if not href:
            return None
        url = urljoin(self.base_url, href)

        # Date
        pr_date = None
        date_sel = self.selectors.get("date")
        if date_sel:
            date_node = node.css_first(date_sel)
            if date_node:
                date_text = date_node.text(strip=True)
                pr_date = self._parse_date(date_text)

        return PressReleaseListItem(
            title=title,
            url=url,
            date=pr_date,
            state=self.state_code,
        )

    def _parse_date(self, text: str) -> date | None:
        """Parse a date string using the configured format, falling back to dateparser."""
        if not text:
            return None
        text = text.strip()

        # Try configured format first
        if self.date_format:
            try:
                return datetime.strptime(text, self.date_format).date()
            except ValueError:
                pass

        # Fallback to dateparser
        parsed = dateparser.parse(text)
        if parsed:
            return parsed.date()

        logger.debug("[%s] Could not parse date: %r", self.state_code, text)
        return None

    # ------------------------------------------------------------------
    # Detail page parsing
    # ------------------------------------------------------------------

    def _parse_detail_page(self, html: str, list_item: PressReleaseListItem) -> PressRelease:
        """Parse a detail page to extract the full press release body."""
        tree = HTMLParser(html)

        body_sel = self.selectors.get("body", "article")
        body_node = tree.css_first(body_sel)

        body_html = ""
        body_text = ""
        if body_node:
            body_html = body_node.html or ""
            body_text = body_node.text(separator="\n", strip=True)
        else:
            # Fallback: try to get the main content area
            for fallback_sel in ["article", "main", ".content", "#content"]:
                node = tree.css_first(fallback_sel)
                if node:
                    body_html = node.html or ""
                    body_text = node.text(separator="\n", strip=True)
                    break

        # If listing page didn't provide a date, try to extract from detail page
        pr_date = list_item.date
        if not pr_date:
            pr_date = self._extract_date_from_detail(tree, body_text)

        return PressRelease(
            title=list_item.title,
            url=list_item.url,
            date=pr_date,
            state=self.state_code,
            body_html=body_html,
            body_text=body_text,
        )

    def _extract_date_from_detail(self, tree: HTMLParser, body_text: str) -> date | None:
        """Try to extract a publication date from a detail page.

        Checks in order:
        1. <time> element with datetime attribute
        2. <meta property="article:published_time">
        3. Configured date selector on detail page
        4. Date pattern in first 200 chars of body text (e.g., "February 11, 2026 | Press Release")
        """
        # 1. <time> element with datetime attribute
        time_node = tree.css_first("time[datetime]")
        if time_node:
            dt_attr = time_node.attributes.get("datetime", "")
            if dt_attr:
                try:
                    return datetime.fromisoformat(dt_attr.replace("Z", "+00:00")).date()
                except ValueError:
                    pass
            # Fall back to text content
            text = time_node.text(strip=True)
            parsed = self._parse_date(text)
            if parsed:
                return parsed

        # 2. <meta property="article:published_time">
        meta = tree.css_first('meta[property="article:published_time"]')
        if meta:
            content = meta.attributes.get("content", "")
            if content:
                try:
                    return datetime.fromisoformat(content.replace("Z", "+00:00")).date()
                except ValueError:
                    pass

        # 3. Configured date selector
        date_sel = self.selectors.get("date")
        if date_sel:
            date_node = tree.css_first(date_sel)
            if date_node:
                parsed = self._parse_date(date_node.text(strip=True))
                if parsed:
                    return parsed

        # 4. Date pattern in first 200 chars of body text
        #    Common pattern: "February 11, 2026 | Press Release"
        match = re.search(
            r'((?:January|February|March|April|May|June|July|August|September|'
            r'October|November|December)\s+\d{1,2},?\s+\d{4})',
            body_text[:300],
        )
        if match:
            parsed = self._parse_date(match.group(1))
            if parsed:
                return parsed

        return None

    # ------------------------------------------------------------------
    # Main scraping entrypoint
    # ------------------------------------------------------------------

    async def scrape_listing(
        self,
        since: date | None = None,
        max_pages: int = 100,
    ) -> list[PressReleaseListItem]:
        """Scrape the listing pages and return all press release items.

        Args:
            since: Only include press releases on or after this date.
            max_pages: Safety limit on number of pages to scrape.
        """
        all_items: list[PressReleaseListItem] = []
        start = self.pagination.get("start", 0)
        ptype = self.pagination.get("type", "none")

        for page_num in range(start, start + max_pages):
            url = self._build_page_url(page_num)
            logger.info("[%s] Scraping listing page %d: %s", self.state_code, page_num, url)

            try:
                html = await self.fetch(url)
            except Exception:
                logger.error("[%s] Failed to fetch listing page %d", self.state_code, page_num, exc_info=True)
                break

            items = self._parse_listing_page(html)
            if not items:
                logger.info("[%s] No items found on page %d, stopping pagination.", self.state_code, page_num)
                break

            # Date filtering
            if since:
                filtered = []
                hit_old_items = False
                for item in items:
                    if item.date and item.date < since:
                        hit_old_items = True
                        continue
                    filtered.append(item)
                items = filtered
                if hit_old_items and not items:
                    logger.info("[%s] All items on page %d are before %s, stopping.", self.state_code, page_num, since)
                    break

            all_items.extend(items)

            if ptype == "none":
                break

        logger.info("[%s] Found %d press release listing items.", self.state_code, len(all_items))
        return all_items

    async def scrape_detail(self, item: PressReleaseListItem) -> PressRelease:
        """Fetch and parse a single press release detail page."""
        logger.info("[%s] Fetching detail page: %s", self.state_code, item.url)
        html = await self.fetch(item.url)
        return self._parse_detail_page(html, item)

    async def scrape(
        self,
        since: date | None = None,
        max_pages: int = 100,
    ) -> list[PressRelease]:
        """Full scrape: listing pages → detail pages for each item.

        Returns fully hydrated PressRelease objects with body text.
        """
        items = await self.scrape_listing(since=since, max_pages=max_pages)
        press_releases: list[PressRelease] = []
        errors = 0

        for item in items:
            try:
                pr = await self.scrape_detail(item)
                press_releases.append(pr)
            except Exception:
                errors += 1
                logger.error(
                    "[%s] Failed to scrape detail for %s",
                    self.state_code, item.url, exc_info=True,
                )

        logger.info(
            "[%s] Scraped %d press releases (%d errors).",
            self.state_code, len(press_releases), errors,
        )
        return press_releases

    # ------------------------------------------------------------------
    # Fixture saving (for tests)
    # ------------------------------------------------------------------

    async def save_fixtures(
        self,
        output_dir: Path,
        since: date | None = None,
        max_items: int = 25,
    ) -> list[Path]:
        """Save raw HTML pages as test fixtures.

        Saves both the listing page(s) and individual detail pages.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        saved: list[Path] = []

        # Save first listing page
        listing_html = await self.fetch(self._build_page_url(self.pagination.get("start", 0)))
        listing_path = output_dir / "listing_page_0.html"
        listing_path.write_text(listing_html, encoding="utf-8")
        saved.append(listing_path)

        items = self._parse_listing_page(listing_html)
        if since:
            items = [i for i in items if not i.date or i.date >= since]
        items = items[:max_items]

        for i, item in enumerate(items):
            try:
                html = await self.fetch(item.url)
                # Create a safe filename from the URL
                slug = re.sub(r"[^\w\-]", "_", urlparse(item.url).path.strip("/"))[:80]
                path = output_dir / f"detail_{i:03d}_{slug}.html"
                path.write_text(html, encoding="utf-8")
                saved.append(path)
                logger.info("[%s] Saved fixture: %s", self.state_code, path.name)
            except Exception:
                logger.error("[%s] Failed to save fixture for %s", self.state_code, item.url, exc_info=True)

        return saved
