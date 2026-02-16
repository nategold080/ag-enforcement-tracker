"""Download press releases from Wayback Machine for states blocked by 403.

Uses the CDX API to find archived URLs, then fetches content from
web.archive.org/web/ snapshots.

Run: python scripts/wayback_scrape.py --states ny,pa --since 2022-01-01
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import click
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from selectolax.parser import HTMLParser
from src.storage.database import Database
from src.storage.models import EnforcementAction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("wayback")

# State config for Wayback Machine scraping
WAYBACK_STATES = {
    "ny": {
        "code": "NY",
        "name": "New York",
        "cdx_pattern": "ag.ny.gov/press-release/{year}/*",
        "years": range(2022, 2027),
        "body_selector": ".node__content",
        "title_from_url": True,  # NY URLs contain year/month/slug
        "date_pattern": r"/press-release/(\d{4})/(\d{1,2})/",
    },
    "pa": {
        "code": "PA",
        "name": "Pennsylvania",
        "cdx_pattern": "www.attorneygeneral.gov/taking-action/{slug}",
        "years": None,  # PA URLs don't have year in path
        "cdx_url_all": "www.attorneygeneral.gov/taking-action/*",
        "body_selector": ".entry-content",
        "exclude_patterns": [
            r"/taking-action/?$",
            r"/taking-action/page/\d+",
            r"/taking-action/?(\?|#)",
        ],
        "date_selector": ".entry-date, time, .posted-on",
    },
    "ct": {
        "code": "CT",
        "name": "Connecticut",
        "cdx_pattern": "portal.ct.gov/ag/press-releases/{year}-press-releases/*",
        "years": range(2022, 2027),
        "body_selector": ".content",
        "exclude_patterns": [
            r"/\d{4}-press-releases/?$",   # listing pages like /2024-Press-Releases
            r"/press-releases/?$",          # main listing page
            r"ag-press-release-template",
        ],
        # Minimum body length to reject empty/nav-only pages
        "min_body_length": 200,
    },
    "ma": {
        "code": "MA",
        "name": "Massachusetts",
        "cdx_pattern": "mass.gov/news/ag-*",
        "years": None,
        "body_selector": ".page-content",
        "exclude_patterns": [
            r"\?.*=",  # URLs with query params are duplicates
        ],
    },
    "il": {
        "code": "IL",
        "name": "Illinois",
        "cdx_pattern": "illinoisattorneygeneral.gov/pressroom/{year}_*",
        "years": range(2022, 2027),
        "body_selector": "td[bgcolor='#FFFFFF']",
        "exclude_patterns": [
            r"/index\.html$",
            r"/pressroom/?$",
        ],
    },
    "wa": {
        "code": "WA",
        "name": "Washington",
        "cdx_pattern": "atg.wa.gov/news/news-releases/*",
        "years": None,
        "body_selector": "#block-atg-content article",
        "exclude_patterns": [
            r"/NEWS/NEWS-RELEASES/?$",
            r"/news/news-releases/?$",
            r"/news/news-releases/\d{4}$",
        ],
    },
}


async def query_cdx(url_pattern: str, since: str = "20220101", limit: int = 5000) -> list[dict]:
    """Query CDX API and return list of {timestamp, url}."""
    cdx_url = "https://web.archive.org/cdx/search/cdx"
    params = [
        ("url", url_pattern),
        ("output", "text"),
        ("fl", "timestamp,original"),
        ("filter", "statuscode:200"),
        ("filter", "mimetype:text/html"),
        ("from", since),
        ("collapse", "urlkey"),
        ("limit", str(limit)),
    ]
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(cdx_url, params=params)
        r.raise_for_status()

    results = []
    for line in r.text.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.strip().split(" ", 1)
        if len(parts) == 2:
            results.append({"timestamp": parts[0], "url": parts[1]})
    return results


async def fetch_wayback_page(client: httpx.AsyncClient, timestamp: str, url: str) -> str | None:
    """Fetch a page from the Wayback Machine."""
    wayback_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
    try:
        r = await client.get(wayback_url)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", wayback_url, e)
        return None


def extract_title_from_html(html: str) -> str:
    """Extract title from HTML page."""
    tree = HTMLParser(html)
    # Try h1 first
    h1 = tree.css_first("h1")
    if h1:
        return h1.text(strip=True)
    # Try <title>
    title = tree.css_first("title")
    if title:
        text = title.text(strip=True)
        # Strip site name suffix
        for sep in [" | ", " - ", " — "]:
            if sep in text:
                text = text.split(sep)[0].strip()
        return text
    return ""


def extract_body_text(html: str, selector: str) -> str:
    """Extract body text using CSS selector with fallbacks."""
    tree = HTMLParser(html)
    node = tree.css_first(selector)
    if node:
        return node.text(separator="\n", strip=True)
    # Fallbacks
    for sel in ["article", "main", ".content", "#content", ".field--name-body"]:
        node = tree.css_first(sel)
        if node:
            return node.text(separator="\n", strip=True)
    return ""


def extract_date_ny(url: str) -> date | None:
    """Extract date from NY URL pattern /press-release/YYYY/MM/slug."""
    m = re.search(r"/press-release/(\d{4})/(\d{1,2})/", url)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            pass
    return None


def extract_date_pa(html: str) -> date | None:
    """Extract date from PA page HTML."""
    import dateparser
    tree = HTMLParser(html)
    for sel in [".entry-date", "time", ".posted-on", ".ta-card-date"]:
        node = tree.css_first(sel)
        if node:
            # Check datetime attribute first
            dt_attr = node.attributes.get("datetime", "")
            if dt_attr:
                try:
                    return datetime.fromisoformat(dt_attr.replace("Z", "+00:00")).date()
                except ValueError:
                    pass
            text = node.text(strip=True)
            if text:
                parsed = dateparser.parse(text)
                if parsed:
                    return parsed.date()
    return None


async def scrape_ny(db: Database, since: date) -> dict:
    """Scrape NY press releases from Wayback Machine."""
    config = WAYBACK_STATES["ny"]
    stats = {"state": "NY", "found": 0, "stored": 0, "skipped": 0, "errors": 0}

    # Get all archived detail page URLs
    all_urls = []
    for year in config["years"]:
        pattern = f"ag.ny.gov/press-release/{year}/*"
        results = await query_cdx(pattern, since=since.strftime("%Y%m%d"))
        all_urls.extend(results)
        logger.info("[NY] CDX year %d: %d URLs", year, len(results))

    stats["found"] = len(all_urls)
    logger.info("[NY] Total unique archived URLs: %d", len(all_urls))

    # Filter out already-scraped
    new_urls = []
    for r in all_urls:
        url = r["url"]
        if not url.startswith("https://"):
            url = "https://" + url if not url.startswith("http") else url
        if not db.action_exists(url):
            new_urls.append({**r, "url": url})
        else:
            stats["skipped"] += 1

    logger.info("[NY] New URLs to fetch: %d (skipped %d existing)", len(new_urls), stats["skipped"])

    # Fetch detail pages
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, entry in enumerate(new_urls):
            url = entry["url"]
            ts = entry["timestamp"]

            try:
                html = await fetch_wayback_page(client, ts, url)
                if not html:
                    stats["errors"] += 1
                    continue

                title = extract_title_from_html(html)
                body = extract_body_text(html, config["body_selector"])
                pr_date = extract_date_ny(url) or since

                if not title or not body:
                    stats["errors"] += 1
                    continue

                # Store
                if not db.action_exists(url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state="NY",
                            date_announced=pr_date,
                            action_type="other",
                            status="announced",
                            headline=title,
                            source_url=url,
                            raw_text=body[:10000],
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 50 == 0:
                    logger.info("[NY] Progress: %d/%d fetched, %d stored",
                                i + 1, len(new_urls), stats["stored"])

                # Rate limit for Wayback
                await asyncio.sleep(1.0)

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[NY] Error fetching %s: %s", url, e)

    return stats


def extract_date_ct(html: str) -> date | None:
    """Extract date from CT press release HTML.

    CT detail pages have date as MM/DD/YYYY in .content (line 2),
    or sometimes in a <p class="date"> tag.
    """
    import dateparser
    tree = HTMLParser(html)

    # Method 1: <p class="date"> tag
    date_node = tree.css_first("p.date")
    if date_node:
        text = date_node.text(strip=True)
        if text:
            parsed = dateparser.parse(text)
            if parsed:
                return parsed.date()

    # Method 2: MM/DD/YYYY in .content text (line 2)
    content = tree.css_first(".content")
    if content:
        text = content.text(separator="\n", strip=True)
        m = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        if m:
            parsed = dateparser.parse(m.group(1))
            if parsed:
                return parsed.date()

    return None


def extract_date_ma(html: str) -> date | None:
    """Extract date from MA press release HTML."""
    import dateparser
    tree = HTMLParser(html)
    # MA uses <div class="ma__press-status__date">10/09/2024</div>
    date_node = tree.css_first(".ma__press-status__date")
    if date_node:
        text = date_node.text(strip=True)
        if text:
            parsed = dateparser.parse(text)
            if parsed:
                return parsed.date()
    return None


def extract_date_il(html: str, url: str) -> date | None:
    """Extract date from IL press release HTML or URL."""
    import dateparser
    tree = HTMLParser(html)
    # IL uses <p class="dateformat"><strong>January 9, 2023</strong></p>
    date_node = tree.css_first("p.dateformat")
    if date_node:
        text = date_node.text(strip=True)
        if text:
            parsed = dateparser.parse(text)
            if parsed:
                return parsed.date()
    # Fallback: extract from URL pattern /pressroom/YYYY_MM/YYYYMMDD.html
    m = re.search(r"/pressroom/(\d{4})_(\d{2})/(\d{4})(\d{2})(\d{2})", url)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(4)), int(m.group(5)))
        except ValueError:
            pass
    # Broader fallback: just year and month from directory
    m = re.search(r"/pressroom/(\d{4})_(\d{2})/", url)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 15)
        except ValueError:
            pass
    return None


def extract_title_ct(html: str) -> str:
    """Extract title from CT press release — uses h3 inside .content."""
    tree = HTMLParser(html)
    content = tree.css_first(".content")
    if content:
        h3 = content.css_first("h3")
        if h3:
            return h3.text(strip=True)
    return extract_title_from_html(html)


def extract_title_il(html: str) -> str:
    """Extract title from IL press release — uses h2.presscontent."""
    tree = HTMLParser(html)
    h2 = tree.css_first("h2.presscontent")
    if h2:
        return h2.text(strip=True)
    return extract_title_from_html(html)


async def scrape_pa(db: Database, since: date) -> dict:
    """Scrape PA press releases from Wayback Machine."""
    config = WAYBACK_STATES["pa"]
    stats = {"state": "PA", "found": 0, "stored": 0, "skipped": 0, "errors": 0}

    # Get all archived URLs
    results = await query_cdx(
        "www.attorneygeneral.gov/taking-action/*",
        since=since.strftime("%Y%m%d"),
        limit=10000,
    )

    # Filter out listing/pagination pages
    detail_urls = []
    for r in results:
        url = r["url"]
        skip = False
        for pattern in config["exclude_patterns"]:
            if re.search(pattern, url):
                skip = True
                break
        if not skip and "/taking-action/" in url:
            # Must have a slug after /taking-action/
            path = urlparse(url).path.rstrip("/")
            if path != "/taking-action" and not re.match(r"/taking-action/page/\d+$", path):
                detail_urls.append(r)

    stats["found"] = len(detail_urls)
    logger.info("[PA] Total unique detail URLs: %d (from %d CDX results)", len(detail_urls), len(results))

    # Filter already-scraped
    new_urls = []
    for r in detail_urls:
        url = r["url"]
        if not url.startswith("https://"):
            url = "https://" + url if not url.startswith("http") else url
        if not db.action_exists(url):
            new_urls.append({**r, "url": url})
        else:
            stats["skipped"] += 1

    logger.info("[PA] New URLs to fetch: %d (skipped %d existing)", len(new_urls), stats["skipped"])

    # Fetch detail pages
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, entry in enumerate(new_urls):
            url = entry["url"]
            ts = entry["timestamp"]

            try:
                html = await fetch_wayback_page(client, ts, url)
                if not html:
                    stats["errors"] += 1
                    continue

                title = extract_title_from_html(html)
                body = extract_body_text(html, config["body_selector"])
                pr_date = extract_date_pa(html) or since

                if not title or not body:
                    stats["errors"] += 1
                    continue

                # Store
                if not db.action_exists(url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state="PA",
                            date_announced=pr_date,
                            action_type="other",
                            status="announced",
                            headline=title,
                            source_url=url,
                            raw_text=body[:10000],
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 50 == 0:
                    logger.info("[PA] Progress: %d/%d fetched, %d stored",
                                i + 1, len(new_urls), stats["stored"])

                # Rate limit for Wayback
                await asyncio.sleep(1.0)

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[PA] Error fetching %s: %s", url, e)

    return stats


async def scrape_ct(db: Database, since: date) -> dict:
    """Scrape CT press releases from Wayback Machine."""
    config = WAYBACK_STATES["ct"]
    stats = {"state": "CT", "found": 0, "stored": 0, "skipped": 0, "errors": 0}

    all_urls = []
    for year in config["years"]:
        pattern = f"portal.ct.gov/ag/press-releases/{year}-press-releases/*"
        results = await query_cdx(pattern, since=since.strftime("%Y%m%d"))
        all_urls.extend(results)
        logger.info("[CT] CDX year %d: %d URLs", year, len(results))

    # Filter out listing/index/template pages
    detail_urls = []
    for r in all_urls:
        url = r["url"]
        skip = False
        for pattern in config["exclude_patterns"]:
            if re.search(pattern, url):
                skip = True
                break
        if not skip:
            detail_urls.append(r)

    stats["found"] = len(detail_urls)
    logger.info("[CT] Total unique detail URLs: %d", len(detail_urls))

    # Filter already-scraped
    new_urls = []
    for r in detail_urls:
        url = r["url"]
        if not url.startswith("https://"):
            url = "https://" + url if not url.startswith("http") else url
        if not db.action_exists(url):
            new_urls.append({**r, "url": url})
        else:
            stats["skipped"] += 1

    logger.info("[CT] New URLs to fetch: %d (skipped %d existing)", len(new_urls), stats["skipped"])

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, entry in enumerate(new_urls):
            url = entry["url"]
            ts = entry["timestamp"]

            try:
                html = await fetch_wayback_page(client, ts, url)
                if not html:
                    stats["errors"] += 1
                    continue

                title = extract_title_ct(html)
                body = extract_body_text(html, config["body_selector"])
                pr_date = extract_date_ct(html) or since

                # Reject short/empty body (listing pages, nav-only)
                min_len = config.get("min_body_length", 100)
                if not title or not body or len(body) < min_len:
                    stats["errors"] += 1
                    continue

                if not db.action_exists(url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state="CT",
                            date_announced=pr_date,
                            action_type="other",
                            status="announced",
                            headline=title,
                            source_url=url,
                            raw_text=body[:10000],
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 50 == 0:
                    logger.info("[CT] Progress: %d/%d fetched, %d stored",
                                i + 1, len(new_urls), stats["stored"])

                await asyncio.sleep(1.0)

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[CT] Error fetching %s: %s", url, e)

    return stats


async def scrape_ma(db: Database, since: date) -> dict:
    """Scrape MA press releases from Wayback Machine."""
    config = WAYBACK_STATES["ma"]
    stats = {"state": "MA", "found": 0, "stored": 0, "skipped": 0, "errors": 0}

    results = await query_cdx(
        "mass.gov/news/ag-*",
        since=since.strftime("%Y%m%d"),
        limit=5000,
    )

    # Filter: remove URLs with query params (duplicates) and keep only clean paths
    detail_urls = []
    seen_paths = set()
    for r in results:
        url = r["url"]
        skip = False
        for pattern in config["exclude_patterns"]:
            if re.search(pattern, url):
                skip = True
                break
        if skip:
            continue
        # Deduplicate by path only (ignore http vs https, www vs not)
        path = urlparse(url).path.rstrip("/")
        if path in seen_paths:
            continue
        seen_paths.add(path)
        detail_urls.append(r)

    stats["found"] = len(detail_urls)
    logger.info("[MA] Total unique detail URLs: %d (from %d CDX results)", len(detail_urls), len(results))

    # Filter already-scraped
    new_urls = []
    for r in detail_urls:
        url = r["url"]
        if not url.startswith("https://"):
            url = "https://" + url if not url.startswith("http") else url
        if not db.action_exists(url):
            new_urls.append({**r, "url": url})
        else:
            stats["skipped"] += 1

    logger.info("[MA] New URLs to fetch: %d (skipped %d existing)", len(new_urls), stats["skipped"])

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, entry in enumerate(new_urls):
            url = entry["url"]
            ts = entry["timestamp"]

            try:
                html = await fetch_wayback_page(client, ts, url)
                if not html:
                    stats["errors"] += 1
                    continue

                title = extract_title_from_html(html)
                body = extract_body_text(html, config["body_selector"])
                pr_date = extract_date_ma(html) or since

                if not title or not body:
                    stats["errors"] += 1
                    continue

                if not db.action_exists(url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state="MA",
                            date_announced=pr_date,
                            action_type="other",
                            status="announced",
                            headline=title,
                            source_url=url,
                            raw_text=body[:10000],
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 50 == 0:
                    logger.info("[MA] Progress: %d/%d fetched, %d stored",
                                i + 1, len(new_urls), stats["stored"])

                await asyncio.sleep(1.0)

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[MA] Error fetching %s: %s", url, e)

    return stats


async def scrape_il(db: Database, since: date) -> dict:
    """Scrape IL press releases from Wayback Machine."""
    config = WAYBACK_STATES["il"]
    stats = {"state": "IL", "found": 0, "stored": 0, "skipped": 0, "errors": 0}

    all_urls = []
    for year in config["years"]:
        # IL uses YYYY_MM directory structure
        for month in range(1, 13):
            pattern = f"illinoisattorneygeneral.gov/pressroom/{year}_{month:02d}/*"
            results = await query_cdx(pattern, since=since.strftime("%Y%m%d"))
            all_urls.extend(results)
        logger.info("[IL] CDX year %d: total so far %d URLs", year, len(all_urls))

    # Filter out index pages and deduplicate
    detail_urls = []
    seen_paths = set()
    for r in all_urls:
        url = r["url"]
        skip = False
        for pattern in config["exclude_patterns"]:
            if re.search(pattern, url):
                skip = True
                break
        if skip:
            continue
        # Must be an .html file (not directory listing)
        if not url.endswith(".html"):
            continue
        # Deduplicate by path
        path = urlparse(url).path.rstrip("/")
        if path in seen_paths:
            continue
        seen_paths.add(path)
        detail_urls.append(r)

    stats["found"] = len(detail_urls)
    logger.info("[IL] Total unique detail URLs: %d", len(detail_urls))

    # Filter already-scraped
    new_urls = []
    for r in detail_urls:
        url = r["url"]
        if not url.startswith("https://"):
            url = "https://" + url if not url.startswith("http") else url
        if not db.action_exists(url):
            new_urls.append({**r, "url": url})
        else:
            stats["skipped"] += 1

    logger.info("[IL] New URLs to fetch: %d (skipped %d existing)", len(new_urls), stats["skipped"])

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, entry in enumerate(new_urls):
            url = entry["url"]
            ts = entry["timestamp"]

            try:
                html = await fetch_wayback_page(client, ts, url)
                if not html:
                    stats["errors"] += 1
                    continue

                title = extract_title_il(html)
                body = extract_body_text(html, config["body_selector"])
                pr_date = extract_date_il(html, url) or since

                if not title or not body:
                    stats["errors"] += 1
                    continue

                if not db.action_exists(url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state="IL",
                            date_announced=pr_date,
                            action_type="other",
                            status="announced",
                            headline=title,
                            source_url=url,
                            raw_text=body[:10000],
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 50 == 0:
                    logger.info("[IL] Progress: %d/%d fetched, %d stored",
                                i + 1, len(new_urls), stats["stored"])

                await asyncio.sleep(1.0)

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[IL] Error fetching %s: %s", url, e)

    return stats


def extract_date_wa(html: str) -> date | None:
    """Extract date from WA press release HTML."""
    tree = HTMLParser(html)
    # WA uses <time datetime="2026-02-03T18:55:01-08:00">
    time_node = tree.css_first("time[datetime]")
    if time_node:
        dt_attr = time_node.attributes.get("datetime", "")
        if dt_attr:
            try:
                return datetime.fromisoformat(dt_attr.replace("Z", "+00:00")).date()
            except ValueError:
                pass
    # Fallback: "FOR IMMEDIATE RELEASE:" followed by date text
    import dateparser
    body = tree.body
    if body:
        text = body.text(separator="\n", strip=True)[:500]
        match = re.search(
            r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4})',
            text,
        )
        if match:
            parsed = dateparser.parse(match.group(1))
            if parsed:
                return parsed.date()
    return None


async def scrape_wa(db: Database, since: date) -> dict:
    """Scrape WA press releases from Wayback Machine."""
    config = WAYBACK_STATES["wa"]
    stats = {"state": "WA", "found": 0, "stored": 0, "skipped": 0, "errors": 0}

    results = await query_cdx(
        "atg.wa.gov/news/news-releases/*",
        since=since.strftime("%Y%m%d"),
        limit=5000,
    )

    # Filter out listing/year pages
    detail_urls = []
    seen_paths = set()
    for r in results:
        url = r["url"]
        skip = False
        for pattern in config["exclude_patterns"]:
            if re.search(pattern, url, re.IGNORECASE):
                skip = True
                break
        if skip:
            continue
        path = urlparse(url).path.rstrip("/").lower()
        if path in seen_paths:
            continue
        seen_paths.add(path)
        detail_urls.append(r)

    stats["found"] = len(detail_urls)
    logger.info("[WA] Total unique detail URLs: %d (from %d CDX results)", len(detail_urls), len(results))

    # Filter already-scraped
    new_urls = []
    for r in detail_urls:
        url = r["url"]
        if not url.startswith("https://"):
            url = "https://" + url if not url.startswith("http") else url
        if not db.action_exists(url):
            new_urls.append({**r, "url": url})
        else:
            stats["skipped"] += 1

    logger.info("[WA] New URLs to fetch: %d (skipped %d existing)", len(new_urls), stats["skipped"])

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, entry in enumerate(new_urls):
            url = entry["url"]
            ts = entry["timestamp"]

            try:
                html = await fetch_wayback_page(client, ts, url)
                if not html:
                    stats["errors"] += 1
                    continue

                title = extract_title_from_html(html)
                body = extract_body_text(html, config["body_selector"])
                pr_date = extract_date_wa(html) or since

                if not title or not body:
                    stats["errors"] += 1
                    continue

                if not db.action_exists(url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state="WA",
                            date_announced=pr_date,
                            action_type="other",
                            status="announced",
                            headline=title,
                            source_url=url,
                            raw_text=body[:10000],
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 50 == 0:
                    logger.info("[WA] Progress: %d/%d fetched, %d stored",
                                i + 1, len(new_urls), stats["stored"])

                await asyncio.sleep(1.0)

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[WA] Error fetching %s: %s", url, e)

    return stats


async def run_all(since: date, states: list[str]):
    db = Database()
    db.create_tables()

    tasks = []
    if "ny" in states:
        tasks.append(scrape_ny(db, since))
    if "pa" in states:
        tasks.append(scrape_pa(db, since))
    if "ct" in states:
        tasks.append(scrape_ct(db, since))
    if "ma" in states:
        tasks.append(scrape_ma(db, since))
    if "il" in states:
        tasks.append(scrape_il(db, since))
    if "wa" in states:
        tasks.append(scrape_wa(db, since))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("\n" + "=" * 60)
    logger.info("WAYBACK SCRAPE SUMMARY")
    logger.info("=" * 60)
    total_stored = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error("Failed: %s", r)
            continue
        logger.info(
            "  %-5s: %4d found, %4d stored, %3d skipped, %3d errors",
            r["state"], r["found"], r["stored"], r["skipped"], r["errors"],
        )
        total_stored += r["stored"]
    logger.info("TOTAL: %d new records stored", total_stored)


@click.command()
@click.option("--since", default="2022-01-01", help="Since date (YYYY-MM-DD)")
@click.option("--states", default="ny,pa", help="Comma-separated state keys")
def main(since, states):
    since_date = date.fromisoformat(since)
    state_list = [s.strip().lower() for s in states.split(",")]
    logger.info("Wayback scraping states: %s since %s", state_list, since_date)
    start = time.time()
    asyncio.run(run_all(since_date, state_list))
    elapsed = time.time() - start
    logger.info("Total time: %.1f minutes", elapsed / 60)


if __name__ == "__main__":
    main()
