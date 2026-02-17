## MUST DO BEFORE SHIPPING
- [ ] Washington (atg.wa.gov) — try curl without JS, fall back to Wayback Machine
- [ ] Massachusetts (mass.gov/ago) — try browser User-Agent headers, fall back to Wayback
- [ ] Illinois (ag.state.il.us or illinoisattorneygeneral.gov) — check both domains, Wayback fallback
- [ ] Connecticut (portal.ct.gov/ag) — try curl without JS, Wayback fallback
- [ ] New Jersey — same 403 fix as Massachusetts
- [ ] Colorado, Minnesota — Wayback investigation
- [ ] Re-run full extraction pipeline after adding states
- [ ] Update dashboard and analytics with new data

## DEFERRED STATES (low-yield scrapers marked inactive 2026-02-16)
- **Virginia (VA)**: Joomla/Gantry 5 site with table-based listing. Only 10 records scraped. CSS selectors (`tr`, `a`) are too generic for the table layout. Needs a custom scraper that targets the specific news-release table structure. The site also lacks dates in listings, requiring body text extraction for dates.
- **Pennsylvania (PA)**: WordPress with custom "Taking Action" post type. Only 2 records scraped. The `.ta-card` selector may not match the current site structure. Needs investigation into whether the site has changed its HTML structure or requires a custom scraper for the card-based layout.
