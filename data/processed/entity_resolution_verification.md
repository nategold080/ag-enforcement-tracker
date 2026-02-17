# Entity Resolution Verification Report

Generated: 2026-02-16

Verification of cross-state entity tracking for 5 major multistate
defendants. These companies were selected because they are known to
face enforcement actions from multiple state AGs simultaneously.

## Results

### Google

| Metric | Value |
|--------|-------|
| Enforcement actions | 73 |
| States tracked | CA, MA, NY, OH, TX, WA (6) |
| Primary canonical name | "Google" |
| Resolution accuracy | 95% of actions resolve to primary |

### Meta / Facebook

| Metric | Value |
|--------|-------|
| Enforcement actions | 27 |
| States tracked | CA, MA, NY, OH, TX, WA (6) |
| Primary canonical name | "Meta" |
| Resolution accuracy | 100% of actions resolve to primary |

### Purdue Pharma

| Metric | Value |
|--------|-------|
| Enforcement actions | 26 |
| States tracked | CA, MA, NY, OH, OR, TX, WA (7) |
| Primary canonical name | "Purdue Pharma" |
| Resolution accuracy | 92% of actions resolve to primary |

### Johnson & Johnson

| Metric | Value |
|--------|-------|
| Enforcement actions | 22 |
| States tracked | CA, NY, OH, TX, WA (5) |
| Primary canonical name | "Johnson & Johnson" |
| Resolution accuracy | 95% of actions resolve to primary |

### JUUL

| Metric | Value |
|--------|-------|
| Enforcement actions | 27 |
| States tracked | CA, MA, NY, OH, TX, WA (6) |
| Primary canonical name | "Juul" |
| Resolution accuracy | 78% of actions resolve to primary |

## Assessment

All 5 verified companies are properly tracked across multiple states.
Entity resolution correctly handles:
- Legal suffix variations (LLC, Inc, Corp, L.P.)
- Name format variations (Google LLC vs Google vs Google, Inc.)
- Cross-brand resolution (Facebook -> Meta)
- Body-text extraction artifacts (trailing sentence fragments)

The pipeline uses deterministic matching (no LLM) with:
1. Known-alias lookups from config/entities.yaml (62 canonical entities)
2. Fuzzy matching via token_sort_ratio (threshold: 85% auto-match)
3. Automated name cleaning (legal suffixes, trailing fragments, descriptors)

Known limitations:
- ~5-10% of raw name extractions include context that fuzzy matching cannot resolve
- Subsidiaries (e.g., Ethicon for J&J) are tracked as separate entities by design
- Very short aliases (e.g., "Purdue" alone) may not meet the 85% fuzzy threshold