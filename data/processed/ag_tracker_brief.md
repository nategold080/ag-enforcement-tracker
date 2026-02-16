# State Attorney General Enforcement Action Database

### Structured data on 5,700+ corporate enforcement actions across 8 states, 2022–2025

---

## What This Database Covers

- **5,770 corporate enforcement actions** — lawsuits, settlements, consent decrees, and judgments — scraped and structured from official state AG press releases across **California, New York, Texas, Washington, Massachusetts, Ohio, Oregon, and Virginia**

- **Named defendants, settlement amounts, violation categories, and statute citations** extracted from unstructured press release text using rule-based NLP — fully deterministic and auditable

- **Multistate enforcement tracking** identifying companies targeted by 3+ state AG offices simultaneously — the clearest signal of coordinated regulatory risk

---

## Top Companies by AG Enforcement Actions (2022–2025)

Settlement amounts shown are verified against source press releases. Where the same multistate settlement appears in multiple states' records, only the total settlement amount is shown once.

| Company | Actions | States | Verified Settlement |
|---------|:-------:|:------:|--------------------:|
| Google | 50 | 6 | $1.1B |
| Intuit (TurboTax) | 37 | 4 | $141M |
| Juul Labs | 21 | 6 | $528M |
| Meta (Facebook) | 19 | 6 | $35M |
| Purdue Pharma | 17 | 6 | $666M |
| Johnson & Johnson | 14 | 5 | $967M |
| TikTok / ByteDance | 11 | 4 | — |
| Sackler Family | 10 | 5 | — |
| Walmart | 9 | 5 | — |
| Amazon | 8 | 4 | $2.9M |
| Walgreens | 8 | 4 | $343M |
| Uber | 7 | 4 | $478M |
| Live Nation | 5 | 4 | — (pending) |
| Marriott | 5 | 4 | $52M |
| Mercedes-Benz USA | 4 | 4 | $150M |

---

## Enforcement Trends: Category Volume by Year

| Category | 2022 | 2023 | 2024 | 2025 | Trend |
|----------|:----:|:----:|:----:|:----:|-------|
| Consumer Protection | 758 | 411 | 343 | 316 | Largest category every year |
| Healthcare Fraud | 388 | 218 | 139 | 155 | Rebounding after dip |
| Environmental | 218 | 172 | 120 | 107 | Steady decline |
| Data Privacy | 236 | 162 | 104 | 89 | Declining |
| Tech Platform | 76 | 36 | 68 | 68 | Only category growing YoY |
| Antitrust | 206 | 141 | 49 | 33 | Down 84% since 2022 |

---

## Sample Queries This Database Answers

1. **Which companies face the most AG enforcement risk?**
   Ranked by action count, state count, and settlement history across all tracked states.

2. **How has data privacy enforcement changed since 2022?**
   Year-over-year category tracking shows privacy actions declining 62% from 2022 peak.

3. **Which states are most active in consumer protection?**
   California (993 actions), New York (893), and Texas (760) lead all categories.

4. **What are the typical settlement terms for my industry?**
   Filter by violation category and defendant to see comparable settlements and terms.

5. **Which multistate actions should my company be monitoring?**
   40+ companies currently targeted by 3+ state AG offices simultaneously.

---

## Dataset Specifications

| Attribute | Detail |
|-----------|--------|
| Records | 5,770 corporate enforcement actions |
| States | CA, NY, TX, WA, MA, OH, OR, VA |
| Date range | 2022–2025 (some states include archives to 1996) |
| Fields per record | State, date, headline, defendant(s), action type, violation category, settlement amount, statute(s), source URL |
| Extraction method | Rule-based (regex + keyword), no LLM dependency |
| Update frequency | Configurable automated scraping pipeline |
| Export formats | CSV, JSON, API (FastAPI), Streamlit dashboard |
| Federal litigation | Tracked separately (1,400+ AG-vs-federal-government actions available) |

---

<div style="text-align: center; margin-top: 40px; padding: 20px; background: #f8f9fa; border-radius: 8px;">

**Interested in access?**

[Your Name] | [your@email.com] | [Your Company]

Sample dataset and live dashboard demo available on request.

</div>
