#!/usr/bin/env python3
"""
Curate a ~65-record representative sample from the AG enforcement database
for a government affairs consultancy demo.
"""

import sqlite3
import csv
import os
import re

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "ag_enforcement.db"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "processed", "multistate_sample_data.csv"
)

BASE_QUERY = """
SELECT
    ea.id,
    ea.state,
    ea.date_announced,
    ea.action_type,
    ea.headline,
    ea.source_url,
    ea.is_multistate,
    ea.quality_score,
    mt.total_amount,
    GROUP_CONCAT(DISTINCT d.canonical_name) as defendants,
    GROUP_CONCAT(DISTINCT d.industry) as industries,
    GROUP_CONCAT(DISTINCT vc.category) as violation_categories
FROM enforcement_actions ea
LEFT JOIN action_defendants ad ON ea.id = ad.action_id
LEFT JOIN defendants d ON ad.defendant_id = d.id
LEFT JOIN monetary_terms mt ON ea.id = mt.action_id
LEFT JOIN violation_categories vc ON ea.id = vc.action_id
WHERE ea.quality_score >= 0.5
GROUP BY ea.id
"""

# Well-known defendant names to prioritize
PRIORITY_DEFENDANTS = [
    "Google", "Meta", "Amazon", "Apple", "Microsoft", "Facebook",
    "Purdue Pharma", "Juul", "Johnson & Johnson", "Walmart",
    "Uber", "Lyft", "T-Mobile", "AT&T", "Equifax",
    "Wells Fargo", "Bank of America", "JPMorgan", "Citibank",
    "TikTok", "Tik Tok", "Snapchat", "Snap",
    "CVS", "Walgreens", "McKesson", "Cardinal Health",
    "3M", "DuPont", "Monsanto", "Syngenta",
    "ExxonMobil", "Chevron", "Shell",
    "Allergan", "Teva", "AbbVie", "Endo",
    "NRA", "KuCoin", "Celsius", "FTX", "Coinbase",
    "Intuit", "TurboTax", "Credit Karma",
    "Sackler", "Mallinckrodt",
    "Robinhood", "Gemini", "Genesis",
    "Albertsons", "Kroger",
    "Volkswagen", "Toyota", "Honda",
    "Comcast", "Verizon", "Sprint",
    "Electron Hydro", "CenturyLink", "Enbridge",
]

TARGET_STATES = ["CA", "NY", "WA", "TX", "MA", "OH", "OR"]

# Words that indicate a garbled/non-defendant name
BAD_NAME_SIGNALS = [
    "Against", "AG ", "Attorney General", "Press Release",
    "the ", "During The", "All ", "Filed", "Announced",
    "Alleged", "Action", "Seeking", "Brought", "Accepting",
    "Agreed", "Allegations", "Administration", "Advice",
    "False", "Misleading", "Operating", "Illinois-based",
    "Charles", "CEO", "Ceo", "Owner", "Owners",
    "Companies", "Centers", "Funding", "U.S",
    "Federal", "Everett man", "Seattle business",
    "Auburn business", "House passes", "Senate passes",
    "Targeting", "Suspects", "Unfair", "Secures",
    "USDA", "National Institutes", "National",
    "CIDs", "Purported", "January", "Senior Management",
    "Benefits", "Wages", "Energy Efficiency",
    "Largest", "Another", "Crypto Firm", "Cryptocurrency",
    "Succeeding", "Mass Health", "E-Cigarette",
    "JUULto", "Perez of",
    "Jeweler", "Ambulance Billing", "Requires The", "Defunding",
    "Others That Suspended", "Publicis Health",
]


def get_all_records(conn):
    cursor = conn.execute(BASE_QUERY)
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        rows.append(record)
    return rows


def clean_name(name):
    """Check if a single name part looks like a real defendant."""
    name = name.strip()
    if len(name) < 3 or len(name) > 70:
        return None
    for signal in BAD_NAME_SIGNALS:
        if name.startswith(signal):
            return None
    # Reject names that are all lowercase (likely sentence fragments)
    if name == name.lower() and len(name) > 15:
        return None
    # Reject names with too many spaces (likely sentence fragments)
    if name.count(" ") > 6:
        return None
    # Remove trailing junk phrases
    import re as _re
    name = _re.sub(r"\s+by\s+[A-Z].*$", "", name)
    name = _re.sub(r"\s+of all counts.*$", "", name)
    name = name.strip()
    if len(name) < 3:
        return None
    return name


def get_clean_defendants(record):
    """Return list of clean defendant names from a record."""
    defs_str = record.get("defendants") or ""
    if not defs_str:
        return []
    parts = [p.strip() for p in defs_str.split(",")]
    clean = [clean_name(p) for p in parts]
    return [c for c in clean if c is not None]


def has_clean_defendant(record):
    return len(get_clean_defendants(record)) > 0


def format_defendants(record, max_count=3):
    clean = get_clean_defendants(record)
    return "; ".join(clean[:max_count])


def clean_headline(headline):
    """Clean up headline artifacts."""
    if not headline:
        return ""
    # Remove "Press ReleaseAG" prefix from MA headlines
    headline = re.sub(r'^Press Release\s*', '', headline)
    # Truncate
    if len(headline) > 150:
        headline = headline[:147].rsplit(" ", 1)[0] + "..."
    return headline


def format_amount(amount):
    if amount is None or amount == 0:
        return ""
    amount = float(amount)
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    elif amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    elif amount >= 1_000:
        return f"${amount / 1_000:.0f}K"
    else:
        return f"${amount:,.0f}"


def get_amount_bucket(amount):
    if amount and float(amount) > 100_000_000:
        return "large"
    elif amount and float(amount) >= 1_000_000:
        return "mid"
    elif amount and float(amount) > 0:
        return "small"
    else:
        return "none"


def get_categories(record):
    """Get violation categories, capped at 3 for readability."""
    cats_str = record.get("violation_categories") or ""
    cats = [c.strip() for c in cats_str.split(",") if c.strip()]
    # Prioritize the most specific/interesting categories
    priority_order = [
        "data_privacy", "antitrust", "environmental", "healthcare",
        "tech_platform", "securities", "tobacco_vaping", "employment",
        "housing_lending", "telecommunications", "charitable",
        "consumer_protection", "other"
    ]
    cats_sorted = sorted(cats, key=lambda c: priority_order.index(c) if c in priority_order else 99)
    return cats_sorted[:3]  # Cap at 3 categories


def has_priority_defendant(record):
    defs = (record.get("defendants") or "").lower()
    for pd_name in PRIORITY_DEFENDANTS:
        if pd_name.lower() in defs:
            return True
    return False


def score_record(record, selected_ids, state_counts, category_counts,
                 action_type_counts, amount_bucket_counts, multistate_count,
                 selected_headline_keys):
    """Score a record for selection priority."""
    if record["id"] in selected_ids:
        return -100
    
    if not has_clean_defendant(record):
        return -100
    
    # Near-duplicate check
    hl_key = clean_headline(record.get("headline") or "")[:50].lower().strip()
    if hl_key in selected_headline_keys:
        return -100
    
    # Reject records with very low settlement amounts (under $500) — not demo-worthy
    amount = record.get("total_amount")
    if amount and 0 < float(amount) < 500:
        return -100
    
    score = 0.0
    score += record["quality_score"] * 2
    score += 3  # Base score for having clean defendant
    
    # Priority defendant bonus
    if has_priority_defendant(record):
        score += 5
    
    # State diversity — strict balancing
    state = record["state"]
    if state in TARGET_STATES:
        current_count = state_counts.get(state, 0)
        if current_count < 5:
            score += 8 - current_count
        elif current_count < 8:
            score += 2
        elif current_count < 11:
            score += 0
        else:
            score -= 15
    else:
        score -= 5
    
    # Category diversity
    cats = get_categories(record)
    for cat in cats:
        cat_count = category_counts.get(cat, 0)
        if cat_count < 2:
            score += 4
        elif cat_count < 5:
            score += 2
        elif cat_count < 8:
            score += 0.5
    
    # Action type diversity
    at = record["action_type"]
    at_count = action_type_counts.get(at, 0)
    if at in ("consent_decree", "assurance_of_discontinuance"):
        if at_count < 2:
            score += 5
        elif at_count < 4:
            score += 2
    elif at == "injunction":
        if at_count < 3:
            score += 4
        elif at_count < 5:
            score += 1
    else:
        if at_count < 10:
            score += 1
    
    # Settlement amount diversity
    bucket = get_amount_bucket(amount)
    bucket_count = amount_bucket_counts.get(bucket, 0)
    if bucket == "large" and bucket_count < 10:
        score += 3
    elif bucket == "mid" and bucket_count < 15:
        score += 2
    elif bucket == "small" and bucket_count < 8:
        score += 2
    elif bucket == "none" and bucket_count < 12:
        score += 1
    
    # Multistate bonus
    if record["is_multistate"]:
        if multistate_count < 12:
            score += 3
    
    # Recency bonus
    date = record.get("date_announced") or ""
    if date >= "2024-01-01":
        score += 2
    elif date >= "2022-01-01":
        score += 1
    elif date >= "2018-01-01":
        score += 0
    else:
        score -= 3
    
    return score


def select_records(all_records, target_count=65):
    selected = []
    selected_ids = set()
    state_counts = {}
    category_counts = {}
    action_type_counts = {}
    amount_bucket_counts = {}
    multistate_count = 0
    selected_headline_keys = set()
    
    candidates = [r for r in all_records if has_clean_defendant(r)]
    print(f"Total qualifying records with clean defendants: {len(candidates)}")
    
    for round_num in range(target_count):
        best_score = -999
        best_record = None
        
        for record in candidates:
            score = score_record(
                record, selected_ids, state_counts, category_counts,
                action_type_counts, amount_bucket_counts, multistate_count,
                selected_headline_keys
            )
            if score > best_score:
                best_score = score
                best_record = record
        
        if best_record is None or best_score <= -100:
            print(f"  Stopped at round {round_num} (best score: {best_score})")
            break
        
        selected.append(best_record)
        selected_ids.add(best_record["id"])
        
        state = best_record["state"]
        state_counts[state] = state_counts.get(state, 0) + 1
        
        for cat in get_categories(best_record):
            category_counts[cat] = category_counts.get(cat, 0) + 1
        
        at = best_record["action_type"]
        action_type_counts[at] = action_type_counts.get(at, 0) + 1
        
        bucket = get_amount_bucket(best_record.get("total_amount"))
        amount_bucket_counts[bucket] = amount_bucket_counts.get(bucket, 0) + 1
        
        if best_record["is_multistate"]:
            multistate_count += 1
        
        hl_key = clean_headline(best_record.get("headline") or "")[:50].lower().strip()
        selected_headline_keys.add(hl_key)
    
    return selected


def write_csv(records, output_path):
    fieldnames = [
        "state", "date", "defendant", "industry", "headline",
        "violation_category", "action_type", "settlement_amount",
        "is_multistate", "source_url"
    ]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in sorted(records, key=lambda r: (r["state"], r.get("date_announced") or "")):
            cats = get_categories(record)
            industries_str = record.get("industries") or ""
            industry = industries_str.split(",")[0].strip() if industries_str else ""
            
            row = {
                "state": record["state"],
                "date": record.get("date_announced") or "",
                "defendant": format_defendants(record),
                "industry": industry,
                "headline": clean_headline(record.get("headline") or ""),
                "violation_category": "; ".join(cats),
                "action_type": record.get("action_type") or "",
                "settlement_amount": format_amount(record.get("total_amount")),
                "is_multistate": "True" if record.get("is_multistate") else "False",
                "source_url": record.get("source_url") or "",
            }
            writer.writerow(row)


def print_summary(records):
    print(f"\n=== SELECTION SUMMARY ({len(records)} records) ===\n")
    
    state_counts = {}
    for r in records:
        s = r["state"]
        state_counts[s] = state_counts.get(s, 0) + 1
    print("By state:")
    for s, c in sorted(state_counts.items(), key=lambda x: -x[1]):
        print(f"  {s}: {c}")
    
    cat_counts = {}
    for r in records:
        for cat in get_categories(r):
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
    print("\nBy violation category (capped at 3 per record):")
    for c, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {c}: {cnt}")
    
    at_counts = {}
    for r in records:
        at = r["action_type"]
        at_counts[at] = at_counts.get(at, 0) + 1
    print("\nBy action type:")
    for at, cnt in sorted(at_counts.items(), key=lambda x: -x[1]):
        print(f"  {at}: {cnt}")
    
    buckets = {"large (>$100M)": 0, "mid ($1M-$100M)": 0, "small (<$1M)": 0, "none": 0}
    for r in records:
        amount = r.get("total_amount")
        if amount and float(amount) > 100_000_000:
            buckets["large (>$100M)"] += 1
        elif amount and float(amount) >= 1_000_000:
            buckets["mid ($1M-$100M)"] += 1
        elif amount and float(amount) > 0:
            buckets["small (<$1M)"] += 1
        else:
            buckets["none"] += 1
    print("\nBy settlement size:")
    for b, cnt in buckets.items():
        print(f"  {b}: {cnt}")
    
    ms_count = sum(1 for r in records if r.get("is_multistate"))
    print(f"\nMultistate actions: {ms_count}")
    
    dates = sorted([r.get("date_announced") or "" for r in records if r.get("date_announced")])
    if dates:
        print(f"Date range: {dates[0]} to {dates[-1]}")
    
    print("\nNotable defendants:")
    seen = set()
    for r in records:
        defs = (r.get("defendants") or "").lower()
        for pd_name in PRIORITY_DEFENDANTS:
            if pd_name.lower() in defs and pd_name not in seen:
                seen.add(pd_name)
                amt = format_amount(r.get("total_amount"))
                print(f"  {pd_name} ({r['state']}, {r.get('date_announced')}, {amt or 'no $'})")


def main():
    print(f"Database: {DB_PATH}")
    print(f"Output:   {OUTPUT_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    all_records = get_all_records(conn)
    print(f"Total records in database (quality >= 0.5): {len(all_records)}")
    
    selected = select_records(all_records, target_count=65)
    print_summary(selected)
    write_csv(selected, OUTPUT_PATH)
    print(f"\nCSV written to: {OUTPUT_PATH}")
    conn.close()


if __name__ == "__main__":
    main()
