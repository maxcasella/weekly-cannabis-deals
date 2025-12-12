import argparse
import csv
import json
import re
import sys
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Iterable, Tuple

import requests
from bs4 import BeautifulSoup
from dateutil.parser import isoparse
import feedparser


# -------------------------
# Config (tune these)
# -------------------------

CANNABIS_TERMS = [
    "cannabis", "marijuana", "dispensary", "dispensaries", "hemp", "THC", "CBD",
    "cultivation", "grower", "processor", "processing", "extract", "extraction",
]

DEAL_TERMS = [
    "acquire", "acquires", "acquired", "acquisition", "merge", "merger",
    "buyout", "purchased", "purchase", "sale", "sold", "transaction",
    "investment", "raises", "raised", "funding", "financing", "credit facility",
    "term loan", "notes", "convertible", "private placement", "PIPE",
    "sale-leaseback", "strategic partnership", "joint venture",
]

# EDGAR RSS feeds (recent filings). We filter by keyword after pulling.
EDGAR_RSS = [
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&company=&dateb=&owner=include&start=0&count=100&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=S-4&company=&dateb=&owner=include&start=0&count=100&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=SC%2013D&company=&dateb=&owner=include&start=0&count=100&output=atom",
]

# Optional: add RSS feeds you legally can access (some trade sites block bots/paywall)
NEWS_RSS = [
    # Examples (replace with feeds you have access to)
    # "https://mjbizdaily.com/feed/",
]

# GDELT v2 DOC API (free). We use it to find local/long-tail coverage.
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"


USER_AGENT = "ViridianWeeklyDealsBot/0.1 (contact: research@yourdomain.com)"


@dataclass
class DealItem:
    source: str
    source_type: str          # "edgar" or "news"
    published_at: str         # ISO8601
    title: str
    url: str
    deal_type_guess: str      # "M&A" / "Capital Raise" / "Debt" / "Other"
    entities_guess: str       # quick guess from title
    amount_guess: str         # "$50M", etc if seen
    snippet: str              # short evidence snippet


def iso_now() -> datetime:
    return datetime.now(timezone.utc)


def within_days(dt: datetime, days: int) -> bool:
    return dt >= (iso_now() - timedelta(days=days))


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def guess_deal_type(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["acquire", "acquired", "acquisition", "merger", "sold to", "purchase agreement"]):
        return "M&A"
    if any(k in t for k in ["raises", "raised", "funding", "series", "private placement", "PIPE"]):
        return "Capital Raise"
    if any(k in t for k in ["credit facility", "term loan", "notes", "convertible", "secured", "debt"]):
        return "Debt"
    return "Other"


AMOUNT_RE = re.compile(r"(\$|USD\s?)\s?([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s?(million|billion|m|bn)?", re.IGNORECASE)

def guess_amount(text: str) -> str:
    m = AMOUNT_RE.search(text)
    if not m:
        return ""
    prefix = "$"
    num = m.group(2)
    scale = (m.group(3) or "").lower()
    if scale in ["million", "m"]:
        return f"{prefix}{num}M"
    if scale in ["billion", "bn"]:
        return f"{prefix}{num}B"
    return f"{prefix}{num}"


def guess_entities_from_title(title: str) -> str:
    # crude heuristic: split on " to ", " by ", " acquires ", " acquisition of "
    t = title
    splits = [" acquires ", " acquisition of ", " to acquire ", " to be acquired by ", " merger with ", " raises ", " secures "]
    low = t.lower()
    for s in splits:
        if s in low:
            i = low.index(s)
            left = clean_text(t[:i])
            right = clean_text(t[i+len(s):])
            return f"{left} | {right}"[:200]
    return clean_text(t)[:200]


def contains_keywords(text: str, required_any: List[str], deal_any: List[str]) -> bool:
    low = text.lower()
    return any(k.lower() in low for k in required_any) and any(k.lower() in low for k in deal_any)


# -------------------------
# EDGAR fetch
# -------------------------

def fetch_edgar(days: int) -> List[DealItem]:
    out: List[DealItem] = []
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}

    for feed_url in EDGAR_RSS:
        # SEC is picky: fetch with requests so we control headers, then parse the returned text
        try:
            r = requests.get(feed_url, headers=headers, timeout=30)
            r.raise_for_status()
            parsed = feedparser.parse(r.text)
        except Exception:
            continue

        for e in parsed.entries:
            pub_raw = getattr(e, "published", None) or getattr(e, "updated", None)
            if not pub_raw:
                continue
            try:
                pub_dt = isoparse(pub_raw)
            except Exception:
                continue
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if not within_days(pub_dt, days):
                continue

            title = clean_text(getattr(e, "title", "") or "")
            link = getattr(e, "link", "") or ""
            summary = clean_text(getattr(e, "summary", "") or "")

            blob = f"{title} {summary}"

            # EDGAR summaries are sparse. For EDGAR, require deal language,
            # and only *soft* cannabis hints (company name sometimes has it).
            has_deal = any(k.lower() in blob.lower() for k in DEAL_TERMS)
            has_cannabis_hint = re.search(r"(cannab|marij|hemp|dispens|thc|cbd)", blob.lower()) is not None

            if not has_deal and not has_cannabis_hint:
                continue

            snippet = summary[:280]
            out.append(DealItem(
                source="SEC EDGAR",
                source_type="edgar",
                published_at=pub_dt.isoformat(),
                title=title,
                url=link,
                deal_type_guess=guess_deal_type(blob),
                entities_guess=guess_entities_from_title(title),
                amount_guess=guess_amount(blob),
                snippet=snippet
            ))
    return out


# -------------------------
# GDELT fetch
# -------------------------

def gdelt_query(days: int) -> List[DealItem]:
    q = '(cannabis OR marijuana OR dispensary OR hemp) AND (acquire OR acquisition OR merger OR raises OR funding OR financing OR "credit facility" OR "private placement" OR "sale-leaseback")'

    start_dt = (iso_now() - timedelta(days=days)).strftime("%Y%m%d%H%M%S")
    end_dt = iso_now().strftime("%Y%m%d%H%M%S")

    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": 250,
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "sort": "HybridRel",
    }

    r = requests.get(GDELT_DOC_API, params=params, timeout=30)
    print("GDELT request URL:", r.url)
    print("GDELT status:", r.status_code)
    print("GDELT response (first 300 chars):", (r.text or "")[:300])

    with open("gdelt_raw.txt", "w", encoding="utf-8") as f:
        f.write(r.text or "")

    # Handle GDELT rate limiting gracefully (GitHub Actions IPs get throttled)
    if r.status_code == 429:
    # Wait ~6-10 seconds and retry once
    wait_s = 6 + random.random() * 4
    print(f"GDELT rate-limited (429). Sleeping {wait_s:.1f}s then retrying once...")
    time.sleep(wait_s)
    r = requests.get(GDELT_DOC_API, params=params, timeout=30)
    print("GDELT retry status:", r.status_code)
    with open("gdelt_raw.txt", "w", encoding="utf-8") as f:
        f.write(r.text or "")

    if r.status_code != 200:
    print("GDELT failed; returning empty list instead of crashing.")
    return []

    try:
    data = r.json()
    except Exception:
    print("GDELT returned non-JSON; returning empty list.")
    return []


    out: List[DealItem] = []
    for a in data.get("articles", []) or []:
        title = clean_text(a.get("title", ""))
        url = a.get("url", "") or ""
        seen = clean_text(a.get("seendate", ""))

        pub_iso = iso_now().isoformat()
        if seen and re.fullmatch(r"\d{14}", seen):
            pub_dt = datetime.strptime(seen, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            pub_iso = pub_dt.isoformat()

        blob = f"{title} {a.get('snippet','') or ''}"

        out.append(DealItem(
            source=f"GDELT ({clean_text(a.get('domain','')) or 'news'})",
            source_type="news",
            published_at=pub_iso,
            title=title,
            url=url,
            deal_type_guess=guess_deal_type(blob),
            entities_guess=guess_entities_from_title(title),
            amount_guess=guess_amount(blob),
            snippet=clean_text(a.get("snippet", ""))[:280]
        ))

    return out


# -------------------------
# RSS fetch (optional)
# -------------------------

def fetch_rss(days: int) -> List[DealItem]:
    out: List[DealItem] = []
    for feed_url in NEWS_RSS:
        parsed = feedparser.parse(feed_url)
        for e in parsed.entries:
            pub_raw = getattr(e, "published", None) or getattr(e, "updated", None)
            if not pub_raw:
                continue
            try:
                pub_dt = isoparse(pub_raw)
            except Exception:
                continue
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if not within_days(pub_dt, days):
                continue

            title = clean_text(getattr(e, "title", "") or "")
            link = getattr(e, "link", "") or ""
            summary = clean_text(getattr(e, "summary", "") or "")

            blob = f"{title} {summary}"
            if not contains_keywords(blob, CANNABIS_TERMS, DEAL_TERMS):
                continue

            out.append(DealItem(
                source=f"RSS ({feed_url})",
                source_type="news",
                published_at=pub_dt.isoformat(),
                title=title,
                url=link,
                deal_type_guess=guess_deal_type(blob),
                entities_guess=guess_entities_from_title(title),
                amount_guess=guess_amount(blob),
                snippet=summary[:280],
            ))
    return out


# -------------------------
# De-dupe
# -------------------------

def dedupe(items: List[DealItem]) -> List[DealItem]:
    # Simple dedupe: same URL OR same (title normalized)
    seen_url = set()
    seen_title = set()
    out = []
    for it in items:
        u = (it.url or "").strip()
        t = re.sub(r"[^a-z0-9]+", "", (it.title or "").lower())
        if u and u in seen_url:
            continue
        if t and t in seen_title:
            continue
        if u:
            seen_url.add(u)
        if t:
            seen_title.add(t)
        out.append(it)
    return out


def write_csv(items: List[DealItem], path: str):
    fieldnames = list(asdict(items[0]).keys()) if items else [
        "source","source_type","published_at","title","url","deal_type_guess","entities_guess","amount_guess","snippet"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            w.writerow(asdict(it))


def write_json(items: List[DealItem], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in items], f, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=int, default=7, help="Lookback window in days")
    ap.add_argument("--out", type=str, default="deals.csv", help="CSV output path")
    ap.add_argument("--out_json", type=str, default="deals.json", help="JSON output path")
    args = ap.parse_args()

    items: List[DealItem] = []
    edgar_items = []  # temporarily disabled (was too noisy without a cannabis universe list)
    print("EDGAR items:", len(edgar_items))
    items += edgar_items


    gdelt_items = gdelt_query(args.since)
    print("GDELT items:", len(gdelt_items))
    items += gdelt_items

    rss_items = fetch_rss(args.since)
    print("RSS items:", len(rss_items))
    items += rss_items

    items = dedupe(items)

    # Sort newest first
    def parse_dt(x: str) -> datetime:
        try:
            return isoparse(x)
        except Exception:
            return datetime(1970,1,1,tzinfo=timezone.utc)

    items.sort(key=lambda x: parse_dt(x.published_at), reverse=True)

    write_csv(items, args.out)
    write_json(items, args.out_json)

    print(f"Wrote {len(items)} items to {args.out} and {args.out_json}")


if __name__ == "__main__":
    main()