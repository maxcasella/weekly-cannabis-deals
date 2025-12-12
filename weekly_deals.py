import argparse
import csv
import json
import re
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List

import requests
from dateutil.parser import isoparse
import feedparser


# -------------------------
# Config (tune these)
# -------------------------

CANNABIS_TERMS = [
    "cannabis", "marijuana", "dispensary", "dispensaries", "hemp", "thc", "cbd",
    "cultivation", "grower", "processor", "processing", "extract", "extraction",
]

DEAL_TERMS = [
    "acquire", "acquires", "acquired", "acquisition", "merge", "merger",
    "buyout", "purchased", "purchase", "sale", "sold", "transaction",
    "investment", "raises", "raised", "funding", "financing", "credit facility",
    "term loan", "notes", "convertible", "private placement", "pipe",
    "sale-leaseback", "strategic partnership", "joint venture",
]

# EDGAR RSS feeds (recent filings) - disabled in main() for now
EDGAR_RSS = [
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&company=&dateb=&owner=include&start=0&count=100&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=S-4&company=&dateb=&owner=include&start=0&count=100&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=SC%2013D&company=&dateb=&owner=include&start=0&count=100&output=atom",
]

# Optional RSS feeds you legally can access
NEWS_RSS = [
    # "https://mjbizdaily.com/feed/",
]

# Bing News Search endpoint
BING_ENDPOINT = "https://api.bing.microsoft.com/v7.0/news/search"

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
    t = (text or "").lower()
    if any(k in t for k in ["acquire", "acquired", "acquisition", "merger", "sold to", "purchase agreement"]):
        return "M&A"
    if any(k in t for k in ["raises", "raised", "funding", "series", "private placement", "pipe"]):
        return "Capital Raise"
    if any(k in t for k in ["credit facility", "term loan", "notes", "convertible", "secured", "debt", "debenture"]):
        return "Debt"
    return "Other"


AMOUNT_RE = re.compile(
    r"(\$|USD\s?)\s?([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s?(million|billion|m|bn)?",
    re.IGNORECASE
)


def guess_amount(text: str) -> str:
    m = AMOUNT_RE.search(text or "")
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
    t = title or ""
    splits = [" acquires ", " acquisition of ", " to acquire ", " to be acquired by ", " merger with ", " raises ", " secures "]
    low = t.lower()
    for s in splits:
        if s in low:
            i = low.index(s)
            left = clean_text(t[:i])
            right = clean_text(t[i + len(s):])
            return f"{left} | {right}"[:200]
    return clean_text(t)[:200]


def contains_keywords(text: str, required_any: List[str], deal_any: List[str]) -> bool:
    low = (text or "").lower()
    return any(k.lower() in low for k in required_any) and any(k.lower() in low for k in deal_any)


# -------------------------
# EDGAR fetch (kept, but disabled in main for now)
# -------------------------

def fetch_edgar(days: int) -> List[DealItem]:
    out: List[DealItem] = []
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}

    for feed_url in EDGAR_RSS:
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

            has_deal = any(k.lower() in blob.lower() for k in DEAL_TERMS)
            has_cannabis_hint = re.search(r"(cannab|marij|hemp|dispens|thc|cbd)", blob.lower()) is not None

            if not has_deal and not has_cannabis_hint:
                continue

            out.append(DealItem(
                source="SEC EDGAR",
                source_type="edgar",
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
# Bing News fetch (primary)
# -------------------------

def bing_query(days: int) -> List[DealItem]:
    key = (os.getenv("BING_NEWS_KEY") or "").strip()
    if not key:
        print("BING_NEWS_KEY is missing; returning empty list.")
        return []

    queries = [
        '(cannabis OR marijuana OR dispensary OR cultivation OR hemp) (acquires OR acquired OR acquisition OR merger OR "asset purchase")',
        '(cannabis OR marijuana OR dispensary OR cultivation OR hemp) (raises OR raised OR funding OR financing OR "private placement" OR "Series A" OR "Series B")',
        '(cannabis OR marijuana OR dispensary OR cultivation OR hemp) ("credit facility" OR "term loan" OR notes OR debenture OR convertible OR "sale-leaseback")',
        '(dispensary OR cannabis) (sold OR purchased OR buyer OR acquisition) (Michigan OR Colorado OR California OR Oregon OR Washington)',
    ]

    since_dt = iso_now() - timedelta(days=days)

    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "User-Agent": USER_AGENT,
    }

    out: List[DealItem] = []

    for q in queries:
        params = {
            "q": q,
            "count": 50,
            "freshness": "Week",
            "sortBy": "Date",
            "textFormat": "Raw",
            "safeSearch": "Off",
        }

        try:
            r = requests.get(BING_ENDPOINT, headers=headers, params=params, timeout=30)
            print("Bing status:", r.status_code)
            if r.status_code != 200:
                # Helpful for debugging authentication issues
                print("Bing error (first 200 chars):", (r.text or "")[:200])
                continue

            data = r.json()
        except Exception:
            continue

        for a in data.get("value", []) or []:
            title = clean_text(a.get("name", ""))
            url = a.get("url", "") or ""
            desc = clean_text(a.get("description", ""))

            date_raw = a.get("datePublished", "") or ""
            pub_iso = iso_now().isoformat()
            try:
                pub_dt = isoparse(date_raw) if date_raw else iso_now()
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                pub_iso = pub_dt.isoformat()
                if pub_dt < since_dt:
                    continue
            except Exception:
                pass

            blob = f"{title} {desc}"

            out.append(DealItem(
                source="Bing News",
                source_type="news",
                published_at=pub_iso,
                title=title,
                url=url,
                deal_type_guess=guess_deal_type(blob),
                entities_guess=guess_entities_from_title(title),
                amount_guess=guess_amount(blob),
                snippet=desc[:280],
            ))

        time.sleep(0.35)  # be polite to the API

    return dedupe(out)


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
    seen_url = set()
    seen_title = set()
    out: List[DealItem] = []
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

    # EDGAR disabled for now (too noisy without a curated cannabis company universe)
    edgar_items: List[DealItem] = []
    print("EDGAR items:", len(edgar_items))
    items += edgar_items

    news_items = bing_query(args.since)
    print("Bing items:", len(news_items))
    items += news_items

    rss_items = fetch_rss(args.since)
    print("RSS items:", len(rss_items))
    items += rss_items

    items = dedupe(items)

    def parse_dt(x: str) -> datetime:
        try:
            return isoparse(x)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    items.sort(key=lambda x: parse_dt(x.published_at), reverse=True)

    write_csv(items, args.out)
    write_json(items, args.out_json)

    print(f"Wrote {len(items)} items to {args.out} and {args.out_json}")


if __name__ == "__main__":
    main()