import json
import os
import sys
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

# Allow this fetcher to run both as a script and as an imported module.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app.db.init_db import ensure_articles_columns
from app.fetchers.url_utils import normalize_canonical_url

# Yahoo Finance discovery endpoint used here:
# https://query1.finance.yahoo.com/v1/finance/search?q={ticker}&quotesCount=0&newsCount={n}
# The endpoint returns ticker-scoped Yahoo Finance search results, including news
# items with title, link, publisher, and providerPublishTime fields.

DB_PATH = os.path.join(BASE_DIR, "data", "news.db")

LOOKBACK_HOURS = 48
NEWS_COUNT = 25
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"
USER_AGENT = "Mozilla/5.0 (compatible; news_system/1.0)"

SOURCE_NORMALIZATION = {
    "reuters.com": "Reuters",
    "reuters": "Reuters",
    "bloomberg": "Bloomberg",
    "cnbc": "CNBC",
    "financial times": "Financial Times",
    "ft": "Financial Times",
    "wsj": "Wall Street Journal",
    "the wall street journal": "Wall Street Journal",
    "wall street journal": "Wall Street Journal",
    "barron's": "Barron's",
    "marketwatch": "MarketWatch",
    "yahoo finance": "Yahoo Finance",
    "seeking alpha": "Seeking Alpha",
    "investor's business daily": "Investor's Business Daily",
    "investors business daily": "Investor's Business Daily",
    "tipranks": "TipRanks",
    "benzinga": "Benzinga",
    "the information": "The Information",
    "associated press": "Associated Press",
    "ap news": "AP News",
    "business wire": "Business Wire",
    "globenewswire": "GlobeNewswire",
    "globe newswire": "GlobeNewswire",
    "pr newswire": "PR Newswire",
}


def get_db_connection():
    return sqlite3.connect(DB_PATH)


def get_watchlist(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, company_name
        FROM watchlist
        ORDER BY ticker
    """)
    return cursor.fetchall()


def build_yahoo_finance_url(ticker):
    query = urllib.parse.urlencode(
        {
            "q": ticker,
            "quotesCount": 0,
            "newsCount": NEWS_COUNT,
            "enableFuzzyQuery": "false",
        }
    )
    return f"{YAHOO_SEARCH_URL}?{query}"


def fetch_yahoo_news(ticker):
    url = build_yahoo_finance_url(ticker)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return payload.get("news") or []


def parse_yahoo_publish_time(value):
    if not value:
        return None

    try:
        timestamp = int(value)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except Exception:
        return None


def is_within_lookback(published_at_str, lookback_hours=48):
    if not published_at_str:
        return False

    try:
        published_dt = datetime.strptime(
            published_at_str, "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        return published_dt >= cutoff
    except Exception:
        return False


def normalize_source_name(source):
    if not source:
        return ""

    source_clean = source.strip()
    source_lower = source_clean.lower()

    for key, normalized in SOURCE_NORMALIZATION.items():
        if source_lower == key or key in source_lower:
            return normalized

    return source_clean


def article_exists(conn, canonical_url):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1
        FROM articles
        WHERE canonical_url = ?
           OR (canonical_url IS NULL AND url = ?)
        LIMIT 1
    """, (canonical_url, canonical_url))
    return cursor.fetchone() is not None


def save_article(conn, ticker, title, source, published_at, url, canonical_url):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO articles (
            ticker,
            title,
            source,
            source_type,
            discovery_source,
            url,
            canonical_url,
            published_at,
            fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (
        ticker,
        title,
        source,
        "yahoo_finance",
        "yahoo_finance",
        url,
        canonical_url,
        published_at,
    ))
    conn.commit()


def fetch_and_store():
    conn = get_db_connection()
    ensure_articles_columns(conn.cursor())
    conn.commit()
    watchlist = get_watchlist(conn)

    if not watchlist:
        print("No watchlist rows found.")
        conn.close()
        return

    total_fetched = 0
    total_saved = 0
    total_skipped_old = 0
    total_skipped_dup = 0
    total_skipped_invalid = 0
    failed_tickers = []

    for ticker, company_name in watchlist:
        print(f"\nFetching Yahoo Finance news for {ticker} | {company_name}")

        try:
            news_items = fetch_yahoo_news(ticker)
        except Exception as exc:
            failed_tickers.append((ticker, str(exc)))
            print(f"  Failed ticker: {ticker} | {exc}")
            continue

        total_fetched += len(news_items)
        print(f"  Yahoo articles fetched: {len(news_items)}")

        saved_count = 0
        skipped_old = 0
        skipped_dup = 0
        skipped_invalid = 0

        for item in news_items:
            title = (item.get("title") or "").strip()
            url = (item.get("link") or "").strip()
            source = normalize_source_name((item.get("publisher") or "").strip())
            published_at = parse_yahoo_publish_time(item.get("providerPublishTime"))
            canonical_url = normalize_canonical_url(url)

            if not title or not url or not canonical_url or not published_at:
                skipped_invalid += 1
                continue

            if not is_within_lookback(published_at, LOOKBACK_HOURS):
                skipped_old += 1
                continue

            if article_exists(conn, canonical_url):
                skipped_dup += 1
                continue

            save_article(
                conn=conn,
                ticker=ticker,
                title=title,
                source=source,
                published_at=published_at,
                url=url,
                canonical_url=canonical_url,
            )
            saved_count += 1

        total_saved += saved_count
        total_skipped_old += skipped_old
        total_skipped_dup += skipped_dup
        total_skipped_invalid += skipped_invalid

        print(f"  Inserted: {saved_count}")
        print(f"  Skipped old (>48h): {skipped_old}")
        print(f"  Skipped duplicate canonical URLs: {skipped_dup}")
        print(f"  Skipped invalid rows: {skipped_invalid}")

    conn.close()

    print("\nYahoo Finance fetch done.")
    print(f"Total Yahoo articles fetched: {total_fetched}")
    print(f"Total inserted: {total_saved}")
    print(f"Total skipped old: {total_skipped_old}")
    print(f"Total skipped duplicate canonical URLs: {total_skipped_dup}")
    print(f"Total skipped invalid rows: {total_skipped_invalid}")

    if failed_tickers:
        print("Failed tickers:")
        for ticker, error in failed_tickers:
            print(f"  {ticker}: {error}")


if __name__ == "__main__":
    fetch_and_store()
