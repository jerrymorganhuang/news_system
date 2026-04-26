import os
import sqlite3
import urllib.parse
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import feedparser

# 專案根目錄
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")

LOOKBACK_HOURS = 48

# ===== 來源過濾設定 =====
# 先用白名單做主過濾：只保留你覺得值得看的來源
ALLOWED_SOURCES = {
    "Reuters",
    "Bloomberg",
    "CNBC",
    "Financial Times",
    "The Wall Street Journal",
    "Wall Street Journal",
    "Barron's",
    "MarketWatch",
    "Yahoo Finance",
    "Seeking Alpha",
    "Investor's Business Daily",
    "TipRanks",
    "Benzinga",
    "The Information",
    "Associated Press",
    "AP News",
    "Business Wire",
    "GlobeNewswire",
    "PR Newswire",
}

# 黑名單關鍵字：常見垃圾站 / 地區站 / 聚合站
BLOCKED_SOURCE_KEYWORDS = [
    "india",
    "nigeria",
    "kenya",
    "uganda",
    "pakistan",
    "philippines",
    "malaysia",
    "south africa",
    "zambia",
    "ghana",
    "naija",
    "tribune",
    "herald",
    "chronicle",
    "daily times",
    "post",
    "gazette",
    "observer",
    "star",
    "sun",
    "times of india",
    "business today",
    "msn",
    "aol",
    "newsbreak",
    "investing.com",
    "streetinsider",
    "fool",
    "motley fool",
]

# 名稱正規化：避免同一來源出現多種寫法
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
        SELECT
            w.ticker,
            w.company_name,
            COALESCE(
                NULLIF(tsm.google_query, ''),
                NULLIF(w.company_name, ''),
                w.ticker
            ) AS google_query
        FROM watchlist w
        LEFT JOIN ticker_source_map tsm
          ON tsm.ticker = w.ticker
    """)
    return cursor.fetchall()


def build_google_news_rss_url(query):
    encoded_query = urllib.parse.quote(query)
    return f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"


def parse_google_pubdate(pubdate_str):
    """
    Google News RSS 時間例子:
    'Sun, 08 Mar 2026 08:00:00 GMT'
    轉成 SQLite 好比較的 UTC 格式:
    '2026-03-08 08:00:00'
    """
    if not pubdate_str:
        return None

    try:
        dt = parsedate_to_datetime(pubdate_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def is_within_lookback(published_at_str, lookback_hours=48):
    """
    published_at_str 必須是 '%Y-%m-%d %H:%M:%S' 格式，且視為 UTC。
    """
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
    """
    將來源名稱做標準化，避免同一來源出現不同名稱。
    """
    if not source:
        return ""

    source_clean = source.strip()
    source_lower = source_clean.lower()

    for key, normalized in SOURCE_NORMALIZATION.items():
        if source_lower == key or key in source_lower:
            return normalized

    return source_clean


def is_allowed_source(source):
    """
    來源過濾邏輯：
    1. 沒有 source → 不收
    2. 若命中黑名單關鍵字 → 不收
    3. 若在白名單 → 收
    4. 其他一律不收
    """
    if not source:
        return False

    source_norm = normalize_source_name(source)
    source_lower = source_norm.lower()

    for keyword in BLOCKED_SOURCE_KEYWORDS:
        if keyword in source_lower:
            return False

    if source_norm in ALLOWED_SOURCES:
        return True

    return False


def article_exists(conn, ticker, title, url):
    """
    先用 url 去重；若 url 有變但 title 一樣，也避免重複。
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1
        FROM articles
        WHERE url = ?
           OR (ticker = ? AND title = ?)
        LIMIT 1
    """, (url, ticker, title))
    return cursor.fetchone() is not None


def save_article(conn, ticker, title, source, published_at, url):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO articles (
            ticker,
            title,
            source,
            source_type,
            url,
            published_at,
            fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (
        ticker,
        title,
        source,
        "google_news",
        url,
        published_at
    ))
    conn.commit()


def fetch_and_store():
    conn = get_db_connection()
    watchlist = get_watchlist(conn)

    if not watchlist:
        print("No watchlist rows found.")
        conn.close()
        return

    total_saved = 0
    total_skipped_old = 0
    total_skipped_dup = 0
    total_skipped_bad_source = 0
    total_skipped_invalid = 0

    for ticker, company_name, google_query in watchlist:
        print(f"\nFetching news for {ticker} | {company_name}")

        rss_url = build_google_news_rss_url(google_query)
        feed = feedparser.parse(rss_url)

        if not feed.entries:
            print("  No articles found.")
            continue

        saved_count = 0
        skipped_old = 0
        skipped_dup = 0
        skipped_bad_source = 0
        skipped_invalid = 0

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            url = entry.get("link", "").strip()
            raw_published = entry.get("published", "").strip()
            published_at = parse_google_pubdate(raw_published)

            source = ""
            if hasattr(entry, "source") and entry.source:
                source = entry.source.get("title", "").strip()

            source = normalize_source_name(source)

            if not title or not url or not published_at:
                skipped_invalid += 1
                continue

            if not is_allowed_source(source):
                skipped_bad_source += 1
                continue

            if not is_within_lookback(published_at, LOOKBACK_HOURS):
                skipped_old += 1
                continue

            if article_exists(conn, ticker, title, url):
                skipped_dup += 1
                continue

            save_article(
                conn=conn,
                ticker=ticker,
                title=title,
                source=source,
                published_at=published_at,
                url=url
            )
            saved_count += 1

        total_saved += saved_count
        total_skipped_old += skipped_old
        total_skipped_dup += skipped_dup
        total_skipped_bad_source += skipped_bad_source
        total_skipped_invalid += skipped_invalid

        print(f"  Saved: {saved_count}")
        print(f"  Skipped old (>48h): {skipped_old}")
        print(f"  Skipped duplicates: {skipped_dup}")
        print(f"  Skipped bad source: {skipped_bad_source}")
        print(f"  Skipped invalid rows: {skipped_invalid}")

    conn.close()

    print("\nDone.")
    print(f"Total saved: {total_saved}")
    print(f"Total skipped old: {total_skipped_old}")
    print(f"Total skipped duplicates: {total_skipped_dup}")
    print(f"Total skipped bad source: {total_skipped_bad_source}")
    print(f"Total skipped invalid rows: {total_skipped_invalid}")


if __name__ == "__main__":
    fetch_and_store()
