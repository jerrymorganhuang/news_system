import os
import sys
import math
import json
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import yfinance as yf


# ========= Paths =========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")

FETCH_SCRIPT_PATH = os.path.join(BASE_DIR, "app", "fetchers", "google_news.py")
PROCESS_SCRIPT_PATH = os.path.join(BASE_DIR, "app", "processors", "process_articles.py")
SUMMARIZE_SCRIPT_PATH = os.path.join(BASE_DIR, "app", "summarizers", "summarize_by_company.py")
RESET_DB_PATH = os.path.join(BASE_DIR, "app", "db", "reset_news_data.py")
SYNC_SEC_MAPPING_PATH = os.path.join(BASE_DIR, "app", "db", "sync_sec_mapping.py")
SEC_MAPPING_CACHE_PATH = os.path.join(BASE_DIR, "data", "sec_company_tickers.json")

WATCHLIST_CHIPS_PER_ROW = 4


# ========= Page Config =========
st.set_page_config(
    page_title="Company News Intelligence",
    layout="wide"
)


# ========= CSS =========
st.markdown(
    """
    <style>
    /* ===== Main area ===== */
    .block-container {
        padding-top: 2.2rem !important;
        padding-bottom: 0.7rem !important;
    }

    h1 {
        font-size: 2.2rem !important;
        line-height: 1.32 !important;
        margin-top: 0.2rem !important;
        margin-bottom: 0.55rem !important;
        padding-top: 0.1rem !important;
        overflow: visible !important;
    }

    h2 {
        font-size: 1.22rem !important;
        line-height: 1.25 !important;
        margin-top: 0.18rem !important;
        margin-bottom: 0.18rem !important;
    }

    h3 {
        font-size: 1.02rem !important;
        line-height: 1.2 !important;
        margin-top: 0.12rem !important;
        margin-bottom: 0.08rem !important;
    }

    p, li, div {
        font-size: 14px;
    }

    div[data-testid="metric-container"] {
        padding-top: 0.22rem !important;
        padding-bottom: 0.22rem !important;
    }

    div[data-testid="metric-container"] label {
        font-size: 12px !important;
    }

    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 1.06rem !important;
        line-height: 1.08 !important;
    }

    .company-block {
        padding-top: 0.02rem;
        padding-bottom: 0.14rem;
    }

    .company-header {
        margin-bottom: 0.10rem;
        line-height: 1.5;
        word-wrap: break-word;
    }

    .ticker-link {
        color: #163a70;
        text-decoration: underline;
        font-weight: 700;
        font-size: 1.28rem;
        margin-right: 0.52rem;
    }

    .price-main {
        font-size: 1.18rem;
        font-weight: 600;
        color: #1e293b;
        margin-right: 0.18rem;
    }

    .stat-label {
        font-size: 0.96rem;
        color: #475569;
        font-weight: 500;
    }

    .news-count {
        font-size: 0.95rem;
        color: #334155;
        font-weight: 600;
    }

    .sep {
        color: #94a3b8;
        margin-left: 0.18rem;
        margin-right: 0.18rem;
    }

    .pos {
        color: #15803d;
        font-weight: 700;
    }

    .neg {
        color: #dc2626;
        font-weight: 700;
    }

    .neu {
        color: #475569;
        font-weight: 600;
    }

    .summary-label {
        font-size: 0.86rem;
        font-weight: 700;
        color: #1f2937;
        margin-top: 0.02rem;
        margin-bottom: 0.10rem;
    }

    .summary-text {
        font-size: 0.96rem !important;
        line-height: 1.53 !important;
        margin-bottom: 0.08rem !important;
    }

    .generated-time {
        font-size: 0.78rem;
        color: #6b7280;
        margin-top: 0.02rem;
        margin-bottom: 0.08rem;
    }

    .company-divider {
        margin-top: 0.42rem;
        margin-bottom: 0.42rem;
        border: none;
        border-top: 1px solid rgba(100, 116, 139, 0.20);
    }

    table {
        font-size: 12.5px !important;
        width: 100% !important;
        border-collapse: collapse !important;
    }

    th, td {
        padding-top: 5px !important;
        padding-bottom: 5px !important;
        vertical-align: top !important;
    }

    /* ===== Sidebar compact layout ===== */
    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 0.2rem !important;
    }

    section[data-testid="stSidebar"] .block-container {
        padding-top: 0.15rem !important;
        padding-bottom: 0.45rem !important;
        padding-left: 0.65rem !important;
        padding-right: 0.65rem !important;
    }

    section[data-testid="stSidebar"] * {
        font-size: 12px !important;
    }

    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        font-size: 1rem !important;
        margin-top: 0.05rem !important;
        margin-bottom: 0.2rem !important;
    }

    section[data-testid="stSidebar"] p {
        margin-bottom: 0.15rem !important;
    }

    section[data-testid="stSidebar"] .stCaptionContainer {
        margin-top: 0.05rem !important;
        margin-bottom: 0.1rem !important;
    }

    section[data-testid="stSidebar"] hr {
        margin-top: 0.4rem !important;
        margin-bottom: 0.4rem !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div {
        margin-bottom: 0.12rem !important;
    }

    section[data-testid="stSidebar"] .stTextInput > div > div > input {
        height: 30px !important;
        min-height: 30px !important;
        padding-top: 0.1rem !important;
        padding-bottom: 0.1rem !important;
        font-size: 12px !important;
    }

    section[data-testid="stSidebar"] .stSelectbox > div > div {
        min-height: 30px !important;
    }

    section[data-testid="stSidebar"] .stSelectbox {
        margin-bottom: 0.1rem !important;
    }

    section[data-testid="stSidebar"] .stCheckbox {
        margin-top: -0.1rem !important;
        margin-bottom: -0.15rem !important;
    }

    section[data-testid="stSidebar"] .stButton > button {
        height: 30px !important;
        min-height: 30px !important;
        padding: 0.08rem 0.4rem !important;
        font-size: 12px !important;
        border-radius: 8px !important;
    }

    section[data-testid="stSidebar"] .st-key-watchlist_add_ticker_input .stTextInput > div > div > input {
        height: 26px !important;
        min-height: 26px !important;
        padding-top: 0.02rem !important;
        padding-bottom: 0.02rem !important;
        padding-left: 0.38rem !important;
        padding-right: 0.38rem !important;
        font-size: 11px !important;
    }

    section[data-testid="stSidebar"] .st-key-watchlist_add_btn .stButton > button {
        height: 26px !important;
        min-height: 26px !important;
        padding: 0.02rem 0.3rem !important;
        font-size: 11px !important;
        border-radius: 7px !important;
    }

    section[data-testid="stSidebar"] div[data-testid="column"] .stButton > button {
        height: 27px !important;
        min-height: 27px !important;
        padding: 0.02rem 0.3rem !important;
        font-size: 11px !important;
        border-radius: 999px !important;
        border: 1px solid rgba(120, 120, 120, 0.45) !important;
        background: transparent !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }

    section[data-testid="stSidebar"] div[data-testid="column"] .stButton > button:hover {
        border-color: rgba(120, 120, 120, 0.8) !important;
    }

    .watchlist-inline {
        font-size: 12px !important;
        line-height: 1.45 !important;
        color: #334155;
        overflow-wrap: anywhere;
        word-break: break-word;
        margin-bottom: 0.15rem;
    }

    .watchlist-empty-inline {
        font-size: 12px !important;
        color: #64748b;
        margin-bottom: 0.15rem;
    }

    @media (max-width: 768px) {
        section[data-testid="stSidebar"] .block-container {
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True
)


# ========= DB =========
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


# ========= Helpers =========
def utc_now():
    return datetime.now(timezone.utc)


def cutoff_str(hours):
    cutoff = utc_now() - timedelta(hours=hours)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(dt_str):
    if not dt_str:
        return None

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]

    for fmt in fmts:
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue

    return None


def format_time(dt_str):
    dt = parse_dt(dt_str)
    if not dt:
        return dt_str or ""
    return dt.strftime("%Y-%m-%d %H:%M")


def format_digest_updated_time(dt_str):
    dt = parse_dt(dt_str)
    if not dt:
        return ""

    try:
        dt_tpe = dt.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Asia/Taipei"))
    except Exception:
        return ""

    return dt_tpe.strftime("%m/%d %H:%M TPE")


def normalize_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper()


def format_cik_10_digits(raw_cik):
    if raw_cik is None:
        return ""

    cik_digits = "".join(ch for ch in str(raw_cik).strip() if ch.isdigit())
    if not cik_digits:
        return ""
    return cik_digits.zfill(10)


def load_local_sec_mapping():
    if not os.path.exists(SEC_MAPPING_CACHE_PATH):
        return {}

    try:
        with open(SEC_MAPPING_CACHE_PATH, "r", encoding="utf-8") as f:
            mapping_json = json.load(f)
    except Exception:
        return {}

    ticker_map = {}
    if not isinstance(mapping_json, dict):
        return ticker_map

    for item in mapping_json.values():
        if not isinstance(item, dict):
            continue

        ticker = normalize_ticker(item.get("ticker", ""))
        if not ticker:
            continue

        ticker_map[ticker] = {
            "sec_cik": format_cik_10_digits(item.get("cik_str")),
            "sec_company_name": (item.get("title", "") or "").strip(),
        }

    return ticker_map


def autofill_source_map_from_local_cache(cursor, ticker):
    local_mapping = load_local_sec_mapping()
    sec_match = local_mapping.get(ticker)
    if not sec_match:
        return False

    existing = cursor.execute(
        """
        SELECT sec_cik, sec_company_name
        FROM ticker_source_map
        WHERE ticker = ?
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()
    if not existing:
        return False

    existing_sec_cik = (existing[0] or "").strip()
    existing_sec_company_name = (existing[1] or "").strip()

    next_sec_cik = existing_sec_cik or (sec_match.get("sec_cik", "") or "").strip()
    next_sec_company_name = existing_sec_company_name or (sec_match.get("sec_company_name", "") or "").strip()

    if next_sec_cik == existing_sec_cik and next_sec_company_name == existing_sec_company_name:
        return False

    cursor.execute(
        """
        UPDATE ticker_source_map
        SET sec_cik = ?, sec_company_name = ?, updated_at = CURRENT_TIMESTAMP
        WHERE ticker = ?
        """,
        (
            next_sec_cik if next_sec_cik else None,
            next_sec_company_name if next_sec_company_name else None,
            ticker,
        ),
    )
    return True


def safe_text(value):
    return value if value is not None else ""


def pct_change(new_value, old_value):
    try:
        if new_value is None or old_value in (None, 0):
            return None
        return ((new_value / old_value) - 1.0) * 100.0
    except Exception:
        return None


def format_price(value):
    try:
        if value is None or math.isnan(value):
            return "—"
        if value >= 1000:
            return f"{value:,.1f}"
        if value >= 100:
            return f"{value:,.2f}"
        if value >= 1:
            return f"{value:,.2f}"
        return f"{value:,.3f}"
    except Exception:
        return "—"


def format_pct_html(value):
    if value is None:
        return '<span class="neu">—</span>'

    sign = "+" if value > 0 else ""
    cls = "pos" if value > 0 else "neg" if value < 0 else "neu"
    return f'<span class="{cls}">{sign}{value:.1f}%</span>'


def build_company_header_html(ticker, price_html, day_html, after_html, week_html, ytd_html, article_count):
    return (
        '<div class="company-block">'
        '<div class="company-header">'
        f'<span class="ticker-link">{ticker}</span>'
        f'<span class="price-main">{price_html}</span>'
        f'<span class="sep">|</span><span class="stat-label">1D</span> {day_html}'
        f'<span class="sep">|</span><span class="stat-label">Ext</span> {after_html}'
        f'<span class="sep">|</span><span class="stat-label">5D</span> {week_html}'
        f'<span class="sep">|</span><span class="stat-label">YTD</span> {ytd_html}'
        f'<span class="sep">|</span><span class="news-count">News {article_count}</span>'
        '</div>'
        '</div>'
    )


# ========= Watchlist =========
def get_watchlist_rows(conn):
    query = """
        SELECT ticker, company_name, google_query
        FROM watchlist
        ORDER BY ticker
    """
    rows = conn.execute(query).fetchall()

    data = []
    for row in rows:
        data.append({
            "ticker": safe_text(row[0]),
            "company_name": safe_text(row[1]),
            "google_query": safe_text(row[2]),
        })
    return data


def add_watchlist_ticker(conn, ticker):
    ticker = normalize_ticker(ticker)

    if not ticker:
        raise ValueError("Ticker cannot be empty.")

    cursor = conn.cursor()

    existing = cursor.execute(
        "SELECT 1 FROM watchlist WHERE ticker = ? LIMIT 1",
        (ticker,)
    ).fetchone()

    if existing:
        raise ValueError(f"{ticker} already exists in watchlist.")

    cursor.execute(
        """
        INSERT INTO watchlist (ticker, company_name, google_query)
        VALUES (?, ?, ?)
        """,
        (ticker, "", ticker)
    )

    existing_source_map = cursor.execute(
        "SELECT 1 FROM ticker_source_map WHERE ticker = ? LIMIT 1",
        (ticker,),
    ).fetchone()
    if not existing_source_map:
        cursor.execute(
            """
            INSERT INTO ticker_source_map (
                ticker,
                google_query,
                sec_cik,
                sec_company_name,
                updated_at
            )
            VALUES (?, ?, NULL, NULL, CURRENT_TIMESTAMP)
            """,
            (ticker, ticker),
        )

    autofill_source_map_from_local_cache(cursor, ticker)
    conn.commit()


def delete_watchlist_tickers(conn, tickers):
    normalized_tickers = [normalize_ticker(ticker) for ticker in tickers]
    normalized_tickers = [ticker for ticker in normalized_tickers if ticker]
    if not normalized_tickers:
        return 0

    cursor = conn.cursor()
    cursor.executemany("DELETE FROM watchlist WHERE ticker = ?", [(ticker,) for ticker in normalized_tickers])
    cursor.executemany("DELETE FROM ticker_source_map WHERE ticker = ?", [(ticker,) for ticker in normalized_tickers])
    conn.commit()
    return len(normalized_tickers)


def remove_watchlist_ticker(conn, ticker):
    ticker = normalize_ticker(ticker)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker,))
    cursor.execute("DELETE FROM ticker_source_map WHERE ticker = ?", (ticker,))
    conn.commit()


def ensure_ticker_source_map_schema(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_source_map (
            ticker TEXT PRIMARY KEY,
            google_query TEXT,
            sec_cik TEXT,
            sec_company_name TEXT,
            updated_at TEXT
        )
        """
    )

    cursor.execute(
        """
        INSERT INTO ticker_source_map (
            ticker,
            google_query,
            sec_cik,
            sec_company_name,
            updated_at
        )
        SELECT
            w.ticker,
            COALESCE(NULLIF(w.google_query, ''), w.ticker) AS google_query,
            NULLIF(w.sec_cik, '') AS sec_cik,
            NULL AS sec_company_name,
            CURRENT_TIMESTAMP
        FROM watchlist w
        WHERE NOT EXISTS (
            SELECT 1
            FROM ticker_source_map tsm
            WHERE tsm.ticker = w.ticker
        )
        """
    )
    conn.commit()


def get_source_map_rows(conn):
    table_columns = set()
    try:
        pragma_rows = conn.execute("PRAGMA table_info(ticker_source_map)").fetchall()
        table_columns = {row[1] for row in pragma_rows}
    except sqlite3.Error:
        table_columns = set()

    updated_at_select = "tsm.updated_at" if "updated_at" in table_columns else "NULL"

    query = """
        SELECT
            w.ticker,
            COALESCE(NULLIF(tsm.google_query, ''), w.ticker) AS google_query,
            COALESCE(tsm.sec_cik, '') AS sec_cik,
            COALESCE(tsm.sec_company_name, '') AS sec_company_name,
            {updated_at_select} AS updated_at
        FROM watchlist w
        LEFT JOIN ticker_source_map tsm
          ON tsm.ticker = w.ticker
        ORDER BY w.ticker
    """.format(updated_at_select=updated_at_select)
    rows = conn.execute(query).fetchall()
    return pd.DataFrame(
        [
            {
                "ticker": safe_text(row[0]),
                "google_query": safe_text(row[1]),
                "sec_cik": safe_text(row[2]),
                "sec_company_name": safe_text(row[3]),
                "updated_at": safe_text(row[4]),
            }
            for row in rows
        ]
    )


def save_source_map_rows(conn, source_map_df):
    cursor = conn.cursor()
    now_str = utc_now().strftime("%Y-%m-%d %H:%M:%S")

    existing_rows = cursor.execute(
        """
        SELECT ticker, google_query, sec_cik, sec_company_name
        FROM ticker_source_map
        """
    ).fetchall()
    existing_by_ticker = {
        normalize_ticker(row[0]): {
            "google_query": (row[1] or "").strip(),
            "sec_cik": (row[2] or "").strip(),
            "sec_company_name": (row[3] or "").strip(),
        }
        for row in existing_rows
        if normalize_ticker(row[0])
    }

    for _, row in source_map_df.iterrows():
        ticker = normalize_ticker(row.get("ticker", ""))
        if not ticker:
            continue

        google_query = (row.get("google_query", "") or "").strip() or ticker
        sec_cik = (row.get("sec_cik", "") or "").strip()
        sec_company_name = (row.get("sec_company_name", "") or "").strip()

        existing = existing_by_ticker.get(ticker)
        existing_google_query = (existing.get("google_query") or ticker) if existing else None
        existing_sec_cik = existing.get("sec_cik", "") if existing else None
        existing_sec_company_name = existing.get("sec_company_name", "") if existing else None

        row_changed = (
            existing is None
            or google_query != existing_google_query
            or sec_cik != existing_sec_cik
            or sec_company_name != existing_sec_company_name
        )

        if not row_changed:
            continue

        cursor.execute(
            """
            INSERT INTO ticker_source_map (
                ticker,
                google_query,
                sec_cik,
                sec_company_name,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                google_query = excluded.google_query,
                sec_cik = excluded.sec_cik,
                sec_company_name = excluded.sec_company_name,
                updated_at = excluded.updated_at
            """,
            (
                ticker,
                google_query,
                sec_cik if sec_cik else None,
                sec_company_name if sec_company_name else None,
                now_str,
            )
        )

    conn.commit()


# ========= Digest / Articles =========
def ensure_company_digest_schema(conn):
    cursor = conn.cursor()
    table_exists = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_digest' LIMIT 1"
    ).fetchone()

    if not table_exists:
        cursor.execute(
            """
            CREATE TABLE company_digest (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                window_hours INTEGER NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                article_count INTEGER DEFAULT 0,
                summary TEXT,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, window_hours)
            )
            """
        )
        conn.commit()
        return

    cursor.execute("PRAGMA table_info(company_digest)")
    columns = {row[1] for row in cursor.fetchall()}
    if "window_hours" not in columns:
        cursor.execute("ALTER TABLE company_digest RENAME TO company_digest_legacy")
        cursor.execute(
            """
            CREATE TABLE company_digest (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                window_hours INTEGER NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                article_count INTEGER DEFAULT 0,
                summary TEXT,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, window_hours)
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO company_digest (
                ticker,
                window_hours,
                window_start,
                window_end,
                article_count,
                summary,
                generated_at
            )
            SELECT
                ticker,
                48 AS window_hours,
                COALESCE(window_start, datetime('now', '-48 hours')),
                COALESCE(window_end, datetime('now')),
                COALESCE(article_count, 0),
                summary,
                COALESCE(generated_at, CURRENT_TIMESTAMP)
            FROM company_digest_legacy
            """
        )
        cursor.execute("DROP TABLE company_digest_legacy")

    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_digest_ticker_window "
        "ON company_digest(ticker, window_hours)"
    )
    conn.commit()


def get_company_digest(conn, ticker, window_hours):
    candidate_queries = [
        """
        SELECT summary, generated_at
        FROM company_digest
        WHERE ticker = ?
          AND window_hours = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        """
        SELECT digest, generated_at
        FROM company_digest
        WHERE ticker = ?
          AND window_hours = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        """
        SELECT ai_summary, generated_at
        FROM company_digest
        WHERE ticker = ?
          AND window_hours = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """
    ]

    for query in candidate_queries:
        try:
            row = conn.execute(query, (ticker, window_hours)).fetchone()
            if row:
                return {
                    "summary": row[0] or "",
                    "generated_at": row[1] or ""
                }
        except Exception:
            continue

    return {
        "summary": "",
        "generated_at": ""
    }


def get_article_count(conn, ticker, lookback_hours):
    query = """
        SELECT COUNT(*)
        FROM articles
        WHERE ticker = ?
          AND datetime(COALESCE(published_at, fetched_at)) >= datetime(?)
    """
    row = conn.execute(query, (ticker, cutoff_str(lookback_hours))).fetchone()
    return row[0] if row else 0


def get_articles(conn, ticker, lookback_hours):
    query = """
        SELECT
            ticker,
            COALESCE(published_at, fetched_at) as article_time,
            source,
            title,
            url
        FROM articles
        WHERE ticker = ?
          AND datetime(COALESCE(published_at, fetched_at)) >= datetime(?)
        ORDER BY datetime(COALESCE(published_at, fetched_at)) DESC
    """
    rows = conn.execute(query, (ticker, cutoff_str(lookback_hours))).fetchall()

    data = []
    for row in rows:
        data.append({
            "ticker": row[0],
            "published_at": format_time(row[1]),
            "source": row[2] or "",
            "title": row[3] or "",
            "url": row[4] or ""
        })

    return pd.DataFrame(data)


# ========= Market Data =========
@st.cache_data(ttl=300, show_spinner=False)
def get_market_snapshot(ticker):
    try:
        tk = yf.Ticker(ticker)
        hist_1y = tk.history(period="1y", auto_adjust=False, prepost=True)

        if hist_1y is None or hist_1y.empty:
            return {
                "price": None,
                "day_pct": None,
                "after_pct": None,
                "week_pct": None,
                "ytd_pct": None,
            }

        hist_1y = hist_1y.dropna(subset=["Close"])
        if hist_1y.empty:
            return {
                "price": None,
                "day_pct": None,
                "after_pct": None,
                "week_pct": None,
                "ytd_pct": None,
            }

        closes = hist_1y["Close"].tolist()
        price = closes[-1] if len(closes) >= 1 else None
        prev_close = closes[-2] if len(closes) >= 2 else None
        week_close = closes[-6] if len(closes) >= 6 else (closes[0] if closes else None)

        info = {}
        try:
            info = tk.fast_info or {}
        except Exception:
            info = {}

        previous_close_info = info.get("previous_close", None)
        last_price_info = info.get("last_price", None)

        day_base = previous_close_info if previous_close_info not in (None, 0) else prev_close
        day_price = last_price_info if last_price_info not in (None, 0) else price
        day_pct = pct_change(day_price, day_base)

        regular_market = None
        post_market = None
        pre_market = None
        post_market_pct = None
        pre_market_pct = None

        try:
            regular_market = info.get("regular_market_price", None)
        except Exception:
            regular_market = None

        try:
            post_market = info.get("post_market_price", None)
        except Exception:
            post_market = None

        try:
            pre_market = info.get("pre_market_price", None)
        except Exception:
            pre_market = None

        if post_market in (None, 0) and pre_market in (None, 0):
            info_ext = {}
            try:
                info_ext = tk.info or {}
            except Exception:
                info_ext = {}

            post_market = info_ext.get("postMarketPrice", None)
            post_market_pct = info_ext.get("postMarketChangePercent", None)
            pre_market = info_ext.get("preMarketPrice", None)
            pre_market_pct = info_ext.get("preMarketChangePercent", None)

        after_pct = None
        if post_market not in (None, 0):
            if post_market_pct is not None:
                after_pct = post_market_pct
            elif regular_market not in (None, 0):
                after_pct = pct_change(post_market, regular_market)
        elif pre_market not in (None, 0):
            if pre_market_pct is not None:
                after_pct = pre_market_pct
            elif regular_market not in (None, 0):
                after_pct = pct_change(pre_market, regular_market)

        week_pct = pct_change(price, week_close)

        dt_index = hist_1y.index
        current_year = datetime.utcnow().year
        ytd_base = None

        for idx, dt_val in enumerate(dt_index):
            py_dt = dt_val.to_pydatetime() if hasattr(dt_val, "to_pydatetime") else dt_val
            if py_dt.year == current_year:
                ytd_base = closes[idx]
                break

        if ytd_base is None and len(closes) > 0:
            ytd_base = closes[0]

        ytd_pct = pct_change(price, ytd_base)

        return {
            "price": day_price if day_price is not None else price,
            "day_pct": day_pct,
            "after_pct": after_pct,
            "week_pct": week_pct,
            "ytd_pct": ytd_pct,
        }

    except Exception:
        return {
            "price": None,
            "day_pct": None,
            "after_pct": None,
            "week_pct": None,
            "ytd_pct": None,
        }


def build_dashboard_rows(conn, window_hours):
    watchlist_rows = get_watchlist_rows(conn)
    results = []

    for item in watchlist_rows:
        ticker = item["ticker"]
        company_name = item["company_name"] or ticker
        digest = get_company_digest(conn, ticker, window_hours)
        article_count = get_article_count(conn, ticker, window_hours)
        articles_df = get_articles(conn, ticker, window_hours)
        market = get_market_snapshot(ticker)

        results.append({
            "ticker": ticker,
            "company_name": company_name,
            "summary": digest["summary"],
            "generated_at": digest["generated_at"],
            "article_count": article_count,
            "articles_df": articles_df,
            "has_data": bool(digest["summary"]) or article_count > 0,
            "price": market["price"],
            "day_pct": market["day_pct"],
            "after_pct": market["after_pct"],
            "week_pct": market["week_pct"],
            "ytd_pct": market["ytd_pct"],
        })

    return results


# ========= Script Runner =========
def run_python_script(script_path, args=None):
    if not os.path.exists(script_path):
        return False, f"Script not found: {script_path}"

    try:
        cmd = [sys.executable, script_path]
        if args:
            cmd.extend(args)
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True
        )

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if result.returncode == 0:
            return True, stdout if stdout else "Completed successfully."

        error_msg = stderr if stderr else stdout if stdout else "Unknown error."
        return False, error_msg

    except Exception as e:
        return False, str(e)


def run_pipeline_with_progress():
    steps = [
        ("Step 1/3 - Fetching news", FETCH_SCRIPT_PATH),
        ("Step 2/3 - Processing articles", PROCESS_SCRIPT_PATH),
        ("Step 3/3 - Generating AI summaries", SUMMARIZE_SCRIPT_PATH, ["--window-hours", "24"]),
    ]

    progress_placeholder = st.sidebar.empty()
    status_placeholder = st.sidebar.empty()
    log_placeholder = st.sidebar.empty()

    progress_bar = progress_placeholder.progress(0, text="Starting pipeline...")
    all_logs = []

    for idx, step in enumerate(steps, start=1):
        if len(step) == 3:
            label, script_path, script_args = step
        else:
            label, script_path = step
            script_args = None
        pct_before = int(((idx - 1) / len(steps)) * 100)
        progress_bar.progress(pct_before, text=label)
        status_placeholder.info(label)

        ok, msg = run_python_script(script_path, args=script_args)

        all_logs.append(f"===== {label} =====")
        all_logs.append(msg)
        all_logs.append("")

        log_placeholder.text_area(
            "Pipeline log",
            "\n".join(all_logs),
            height=220
        )

        if not ok:
            progress_bar.progress(pct_before, text=f"Failed at: {label}")
            status_placeholder.error(f"Failed: {label}")
            st.session_state["pipeline_log"] = "\n".join(all_logs)
            st.session_state["pipeline_status"] = "failed"
            return False

        pct_after = int((idx / len(steps)) * 100)
        progress_bar.progress(pct_after, text=label)

    progress_bar.progress(100, text="Pipeline completed")
    status_placeholder.success("Pipeline completed.")
    st.session_state["pipeline_log"] = "\n".join(all_logs)
    st.session_state["pipeline_status"] = "success"
    return True


# ========= Session State =========
if "pipeline_log" not in st.session_state:
    st.session_state["pipeline_log"] = ""

if "pipeline_status" not in st.session_state:
    st.session_state["pipeline_status"] = ""

if "page_mode" not in st.session_state:
    st.session_state["page_mode"] = "Dashboard"

if "sec_mapping_notice" not in st.session_state:
    st.session_state["sec_mapping_notice"] = None


# ========= Page =========
if not os.path.exists(DB_PATH):
    st.error(f"Database not found: {DB_PATH}")
    st.stop()


# ========= App =========
conn = get_connection()

try:
    ensure_company_digest_schema(conn)
    ensure_ticker_source_map_schema(conn)

    watchlist_rows = get_watchlist_rows(conn)
    watchlist_tickers = sorted([row["ticker"] for row in watchlist_rows])

    # ----- Sidebar -----
    hide_empty = st.sidebar.checkbox(
        "Hide tickers with no data",
        value=False
    )

    window_label = st.sidebar.selectbox(
        "News Window",
        options=["24h", "48h"],
        index=0,
        label_visibility="collapsed",
    )
    selected_window_hours = int(window_label.replace("h", ""))

    sort_option = st.sidebar.selectbox(
        "Sort",
        ["Sort by Ticker A-Z", "Sort by News count (high to low)"],
        index=0,
        label_visibility="collapsed",
    )

    st.sidebar.divider()

    action_col1, action_col2, action_col3 = st.sidebar.columns(3, gap="small")

    with action_col1:
        run_clicked = st.button("Run", use_container_width=True)
    with action_col2:
        refresh_clicked = st.button("Refresh", use_container_width=True)
    with action_col3:
        reset_clicked = st.button("Reset", use_container_width=True)

    if run_clicked:
        success = run_pipeline_with_progress()
        if success:
            st.rerun()

    if refresh_clicked:
        st.rerun()

    confirm_reset = st.sidebar.checkbox(
        "I understand this will clear all news data",
        value=False,
        key="confirm_reset_database"
    )

    if reset_clicked:
        if not confirm_reset:
            st.sidebar.warning("Please confirm reset first.")
        else:
            ok, msg = run_python_script(RESET_DB_PATH)
            st.session_state["pipeline_log"] = msg
            st.session_state["pipeline_status"] = "success" if ok else "failed"

            if ok:
                st.rerun()
            else:
                st.sidebar.error("Database reset failed.")

    st.sidebar.divider()

    st.sidebar.markdown("### Watchlist")

    add_col1, add_col2 = st.sidebar.columns([4, 1.2], gap="small")
    with add_col1:
        new_ticker = st.text_input(
            "Add ticker",
            value="",
            placeholder="NVDA",
            label_visibility="collapsed",
            key="watchlist_add_ticker_input"
        )
    with add_col2:
        add_clicked = st.button("Add", use_container_width=True, key="watchlist_add_btn")

    if add_clicked:
        try:
            add_watchlist_ticker(conn, new_ticker)
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

    if watchlist_tickers:
        st.sidebar.caption(f"Watchlist ({len(watchlist_tickers)})")
        watchlist_inline = " · ".join(watchlist_tickers)
        st.sidebar.markdown(
            f'<div class="watchlist-inline">{watchlist_inline}</div>',
            unsafe_allow_html=True
        )
    else:
        st.sidebar.caption("Watchlist (0)")
        st.sidebar.markdown(
            '<div class="watchlist-empty-inline">No tickers yet.</div>',
            unsafe_allow_html=True
        )

    st.sidebar.divider()

    page_mode = st.sidebar.selectbox(
        "Page",
        options=["Dashboard", "Ticker Admin"],
        key="page_mode",
    )

    if st.session_state["pipeline_log"]:
        with st.sidebar.expander("Last run log", expanded=False):
            st.text_area(
                "Log output",
                st.session_state["pipeline_log"],
                height=220,
                label_visibility="collapsed"
            )

    if page_mode == "Ticker Admin":
        st.title("Ticker Admin")

        sec_notice = st.session_state.get("sec_mapping_notice")
        if sec_notice:
            notice_type = sec_notice.get("type", "info")
            notice_text = sec_notice.get("text", "")
            if notice_type == "success":
                st.success(notice_text)
            elif notice_type == "error":
                st.error(notice_text)
            else:
                st.info(notice_text)
            st.session_state["sec_mapping_notice"] = None

        toolbar_col1, toolbar_col2, toolbar_col3, toolbar_col4, toolbar_col5 = st.columns(
            [2.2, 0.8, 1.2, 1.3, 1.5], gap="small"
        )
        with toolbar_col1:
            admin_new_ticker = st.text_input(
                "Add ticker (Ticker Admin)",
                value="",
                placeholder="NVDA",
                label_visibility="collapsed",
                key="ticker_admin_add_ticker_input",
            )
        with toolbar_col2:
            admin_add_clicked = st.button("Add", use_container_width=True, key="ticker_admin_add_btn")
        with toolbar_col3:
            save_clicked = st.button("Save Changes", use_container_width=True, key="ticker_admin_save_btn")
        with toolbar_col4:
            delete_clicked = st.button("Delete Selected", use_container_width=True, key="ticker_admin_delete_selected_btn")
        with toolbar_col5:
            refresh_sec_mapping_clicked = st.button(
                "Refresh SEC",
                use_container_width=True,
                key="ticker_admin_refresh_sec_mapping_btn",
            )

        if refresh_sec_mapping_clicked:
            ok, msg = run_python_script(SYNC_SEC_MAPPING_PATH)
            if ok:
                if "no rows changed" in (msg or "").lower():
                    st.session_state["sec_mapping_notice"] = {"type": "info", "text": msg}
                else:
                    st.session_state["sec_mapping_notice"] = {"type": "success", "text": msg}
            else:
                st.session_state["sec_mapping_notice"] = {"type": "error", "text": msg}
            st.rerun()

        if admin_add_clicked:
            try:
                add_watchlist_ticker(conn, admin_new_ticker)
                st.rerun()
            except Exception as e:
                st.error(str(e))

        source_map_df = get_source_map_rows(conn)
        if source_map_df.empty:
            st.info("No watchlist tickers available.")
        else:
            source_map_df["updated_at"] = source_map_df["updated_at"].apply(format_time)
            source_map_df["delete"] = False
            source_map_df = source_map_df[["delete", "ticker", "google_query", "sec_cik", "sec_company_name", "updated_at"]]

            edited_df = st.data_editor(
                source_map_df,
                use_container_width=True,
                hide_index=True,
                disabled=["ticker", "updated_at"],
                column_config={
                    "delete": st.column_config.CheckboxColumn("delete", width="small"),
                    "ticker": st.column_config.TextColumn("ticker", width="small"),
                    "google_query": st.column_config.TextColumn("google_query", width="medium"),
                    "sec_cik": st.column_config.TextColumn("sec_cik", width=90),
                    "sec_company_name": st.column_config.TextColumn("sec_company_name", width="medium"),
                    "updated_at": st.column_config.TextColumn("updated_at", width=160),
                },
                key="ticker_admin_data_editor",
            )

            if save_clicked:
                save_df = edited_df[["ticker", "google_query", "sec_cik", "sec_company_name", "updated_at"]].copy()
                save_source_map_rows(conn, save_df)
                st.success("Source mapping saved.")
                st.rerun()

            if delete_clicked:
                selected_tickers = edited_df.loc[edited_df["delete"] == True, "ticker"].tolist()
                if not selected_tickers:
                    st.warning("No tickers selected for deletion.")
                else:
                    deleted_count = delete_watchlist_tickers(conn, selected_tickers)
                    st.success(f"Deleted {deleted_count} ticker(s) from watchlist and ticker mappings.")
                    st.rerun()
    else:
        st.title(f"{selected_window_hours}h Company News Dashboard")

        if watchlist_tickers:
            with st.spinner(f"Ensuring {selected_window_hours}h digests..."):
                run_python_script(
                    SUMMARIZE_SCRIPT_PATH, args=["--window-hours", str(selected_window_hours)]
                )

        # ----- Main -----
        dashboard_rows = build_dashboard_rows(conn, selected_window_hours)

        if hide_empty:
            dashboard_rows = [row for row in dashboard_rows if row["has_data"]]

        if sort_option == "Sort by Ticker A-Z":
            dashboard_rows = sorted(dashboard_rows, key=lambda x: x["ticker"])
        else:
            dashboard_rows = sorted(
                dashboard_rows,
                key=lambda x: (-x["article_count"], x["ticker"])
            )

        if not dashboard_rows:
            st.info("No rows to display.")
            st.stop()

        overview_col1, overview_col2, overview_col3 = st.columns(3)
        overview_col1.metric("Displayed tickers", len(dashboard_rows))
        overview_col2.metric("Tickers with data", sum(1 for row in dashboard_rows if row["has_data"]))
        overview_col3.metric(
            f"Articles ({selected_window_hours}h)",
            sum(row["article_count"] for row in dashboard_rows),
        )

        st.markdown("---")

        for row in dashboard_rows:
            ticker = row["ticker"]
            summary = row["summary"]
            generated_at = row["generated_at"]
            article_count = row["article_count"]
            articles_df = row["articles_df"]

            price_html = format_price(row["price"])
            day_html = format_pct_html(row["day_pct"])
            after_html = format_pct_html(row["after_pct"])
            week_html = format_pct_html(row["week_pct"])
            ytd_html = format_pct_html(row["ytd_pct"])

            header_html = build_company_header_html(
                ticker=ticker,
                price_html=price_html,
                day_html=day_html,
                after_html=after_html,
                week_html=week_html,
                ytd_html=ytd_html,
                article_count=article_count,
            )
            st.markdown(header_html, unsafe_allow_html=True)

            summary_label = f"AI Summary · {selected_window_hours}h"
            if generated_at:
                updated_label = format_digest_updated_time(generated_at)
                if updated_label:
                    summary_label = f"AI Summary · {selected_window_hours}h · Updated: {updated_label}"

            st.markdown(
                f'<div class="summary-label">{summary_label}</div>',
                unsafe_allow_html=True
            )

            if summary:
                st.markdown(
                    f'<div class="summary-text">{summary}</div>',
                    unsafe_allow_html=True
                )
            else:
                st.write("")

            with st.expander(f"Articles ({article_count})", expanded=False):
                if articles_df.empty:
                    st.write("")
                else:
                    display_df = articles_df.copy()
                    display_df["link"] = display_df["url"].apply(
                        lambda x: f'<a href="{x}" target="_blank">Open</a>' if x else ""
                    )
                    display_df = display_df[["published_at", "source", "title", "link"]]

                    st.markdown(
                        display_df.to_html(escape=False, index=False),
                        unsafe_allow_html=True
                    )

            st.markdown('<hr class="company-divider">', unsafe_allow_html=True)

finally:
    conn.close()
