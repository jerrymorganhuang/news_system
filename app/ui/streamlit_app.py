import os
import sys
import math
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta

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

LOOKBACK_HOURS = 48
WATCHLIST_CHIPS_PER_ROW = 4


# ========= Page Config =========
st.set_page_config(
    page_title="48h Company News Intelligence",
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


def cutoff_str(hours=48):
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


def normalize_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper()


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
    conn.commit()


def remove_watchlist_ticker(conn, ticker):
    ticker = normalize_ticker(ticker)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker,))
    conn.commit()


# ========= Digest / Articles =========
def get_company_digest(conn, ticker):
    candidate_queries = [
        """
        SELECT summary, generated_at
        FROM company_digest
        WHERE ticker = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        """
        SELECT digest, generated_at
        FROM company_digest
        WHERE ticker = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        """
        SELECT ai_summary, generated_at
        FROM company_digest
        WHERE ticker = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """
    ]

    for query in candidate_queries:
        try:
            row = conn.execute(query, (ticker,)).fetchone()
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


def get_article_count_48h(conn, ticker):
    query = """
        SELECT COUNT(*)
        FROM articles
        WHERE ticker = ?
          AND published_at >= ?
    """
    row = conn.execute(query, (ticker, cutoff_str(LOOKBACK_HOURS))).fetchone()
    return row[0] if row else 0


def get_articles_48h(conn, ticker):
    query = """
        SELECT
            ticker,
            published_at,
            source,
            title,
            url
        FROM articles
        WHERE ticker = ?
          AND published_at >= ?
        ORDER BY published_at DESC
    """
    rows = conn.execute(query, (ticker, cutoff_str(LOOKBACK_HOURS))).fetchall()

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

        after_pct = None
        if post_market not in (None, 0) and regular_market not in (None, 0):
            after_pct = pct_change(post_market, regular_market)
        elif pre_market not in (None, 0) and regular_market not in (None, 0):
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


def build_dashboard_rows(conn):
    watchlist_rows = get_watchlist_rows(conn)
    results = []

    for item in watchlist_rows:
        ticker = item["ticker"]
        company_name = item["company_name"] or ticker
        digest = get_company_digest(conn, ticker)
        article_count = get_article_count_48h(conn, ticker)
        articles_df = get_articles_48h(conn, ticker)
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
def run_python_script(script_path):
    if not os.path.exists(script_path):
        return False, f"Script not found: {script_path}"

    try:
        result = subprocess.run(
            [sys.executable, script_path],
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
        ("Step 3/3 - Generating AI summaries", SUMMARIZE_SCRIPT_PATH),
    ]

    progress_placeholder = st.sidebar.empty()
    status_placeholder = st.sidebar.empty()
    log_placeholder = st.sidebar.empty()

    progress_bar = progress_placeholder.progress(0, text="Starting pipeline...")
    all_logs = []

    for idx, (label, script_path) in enumerate(steps, start=1):
        pct_before = int(((idx - 1) / len(steps)) * 100)
        progress_bar.progress(pct_before, text=label)
        status_placeholder.info(label)

        ok, msg = run_python_script(script_path)

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
if "pending_delete_ticker" not in st.session_state:
    st.session_state["pending_delete_ticker"] = None

if "pipeline_log" not in st.session_state:
    st.session_state["pipeline_log"] = ""

if "pipeline_status" not in st.session_state:
    st.session_state["pipeline_status"] = ""


# ========= Delete Dialog =========
@st.dialog("Confirm delete")
def confirm_delete_dialog(ticker):
    st.write(f"Remove **{ticker}** from watchlist?")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Delete", key=f"confirm_delete_{ticker}", use_container_width=True):
            conn = get_connection()
            try:
                remove_watchlist_ticker(conn, ticker)
            finally:
                conn.close()

            st.session_state["pending_delete_ticker"] = None
            st.rerun()

    with col2:
        if st.button("Cancel", key=f"cancel_delete_{ticker}", use_container_width=True):
            st.session_state["pending_delete_ticker"] = None
            st.rerun()


# ========= Page =========
st.title("48h Company News Intelligence Dashboard")
st.caption("Rolling 48-hour company news dashboard with AI-generated company digests")

if not os.path.exists(DB_PATH):
    st.error(f"Database not found: {DB_PATH}")
    st.stop()


# ========= App =========
conn = get_connection()

try:
    watchlist_rows = get_watchlist_rows(conn)
    watchlist_tickers = sorted([row["ticker"] for row in watchlist_rows])

    # ----- Sidebar -----
    st.sidebar.markdown("### Watchlist")

    add_col1, add_col2 = st.sidebar.columns([4, 1.2], gap="small")
    with add_col1:
        new_ticker = st.text_input(
            "Add ticker",
            value="",
            placeholder="NVDA",
            label_visibility="collapsed"
        )
    with add_col2:
        add_clicked = st.button("Add", use_container_width=True)

    if add_clicked:
        try:
            add_watchlist_ticker(conn, new_ticker)
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

    if watchlist_tickers:
        st.sidebar.caption("Current watchlist")

        for i in range(0, len(watchlist_tickers), WATCHLIST_CHIPS_PER_ROW):
            row_tickers = watchlist_tickers[i:i + WATCHLIST_CHIPS_PER_ROW]
            cols = st.sidebar.columns(WATCHLIST_CHIPS_PER_ROW, gap="small")

            for j in range(WATCHLIST_CHIPS_PER_ROW):
                with cols[j]:
                    if j < len(row_tickers):
                        ticker = row_tickers[j]
                        if st.button(
                            f"× {ticker}",
                            key=f"delete_btn_{ticker}",
                            use_container_width=True
                        ):
                            st.session_state["pending_delete_ticker"] = ticker
                    else:
                        st.write("")
    else:
        st.sidebar.info("Watchlist is empty.")

    st.sidebar.divider()

    st.sidebar.caption("Display")

    hide_empty = st.sidebar.checkbox(
        "Hide tickers with no data",
        value=False
    )

    sort_option = st.sidebar.selectbox(
        "Sort by",
        ["Ticker A-Z", "News count (high to low)"],
        index=0
    )

    st.sidebar.divider()

    st.sidebar.caption("Actions")

    if st.sidebar.button("Run pipeline", use_container_width=True):
        success = run_pipeline_with_progress()
        if success:
            st.rerun()

    if st.sidebar.button("Refresh page", use_container_width=True):
        st.rerun()

    confirm_reset = st.sidebar.checkbox(
        "I understand this will clear all news data",
        value=False,
        key="confirm_reset_database"
    )

    if st.sidebar.button("Reset database", use_container_width=True):
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

    if st.session_state["pipeline_log"]:
        with st.sidebar.expander("Last run log", expanded=False):
            st.text_area(
                "Log output",
                st.session_state["pipeline_log"],
                height=220,
                label_visibility="collapsed"
            )

    if st.session_state["pending_delete_ticker"]:
        confirm_delete_dialog(st.session_state["pending_delete_ticker"])

    # ----- Main -----
    dashboard_rows = build_dashboard_rows(conn)

    if hide_empty:
        dashboard_rows = [row for row in dashboard_rows if row["has_data"]]

    if sort_option == "Ticker A-Z":
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
    overview_col3.metric("Articles (48h)", sum(row["article_count"] for row in dashboard_rows))

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

        st.markdown('<div class="summary-label">AI Summary</div>', unsafe_allow_html=True)

        if summary:
            st.markdown(
                f'<div class="summary-text">{summary}</div>',
                unsafe_allow_html=True
            )
            if generated_at:
                st.markdown(
                    f'<div class="generated-time">Generated at: {format_time(generated_at)} UTC</div>',
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