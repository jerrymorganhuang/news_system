from datetime import date, datetime, timezone
import ast
import sqlite3
import sys
import types
from pathlib import Path

from app.db.company_digest_schema import available_report_dates, daily_window, ensure_company_digest_schema, utc_sql_timestamp
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *_: None))
sys.modules.setdefault("openai", types.SimpleNamespace(OpenAI=object))

from app.summarizers.summarize_by_company import get_articles_for_ticker, save_company_digest


def articles_schema(conn):
    conn.execute("""CREATE TABLE articles (ticker TEXT, title TEXT, source TEXT,
        published_at TEXT, fetched_at TEXT, content TEXT, url TEXT)""")


def test_daily_window_is_fixed_taipei_six_am():
    start, end = daily_window(date(2026, 8, 25))
    assert start == datetime(2026, 8, 23, 22, tzinfo=timezone.utc)
    assert end == datetime(2026, 8, 24, 22, tzinfo=timezone.utc)
    assert end - start == __import__("datetime").timedelta(days=1)


def test_article_bounds_are_inclusive_exclusive():
    conn = sqlite3.connect(":memory:")
    articles_schema(conn)
    conn.executemany("INSERT INTO articles VALUES ('AAA', ?, '', ?, NULL, '', '')", [
        ("before", "2026-08-23 21:59:59"), ("start", "2026-08-23 22:00:00"),
        ("inside", "2026-08-24 21:59:59"), ("end", "2026-08-24 22:00:00")])
    rows = get_articles_for_ticker(conn, "AAA", "2026-08-23 22:00:00", "2026-08-24 22:00:00")
    assert {row[0] for row in rows} == {"start", "inside"}


def test_snapshots_upsert_only_same_date():
    conn = sqlite3.connect(":memory:")
    ensure_company_digest_schema(conn)
    for report, summary in [(date(2026, 8, 24), "old"), (date(2026, 8, 25), "first")]:
        start, end = daily_window(report)
        save_company_digest(conn, report, "AAA", utc_sql_timestamp(start), utc_sql_timestamp(end), summary, 1)
    start, end = daily_window(date(2026, 8, 25))
    save_company_digest(conn, date(2026, 8, 25), "AAA", utc_sql_timestamp(start), utc_sql_timestamp(end), "updated", 2)
    assert conn.execute("SELECT report_date, summary FROM company_digest ORDER BY report_date").fetchall() == [
        ("2026-08-24", "old"), ("2026-08-25", "updated")]
    assert available_report_dates(conn) == ["2026-08-25", "2026-08-24"]


def test_legacy_24h_migration_is_idempotent_and_visible():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE company_digest (id INTEGER PRIMARY KEY, ticker TEXT,
        window_hours INTEGER, window_start TEXT, window_end TEXT, article_count INTEGER,
        summary TEXT, generated_at TEXT, UNIQUE(ticker, window_hours))""")
    conn.execute("INSERT INTO company_digest VALUES (1, 'AAA', 24, 'rolling', '2026-08-25 01:00:00', 3, 'kept', '2026-08-25 01:05:00')")
    ensure_company_digest_schema(conn)
    ensure_company_digest_schema(conn)
    row = conn.execute("SELECT report_date, ticker, summary, window_start, window_end FROM company_digest").fetchone()
    assert row == ("2026-08-25", "AAA", "kept", "2026-08-23 22:00:00", "2026-08-24 22:00:00")


def test_dashboard_is_read_only_and_preserves_admin_features():
    source = Path("app/ui/streamlit_app.py").read_text()
    dashboard = source[source.index('else:\n        st.title("Company News Dashboard")'):]
    assert "run_python_script(\n                    SUMMARIZE_SCRIPT_PATH" not in dashboard
    assert "As-of Date" in source and "News Window" not in source
    for feature in ("Ticker Admin", "Save Changes", "Delete Selected", "Refresh SEC", "Hide tickers with no data"):
        assert feature in source


def test_dashboard_uses_calendar_date_and_newspaper_icon():
    source = Path("app/ui/streamlit_app.py").read_text()
    assert 'page_icon="📰"' in source
    assert "st.sidebar.date_input(" in source
    assert 'value=default_report_date()' in source
    assert 'st.sidebar.selectbox(\n        "As-of Date"' not in source


def dashboard_functions(*names):
    source = Path("app/ui/streamlit_app.py").read_text()
    tree = ast.parse(source)
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), "<dashboard-functions>", "exec"), namespace)
    return source, namespace


def test_hide_empty_filter_includes_article_count_and_low_signal_summary():
    source, namespace = dashboard_functions("is_low_signal_summary", "should_hide_dashboard_row")
    is_low_signal = namespace["is_low_signal_summary"]
    should_hide = namespace["should_hide_dashboard_row"]

    assert is_low_signal("中性：來源內容無重大消息")
    assert is_low_signal("正面：來源內容無重大消息")
    assert is_low_signal("來源內容無重大消息")
    assert not is_low_signal("12345678901234567890來源內容無重大消息")
    assert should_hide({"article_count": 0, "summary": "A normal summary"})
    low_signal_row = {"article_count": 5, "summary": "中性： 來源內容無重大消息。近期新聞主要..."}
    assert should_hide(low_signal_row)
    assert not should_hide({"article_count": 5, "summary": "A normal summary"})
    assert [row for row in [low_signal_row] if not should_hide(row)] == []
    assert [row for row in [low_signal_row]] == [low_signal_row]
    assert '"Hide tickers with no data",\n        value=True' in source
    assert "if hide_empty:" in source


def test_dashboard_metrics_use_full_rows_and_render_in_funnel_order():
    source, namespace = dashboard_functions("is_low_signal_summary", "dashboard_metrics")
    rows = [
        {"article_count": 0, "summary": ""},
        {"article_count": 3, "summary": "中性：來源內容無重大消息"},
        {"article_count": 2, "summary": "Material news"},
    ]
    metrics = namespace["dashboard_metrics"](rows, [rows[2]])
    assert metrics == {
        "watchlist": 3,
        "with_news": 2,
        "low_signal": 1,
        "displayed": 1,
        "articles": 5,
    }
    metric_calls = [
        'overview_col1.metric("Watchlist"',
        'overview_col2.metric("With News"',
        'overview_col3.metric("Low Signal"',
        'overview_col4.metric("Displayed"',
        'overview_col5.metric(',
    ]
    assert [source.index(call) for call in metric_calls] == sorted(source.index(call) for call in metric_calls)
