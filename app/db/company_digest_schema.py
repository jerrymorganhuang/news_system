"""Schema and time-window helpers for historical company news snapshots."""

from datetime import date, datetime, time, timedelta, timezone
import sqlite3
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")


def daily_window(report_date: date) -> tuple[datetime, datetime]:
    """Return the fixed [start, end) UTC window for a Taipei report date."""
    end_tpe = datetime.combine(report_date, time(6), tzinfo=TAIPEI)
    start_tpe = end_tpe - timedelta(days=1)
    return start_tpe.astimezone(timezone.utc), end_tpe.astimezone(timezone.utc)


def utc_sql_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def default_report_date(now: datetime | None = None) -> date:
    now = now or datetime.now(TAIPEI)
    if now.tzinfo is None:
        now = now.replace(tzinfo=TAIPEI)
    return now.astimezone(TAIPEI).date()


def available_report_dates(conn: sqlite3.Connection) -> list[str]:
    """Return stored report dates newest first for the dashboard selector."""
    return [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT report_date FROM company_digest ORDER BY report_date DESC"
        ).fetchall()
    ]


def _legacy_report_date(window_end: str | None, generated_at: str | None) -> date:
    raw = window_end or generated_at
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(TAIPEI).date()
        except (TypeError, ValueError):
            pass
    return default_report_date()


def ensure_company_digest_schema(conn: sqlite3.Connection) -> None:
    """Create or safely migrate the rolling digest table to daily snapshots."""
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_digest'"
    ).fetchone()
    if not table:
        _create_table(conn)
        conn.commit()
        return

    columns = {row[1] for row in conn.execute("PRAGMA table_info(company_digest)")}
    if "report_date" in columns and "window_hours" not in columns:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_digest_report_ticker "
            "ON company_digest(report_date, ticker)"
        )
        conn.commit()
        return

    rows = conn.execute("SELECT * FROM company_digest").fetchall()
    names = [item[0] for item in conn.execute("SELECT name FROM pragma_table_info('company_digest')")]
    records = [dict(zip(names, row)) for row in rows]

    conn.execute("ALTER TABLE company_digest RENAME TO company_digest_legacy")
    _create_table(conn)
    # Prefer the existing 24-hour row. Older schemas without a window marker are
    # also retained; 48-hour rows are only a fallback when no 24-hour row exists.
    chosen: dict[tuple[str, str], dict] = {}
    fallback: dict[tuple[str, str], dict] = {}
    for record in records:
        hours = record.get("window_hours")
        report = _legacy_report_date(record.get("window_end"), record.get("generated_at"))
        key = (report.isoformat(), record.get("ticker"))
        fallback[key] = record
        if hours in (None, 24):
            chosen[key] = record
    for key, record in fallback.items():
        chosen.setdefault(key, record)

    for (report_text, ticker), record in chosen.items():
        if not ticker:
            continue
        report = date.fromisoformat(report_text)
        start, end = daily_window(report)
        conn.execute(
            """INSERT OR REPLACE INTO company_digest
               (report_date, ticker, window_start, window_end, article_count, summary, generated_at)
               VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))""",
            (report_text, ticker, utc_sql_timestamp(start), utc_sql_timestamp(end),
             record.get("article_count") or 0,
             record.get("summary") or record.get("digest") or record.get("ai_summary"),
             record.get("generated_at")),
        )
    conn.execute("DROP TABLE company_digest_legacy")
    conn.commit()


def _create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE company_digest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            article_count INTEGER DEFAULT 0,
            summary TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date, ticker)
        )"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_digest_report_ticker "
        "ON company_digest(report_date, ticker)"
    )
