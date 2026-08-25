import sqlite3

from app.db.sec_dashboard import get_sec_summary_and_rows


def make_connection():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE sec_digest (
            id INTEGER PRIMARY KEY,
            ticker TEXT,
            window_hours INTEGER,
            window_start TEXT,
            window_end TEXT,
            summary_zh TEXT,
            generated_at TEXT
        );
        CREATE TABLE sec_filings (
            id INTEGER PRIMARY KEY,
            accepted_datetime TEXT,
            filing_date TEXT
        );
        CREATE TABLE sec_documents (
            id INTEGER PRIMARY KEY,
            filing_id INTEGER,
            ticker TEXT,
            document_type TEXT,
            document_title TEXT,
            document_url TEXT,
            fetched_at TEXT
        );
        """
    )
    return conn


def add_digest(conn, digest_id, covered_end, summary, generated_at):
    conn.execute(
        """INSERT INTO sec_digest VALUES
           (?, 'AAA', 24, '2026-08-24 00:00:00', ?, ?, ?)""",
        (digest_id, covered_end, summary, generated_at),
    )


def test_filing_timestamp_inside_window_finds_digest_without_exact_end_match():
    conn = make_connection()
    add_digest(
        conn, 1, "2026-08-24 20:15:00", "current summary", "2026-08-25 02:30:00"
    )

    result = get_sec_summary_and_rows(
        conn, "AAA", "2026-08-23 22:00:00", "2026-08-24 22:00:00"
    )

    assert result["summary"] == "current summary"
    assert result["generated_at"] == "2026-08-25 02:30:00"


def test_old_snapshot_does_not_leak_future_digest_and_empty_is_clean():
    conn = make_connection()
    add_digest(conn, 1, "2026-08-25 10:00:00", "future summary", "2026-08-25 11:00:00")

    result = get_sec_summary_and_rows(
        conn, "AAA", "2026-08-23 22:00:00", "2026-08-24 22:00:00"
    )

    assert result["summary"] == ""
    assert result["generated_at"] == ""


def test_latest_compatible_covered_digest_is_preferred():
    conn = make_connection()
    add_digest(conn, 1, "2026-08-24 08:00:00", "earlier", "2026-08-24 09:00:00")
    add_digest(conn, 2, "2026-08-24 20:00:00", "latest compatible", "2026-08-25 02:00:00")
    add_digest(conn, 3, "2026-08-25 01:00:00", "future", "2026-08-25 02:30:00")

    result = get_sec_summary_and_rows(
        conn, "AAA", "2026-08-23 22:00:00", "2026-08-24 22:00:00"
    )

    assert result["summary"] == "latest compatible"


def test_document_rows_keep_inclusive_lower_exclusive_upper_bounds():
    conn = make_connection()
    filings = [
        (1, "2026-08-23 21:59:59", "2026-08-23"),
        (2, "2026-08-23 22:00:00", "2026-08-23"),
        (3, "2026-08-24 21:59:59", "2026-08-24"),
        (4, "2026-08-24 22:00:00", "2026-08-24"),
    ]
    conn.executemany("INSERT INTO sec_filings VALUES (?, ?, ?)", filings)
    conn.executemany(
        "INSERT INTO sec_documents VALUES (?, ?, 'AAA', '8-K', ?, ?, '2026-08-25')",
        [(row[0], row[0], f"doc-{row[0]}", f"https://example.com/{row[0]}") for row in filings],
    )

    result = get_sec_summary_and_rows(
        conn, "AAA", "2026-08-23 22:00:00", "2026-08-24 22:00:00"
    )

    assert {row[2] for row in result["document_rows"]} == {"doc-2", "doc-3"}
