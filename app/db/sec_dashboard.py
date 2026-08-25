"""Read-only SEC queries used by the historical snapshot dashboard."""

import re
import sqlite3


def get_sec_summary_and_rows(
    conn: sqlite3.Connection, ticker: str, window_start: str, window_end: str
) -> dict:
    """Return the best compatible rolling SEC digest and bounded documents.

    SEC digests are still generated on rolling 24-hour windows and their
    ``window_end`` is the latest filing time.  A digest is therefore compatible
    with a daily company snapshot when that covered filing time falls inside the
    selected snapshot; equality with the snapshot's fixed 06:00 boundary is not
    expected.  Ordering by covered time prevents a later period from leaking
    into an older dashboard date.
    """
    digest = conn.execute(
        """
        SELECT summary_zh, generated_at
        FROM sec_digest
        WHERE ticker = ?
          AND window_hours = 24
          AND datetime(window_end) >= datetime(?)
          AND datetime(window_end) < datetime(?)
        ORDER BY datetime(window_end) DESC, datetime(generated_at) DESC, id DESC
        LIMIT 1
        """,
        (ticker, window_start, window_end),
    ).fetchone()

    accepted_expr = (
        "CASE WHEN length(sf.accepted_datetime)=14 THEN "
        "substr(sf.accepted_datetime,1,4)||'-'||substr(sf.accepted_datetime,5,2)||'-'||"
        "substr(sf.accepted_datetime,7,2)||' '||substr(sf.accepted_datetime,9,2)||':'||"
        "substr(sf.accepted_datetime,11,2)||':'||substr(sf.accepted_datetime,13,2) "
        "ELSE sf.accepted_datetime END"
    )
    document_time_expr = f"COALESCE({accepted_expr}, sf.filing_date, sd.fetched_at)"
    document_rows = conn.execute(
        f"""
        SELECT {document_time_expr}, sd.document_type,
               COALESCE(NULLIF(sd.document_title, ''), sd.document_url),
               sd.document_url
        FROM sec_documents sd
        LEFT JOIN sec_filings sf ON sf.id = sd.filing_id
        WHERE sd.ticker = ?
          AND datetime({document_time_expr}) >= datetime(?)
          AND datetime({document_time_expr}) < datetime(?)
        ORDER BY datetime({document_time_expr}) DESC, sd.id DESC
        """,
        (ticker, window_start, window_end),
    ).fetchall()

    summary = ""
    generated_at = ""
    if digest:
        summary = re.sub(
            r"\n?\[fp:[0-9a-f]{64}\]\s*$", "", (digest[0] or "").strip()
        )
        generated_at = digest[1] or ""
    return {
        "summary": summary,
        "generated_at": generated_at,
        "document_rows": document_rows,
    }
