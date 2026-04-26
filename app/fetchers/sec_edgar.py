import json
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone, timedelta


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")

SEC_LOOKBACK_DAYS = int(os.getenv("SEC_LOOKBACK_DAYS", "7"))
SEC_SLEEP_SECONDS = float(os.getenv("SEC_SLEEP_SECONDS", "0.2"))
SEC_TIMEOUT_SECONDS = float(os.getenv("SEC_TIMEOUT_SECONDS", "20"))
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "news_system/1.0 (SEC metadata fetch; contact: devnull@example.com)",
)


def get_connection():
    return sqlite3.connect(DB_PATH)


def ensure_sec_filings_schema(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sec_filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            cik TEXT NOT NULL,
            accession_number TEXT NOT NULL,
            form_type TEXT NOT NULL,
            filing_date TEXT,
            accepted_datetime TEXT,
            report_date TEXT,
            item_numbers TEXT,
            primary_doc TEXT,
            primary_doc_url TEXT,
            filing_detail_url TEXT,
            title TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cik, accession_number)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker ON sec_filings(ticker)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_filings_filing_date ON sec_filings(filing_date)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_filings_accepted_datetime ON sec_filings(accepted_datetime)"
    )
    conn.commit()


def normalize_cik_for_submissions(raw_cik):
    cik_digits = "".join(ch for ch in str(raw_cik or "").strip() if ch.isdigit())
    if not cik_digits:
        return ""
    return cik_digits.zfill(10)


def cik_for_archive_path(cik_10):
    return str(int(cik_10))


def accession_without_dashes(accession_number):
    return (accession_number or "").replace("-", "").strip()


def parse_filing_date(filing_date_str):
    if not filing_date_str:
        return None
    try:
        return datetime.strptime(filing_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def get_watchlist_ciks(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT w.ticker, COALESCE(NULLIF(tsm.sec_cik, ''), NULL) AS sec_cik
        FROM watchlist w
        LEFT JOIN ticker_source_map tsm
          ON tsm.ticker = w.ticker
        WHERE COALESCE(w.enabled, 1) = 1
        ORDER BY w.ticker
        """
    )
    return cursor.fetchall()


def fetch_company_submissions(cik_10):
    url = f"https://data.sec.gov/submissions/CIK{cik_10}.json"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": SEC_USER_AGENT,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=SEC_TIMEOUT_SECONDS) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def build_urls(cik_10, accession_number, primary_doc):
    cik_archive = cik_for_archive_path(cik_10)
    accession_clean = accession_without_dashes(accession_number)
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_archive}/{accession_clean}/"
    primary_doc_url = f"{base}{primary_doc}" if primary_doc else ""
    return base, primary_doc_url


def insert_sec_filing(conn, filing):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO sec_filings (
            ticker,
            cik,
            accession_number,
            form_type,
            filing_date,
            accepted_datetime,
            report_date,
            item_numbers,
            primary_doc,
            primary_doc_url,
            filing_detail_url,
            title
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filing["ticker"],
            filing["cik"],
            filing["accession_number"],
            filing["form_type"],
            filing.get("filing_date"),
            filing.get("accepted_datetime"),
            filing.get("report_date"),
            filing.get("item_numbers"),
            filing.get("primary_doc"),
            filing.get("primary_doc_url"),
            filing.get("filing_detail_url"),
            filing.get("title"),
        ),
    )
    conn.commit()
    return cursor.rowcount


def fetch_and_store_sec_8k(lookback_days=SEC_LOOKBACK_DAYS):
    conn = get_connection()
    ensure_sec_filings_schema(conn)

    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    rows = get_watchlist_ciks(conn)

    tickers_checked = 0
    tickers_skipped_missing_cik = 0
    inserted = 0
    duplicates = 0
    errors = 0

    for ticker, raw_cik in rows:
        tickers_checked += 1
        cik_10 = normalize_cik_for_submissions(raw_cik)
        if not cik_10:
            tickers_skipped_missing_cik += 1
            print(f"[SEC] Skip {ticker}: missing sec_cik")
            continue

        try:
            submissions = fetch_company_submissions(cik_10)
            recent = ((submissions or {}).get("filings") or {}).get("recent") or {}
            forms = recent.get("form", [])
            accession_numbers = recent.get("accessionNumber", [])
            filing_dates = recent.get("filingDate", [])
            accepted_datetimes = recent.get("acceptanceDateTime", [])
            report_dates = recent.get("reportDate", [])
            item_numbers = recent.get("items", [])
            primary_docs = recent.get("primaryDocument", [])
            titles = recent.get("primaryDocDescription", [])

            total_rows = len(forms)
            for i in range(total_rows):
                form_type = (forms[i] or "").strip() if i < len(forms) else ""
                if form_type != "8-K":
                    continue

                filing_date = filing_dates[i] if i < len(filing_dates) else None
                filing_dt = parse_filing_date(filing_date)
                if filing_dt is None or filing_dt < cutoff_dt:
                    continue

                accession_number = accession_numbers[i] if i < len(accession_numbers) else ""
                if not accession_number:
                    continue

                primary_doc = primary_docs[i] if i < len(primary_docs) else ""
                filing_detail_url, primary_doc_url = build_urls(cik_10, accession_number, primary_doc)

                filing = {
                    "ticker": ticker,
                    "cik": cik_10,
                    "accession_number": accession_number,
                    "form_type": form_type,
                    "filing_date": filing_date,
                    "accepted_datetime": accepted_datetimes[i] if i < len(accepted_datetimes) else None,
                    "report_date": report_dates[i] if i < len(report_dates) else None,
                    "item_numbers": item_numbers[i] if i < len(item_numbers) else None,
                    "primary_doc": primary_doc,
                    "primary_doc_url": primary_doc_url,
                    "filing_detail_url": filing_detail_url,
                    "title": titles[i] if i < len(titles) else None,
                }

                rowcount = insert_sec_filing(conn, filing)
                if rowcount:
                    inserted += 1
                else:
                    duplicates += 1

        except Exception as exc:
            errors += 1
            print(f"[SEC] Error {ticker} (CIK {cik_10}): {exc}")

        time.sleep(SEC_SLEEP_SECONDS)

    conn.close()

    print("\n[SEC] 8-K fetch summary")
    print(f"[SEC] Tickers checked: {tickers_checked}")
    print(f"[SEC] Tickers skipped (missing CIK): {tickers_skipped_missing_cik}")
    print(f"[SEC] 8-K filings inserted: {inserted}")
    print(f"[SEC] Duplicates skipped: {duplicates}")
    print(f"[SEC] Errors encountered: {errors}")


if __name__ == "__main__":
    fetch_and_store_sec_8k()
