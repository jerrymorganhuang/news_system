import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")


def get_db_connection():
    return sqlite3.connect(DB_PATH)


def ensure_ticker_metadata_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_metadata (
            ticker TEXT PRIMARY KEY,
            next_earnings_date TEXT,
            earnings_time TEXT,
            earnings_source TEXT,
            earnings_updated_at TEXT
        )
        """
    )
    conn.commit()


def get_watchlist_tickers(conn):
    rows = conn.execute("SELECT ticker FROM watchlist ORDER BY ticker").fetchall()
    return [row[0] for row in rows if row[0]]


def parse_earnings_datetime(ticker):
    try:
        cal = ticker.calendar
        if cal is None or (hasattr(cal, "empty") and cal.empty):
            return None, None

        if isinstance(cal, pd.DataFrame):
            if "Earnings Date" in cal.index:
                val = cal.loc["Earnings Date"].iloc[0]
            elif "Earnings Date" in cal.columns:
                val = cal["Earnings Date"].iloc[0]
            else:
                val = cal.iloc[0, 0]
        else:
            val = None

        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None, None

        dt = pd.to_datetime(val, errors="coerce", utc=True)
        if pd.isna(dt):
            return None, None

        date_str = dt.strftime("%Y-%m-%d")
        time_str = None if dt.hour == 0 and dt.minute == 0 and dt.second == 0 else dt.strftime("%H:%M:%S")
        return date_str, time_str
    except Exception:
        return None, None


def upsert_earnings(conn, ticker, next_date, earnings_time):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO ticker_metadata (
            ticker, next_earnings_date, earnings_time, earnings_source, earnings_updated_at
        ) VALUES (?, ?, ?, 'yfinance', ?)
        ON CONFLICT(ticker) DO UPDATE SET
            next_earnings_date = excluded.next_earnings_date,
            earnings_time = excluded.earnings_time,
            earnings_source = excluded.earnings_source,
            earnings_updated_at = excluded.earnings_updated_at
        """,
        (ticker, next_date, earnings_time, now_str),
    )


def main():
    conn = get_db_connection()
    ensure_ticker_metadata_schema(conn)

    tickers = get_watchlist_tickers(conn)
    if not tickers:
        print("No watchlist rows found.")
        return

    for symbol in tickers:
        next_date = None
        earnings_time = None
        try:
            yf_ticker = yf.Ticker(symbol)
            next_date, earnings_time = parse_earnings_datetime(yf_ticker)
        except Exception:
            next_date, earnings_time = None, None

        upsert_earnings(conn, symbol, next_date, earnings_time)
        print(f"{symbol}: earnings={next_date or 'N/A'} time={earnings_time or 'N/A'}")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
