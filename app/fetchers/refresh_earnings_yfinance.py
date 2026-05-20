import os
import sqlite3
from datetime import date, datetime, timezone

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
            price REAL,
            day_pct REAL,
            after_pct REAL,
            week_pct REAL,
            ytd_pct REAL,
            market_source TEXT,
            market_updated_at TEXT,
            next_earnings_date TEXT,
            earnings_time TEXT,
            earnings_source TEXT,
            earnings_updated_at TEXT
        )
        """
    )
    existing_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(ticker_metadata)").fetchall()
    }
    required_columns = {
        "price": "REAL",
        "day_pct": "REAL",
        "after_pct": "REAL",
        "week_pct": "REAL",
        "ytd_pct": "REAL",
        "market_source": "TEXT",
        "market_updated_at": "TEXT",
        "next_earnings_date": "TEXT",
        "earnings_time": "TEXT",
        "earnings_source": "TEXT",
        "earnings_updated_at": "TEXT",
    }
    for name, col_type in required_columns.items():
        if name not in existing_columns:
            conn.execute(f"ALTER TABLE ticker_metadata ADD COLUMN {name} {col_type}")
    conn.commit()


def get_watchlist_tickers(conn):
    rows = conn.execute("SELECT ticker FROM watchlist ORDER BY ticker").fetchall()
    return [row[0] for row in rows if row[0]]


def parse_earnings_datetime(ticker):
    def extract_value(raw):
        if raw is None:
            return None
        if isinstance(raw, dict):
            candidate = raw.get("Earnings Date")
            if candidate is None and raw:
                candidate = next(iter(raw.values()))
            return extract_value(candidate)
        if isinstance(raw, pd.DataFrame):
            if raw.empty:
                return None
            if "Earnings Date" in raw.index:
                return extract_value(raw.loc["Earnings Date"].iloc[0])
            if "Earnings Date" in raw.columns:
                return extract_value(raw["Earnings Date"].iloc[0])
            return extract_value(raw.iloc[0, 0])
        if isinstance(raw, (list, tuple)):
            return extract_value(raw[0]) if raw else None
        if isinstance(raw, (pd.Timestamp, datetime, date)):
            return raw
        return raw

    try:
        cal = ticker.calendar
        if cal is None or (hasattr(cal, "empty") and cal.empty):
            return None, None

        val = extract_value(cal)

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


def pct_change(current, base):
    if current is None or base is None:
        return None
    try:
        if base == 0:
            return None
        return ((float(current) - float(base)) / float(base)) * 100.0
    except Exception:
        return None


def get_market_snapshot(ticker):
    try:
        hist_1y = ticker.history(period="1y", auto_adjust=False, prepost=True)
        if hist_1y is None or hist_1y.empty:
            return {"price": None, "day_pct": None, "after_pct": None, "week_pct": None, "ytd_pct": None}

        hist_1y = hist_1y.dropna(subset=["Close"])
        if hist_1y.empty:
            return {"price": None, "day_pct": None, "after_pct": None, "week_pct": None, "ytd_pct": None}

        closes = hist_1y["Close"].tolist()
        price = closes[-1] if closes else None
        prev_close = closes[-2] if len(closes) >= 2 else None
        week_close = closes[-6] if len(closes) >= 6 else (closes[0] if closes else None)

        info = ticker.fast_info or {}
        previous_close_info = info.get("previous_close")
        last_price_info = info.get("last_price")
        day_base = previous_close_info if previous_close_info not in (None, 0) else prev_close
        day_price = last_price_info if last_price_info not in (None, 0) else price
        day_pct = pct_change(day_price, day_base)

        regular_market = info.get("regular_market_price")
        post_market = info.get("post_market_price")
        pre_market = info.get("pre_market_price")
        post_market_pct = None
        pre_market_pct = None

        if post_market in (None, 0) and pre_market in (None, 0):
            info_ext = ticker.info or {}
            post_market = info_ext.get("postMarketPrice")
            post_market_pct = info_ext.get("postMarketChangePercent")
            pre_market = info_ext.get("preMarketPrice")
            pre_market_pct = info_ext.get("preMarketChangePercent")

        after_pct = None
        if post_market not in (None, 0):
            after_pct = post_market_pct if post_market_pct is not None else pct_change(post_market, regular_market)
        elif pre_market not in (None, 0):
            after_pct = pre_market_pct if pre_market_pct is not None else pct_change(pre_market, regular_market)

        week_pct = pct_change(price, week_close)
        current_year = datetime.utcnow().year
        ytd_base = None
        for idx, dt_val in enumerate(hist_1y.index):
            py_dt = dt_val.to_pydatetime() if hasattr(dt_val, "to_pydatetime") else dt_val
            if py_dt.year == current_year:
                ytd_base = closes[idx]
                break
        if ytd_base is None and closes:
            ytd_base = closes[0]

        return {
            "price": day_price if day_price is not None else price,
            "day_pct": day_pct,
            "after_pct": after_pct,
            "week_pct": week_pct,
            "ytd_pct": pct_change(price, ytd_base),
        }
    except Exception:
        return {"price": None, "day_pct": None, "after_pct": None, "week_pct": None, "ytd_pct": None}


def upsert_metadata(conn, ticker, market, next_date, earnings_time):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO ticker_metadata (
            ticker, price, day_pct, after_pct, week_pct, ytd_pct,
            market_source, market_updated_at,
            next_earnings_date, earnings_time, earnings_source, earnings_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'yfinance', ?, ?, ?, 'yfinance', ?)
        ON CONFLICT(ticker) DO UPDATE SET
            price = excluded.price,
            day_pct = excluded.day_pct,
            after_pct = excluded.after_pct,
            week_pct = excluded.week_pct,
            ytd_pct = excluded.ytd_pct,
            market_source = excluded.market_source,
            market_updated_at = excluded.market_updated_at,
            next_earnings_date = excluded.next_earnings_date,
            earnings_time = excluded.earnings_time,
            earnings_source = excluded.earnings_source,
            earnings_updated_at = excluded.earnings_updated_at
        """,
        (
            ticker,
            market["price"],
            market["day_pct"],
            market["after_pct"],
            market["week_pct"],
            market["ytd_pct"],
            now_str,
            next_date,
            earnings_time,
            now_str,
        ),
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
        market = {"price": None, "day_pct": None, "after_pct": None, "week_pct": None, "ytd_pct": None}
        try:
            yf_ticker = yf.Ticker(symbol)
            market = get_market_snapshot(yf_ticker)
            next_date, earnings_time = parse_earnings_datetime(yf_ticker)
        except Exception:
            next_date, earnings_time = None, None

        upsert_metadata(conn, symbol, market, next_date, earnings_time)
        print(
            f"{symbol}: price={market['price'] if market['price'] is not None else 'N/A'} "
            f"1D={market['day_pct'] if market['day_pct'] is not None else 'N/A'} "
            f"Ext={market['after_pct'] if market['after_pct'] is not None else 'N/A'} "
            f"5D={market['week_pct'] if market['week_pct'] is not None else 'N/A'} "
            f"YTD={market['ytd_pct'] if market['ytd_pct'] is not None else 'N/A'} "
            f"earnings={next_date or 'N/A'} time={earnings_time or 'N/A'}"
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
