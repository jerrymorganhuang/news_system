import json
import os
import sqlite3
import urllib.request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")
SEC_MAPPING_CACHE_PATH = os.path.join(BASE_DIR, "data", "sec_company_tickers.json")
SEC_MAPPING_URL = "https://www.sec.gov/files/company_tickers.json"


def normalize_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper()


def format_cik_10_digits(raw_cik):
    if raw_cik is None:
        return ""

    cik_digits = "".join(ch for ch in str(raw_cik).strip() if ch.isdigit())
    if not cik_digits:
        return ""
    return cik_digits.zfill(10)


def download_sec_mapping():
    request = urllib.request.Request(
        SEC_MAPPING_URL,
        headers={"User-Agent": "news-system/1.0 (local sec mapping sync)"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def save_mapping_file(mapping_json):
    os.makedirs(os.path.dirname(SEC_MAPPING_CACHE_PATH), exist_ok=True)
    with open(SEC_MAPPING_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping_json, f, ensure_ascii=False)


def build_ticker_mapping(mapping_json):
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


def fill_missing_sec_fields(conn, ticker_map):
    cursor = conn.cursor()

    table_exists = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ticker_source_map' LIMIT 1"
    ).fetchone()
    if not table_exists:
        return 0

    rows = cursor.execute(
        """
        SELECT ticker, sec_cik, sec_company_name
        FROM ticker_source_map
        """
    ).fetchall()

    rows_filled = 0
    for ticker_raw, sec_cik_raw, sec_company_name_raw in rows:
        ticker = normalize_ticker(ticker_raw)
        if not ticker:
            continue

        sec_match = ticker_map.get(ticker)
        if not sec_match:
            continue

        existing_sec_cik = (sec_cik_raw or "").strip()
        existing_sec_company_name = (sec_company_name_raw or "").strip()

        next_sec_cik = existing_sec_cik or (sec_match.get("sec_cik", "") or "").strip()
        next_sec_company_name = existing_sec_company_name or (sec_match.get("sec_company_name", "") or "").strip()

        if next_sec_cik == existing_sec_cik and next_sec_company_name == existing_sec_company_name:
            continue

        cursor.execute(
            """
            UPDATE ticker_source_map
            SET sec_cik = ?,
                sec_company_name = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE ticker = ?
            """,
            (
                next_sec_cik if next_sec_cik else None,
                next_sec_company_name if next_sec_company_name else None,
                ticker,
            ),
        )
        rows_filled += 1

    conn.commit()
    return rows_filled


def main():
    mapping_json = download_sec_mapping()
    save_mapping_file(mapping_json)

    ticker_map = build_ticker_mapping(mapping_json)

    conn = sqlite3.connect(DB_PATH)
    try:
        rows_filled = fill_missing_sec_fields(conn, ticker_map)
    finally:
        conn.close()

    if rows_filled > 0:
        print(f"SEC mapping refreshed: file updated, {rows_filled} rows filled")
    else:
        print("SEC mapping refreshed: file updated, no rows changed")


if __name__ == "__main__":
    main()
