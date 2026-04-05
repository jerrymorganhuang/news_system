from pathlib import Path
import sqlite3


BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "news.db"


def insert_watchlist(ticker, company_name, google_query):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    INSERT OR IGNORE INTO watchlist (ticker, company_name, google_query)
    VALUES (?, ?, ?)
    """, (ticker, company_name, google_query))

    conn.commit()
    conn.close()

    print(f"Inserted: {ticker}")


def show_watchlist():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT ticker, company_name FROM watchlist")
    rows = cursor.fetchall()

    conn.close()

    print("Current watchlist:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    insert_watchlist("OKLO", "Oklo Inc.", "Oklo nuclear")
    insert_watchlist("NVDA", "NVIDIA", "NVIDIA AI")
    insert_watchlist("MDB", "MongoDB", "MongoDB earnings")

    show_watchlist()