import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")

CLEARED_TABLES = [
    "sec_digest",
    "sec_documents",
    "sec_filings",
    "company_digest",
    "articles",
]
PRESERVED_TABLES = ["watchlist", "ticker_source_map"]


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        conn.execute("BEGIN")

        for table in CLEARED_TABLES:
            cursor.execute(f"DELETE FROM {table}")

        placeholders = ", ".join("?" for _ in CLEARED_TABLES)
        cursor.execute(
            f"DELETE FROM sqlite_sequence WHERE name IN ({placeholders})",
            CLEARED_TABLES,
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("Done.")
    print(f"Cleared tables: {', '.join(CLEARED_TABLES)}")
    print(f"Preserved tables: {', '.join(PRESERVED_TABLES)}")
    print("Confirmed untouched tables: watchlist, ticker_source_map")


if __name__ == "__main__":
    main()
