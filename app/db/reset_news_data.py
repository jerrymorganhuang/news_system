import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM articles")
    cursor.execute("DELETE FROM company_digest")
    cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('articles', 'company_digest')")

    conn.commit()
    conn.close()

    print("Done.")
    print("Cleared tables: articles, company_digest")
    print("Preserved table: watchlist")


if __name__ == "__main__":
    main()