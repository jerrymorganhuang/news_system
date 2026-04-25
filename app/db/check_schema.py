import argparse
import sqlite3


REQUIRED_TABLES = {
    "watchlist",
    "articles",
    "company_digest",
}

REQUIRED_ARTICLES_COLUMNS = {
    "content",
    "content_status",
    "content_error",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate SQLite schema")
    parser.add_argument(
        "--db",
        default="data/news.db",
        help="Path to SQLite database file (default: data/news.db)",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        tables = {row[0] for row in cursor.fetchall()}

        missing_items: list[str] = []

        missing_tables = sorted(REQUIRED_TABLES - tables)
        for table in missing_tables:
            missing_items.append(f"missing table: {table}")

        if "articles" in tables:
            cursor.execute("PRAGMA table_info(articles)")
            article_columns = {row[1] for row in cursor.fetchall()}
            missing_columns = sorted(REQUIRED_ARTICLES_COLUMNS - article_columns)
            for column in missing_columns:
                missing_items.append(f"missing articles column: {column}")

        if missing_items:
            print("Schema check failed.")
            for item in missing_items:
                print(item)
            raise SystemExit(1)

        print("Schema check passed.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
