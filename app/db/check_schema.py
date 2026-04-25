import argparse
import sqlite3


def main() -> None:
    parser = argparse.ArgumentParser(description="Show SQLite schema details")
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
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]

        if not tables:
            print(f"No user tables found in {args.db}")
            return

        print(f"Database: {args.db}")
        print("Tables:")
        for table in tables:
            print(f"\n- {table}")
            cursor.execute(f"PRAGMA table_info({table})")
            for col in cursor.fetchall():
                cid, name, ctype, notnull, default_value, pk = col
                print(
                    f"  {cid}: {name} {ctype}"
                    f"{' NOT NULL' if notnull else ''}"
                    f"{' PRIMARY KEY' if pk else ''}"
                    f" DEFAULT {default_value}" if default_value is not None else ""
                )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
