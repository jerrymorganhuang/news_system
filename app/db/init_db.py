from pathlib import Path
import sqlite3


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "news.db"


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS watchlist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL UNIQUE,
        company_name TEXT,
        google_query TEXT,
        sec_cik TEXT,
        press_rss TEXT,
        sa_rss TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        title TEXT NOT NULL,
        source TEXT,
        source_type TEXT,
        url TEXT NOT NULL,
        published_at TEXT,
        content TEXT,
        content_status TEXT,
        content_error TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, url)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS company_digest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        window_hours INTEGER NOT NULL,
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        article_count INTEGER DEFAULT 0,
        summary TEXT,
        generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, window_hours)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ticker_source_map (
        ticker TEXT PRIMARY KEY,
        google_query TEXT,
        sec_cik TEXT,
        sec_company_name TEXT,
        updated_at TEXT
    )
    """)

    cursor.execute(
        """
        INSERT INTO ticker_source_map (
            ticker,
            google_query,
            sec_cik,
            sec_company_name,
            updated_at
        )
        SELECT
            w.ticker,
            COALESCE(NULLIF(w.google_query, ''), w.ticker) AS google_query,
            NULLIF(w.sec_cik, '') AS sec_cik,
            NULL AS sec_company_name,
            CURRENT_TIMESTAMP
        FROM watchlist w
        WHERE NOT EXISTS (
            SELECT 1
            FROM ticker_source_map tsm
            WHERE tsm.ticker = w.ticker
        )
        """
    )

    conn.commit()
    conn.close()

    print(f"Database initialized at: {DB_PATH}")


if __name__ == "__main__":
    init_db()
