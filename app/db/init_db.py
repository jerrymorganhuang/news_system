from pathlib import Path
import sqlite3


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "news.db"


def ensure_articles_columns(cursor):
    cursor.execute("PRAGMA table_info(articles)")
    columns = {row[1] for row in cursor.fetchall()}

    if "discovery_source" not in columns:
        cursor.execute("ALTER TABLE articles ADD COLUMN discovery_source TEXT")

    if "canonical_url" not in columns:
        cursor.execute("ALTER TABLE articles ADD COLUMN canonical_url TEXT")

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_articles_canonical_url ON articles(canonical_url)"
    )
    cursor.execute(
        """
        UPDATE articles
        SET discovery_source = source_type
        WHERE discovery_source IS NULL
          AND source_type IN ('google_news', 'yahoo_finance')
        """
    )


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
        discovery_source TEXT,
        url TEXT NOT NULL,
        canonical_url TEXT,
        published_at TEXT,
        content TEXT,
        content_status TEXT,
        content_error TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, url)
    )
    """)

    ensure_articles_columns(cursor)

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

    cursor.execute("""
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
    """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker ON sec_filings(ticker)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_filings_filing_date ON sec_filings(filing_date)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_filings_accepted_datetime ON sec_filings(accepted_datetime)"
    )

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sec_documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filing_id INTEGER,
        ticker TEXT NOT NULL,
        cik TEXT NOT NULL,
        accession_number TEXT NOT NULL,
        document_type TEXT NOT NULL,
        document_title TEXT,
        document_url TEXT NOT NULL,
        raw_text TEXT,
        clean_text TEXT,
        content_status TEXT,
        content_error TEXT,
        fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(cik, accession_number, document_url),
        FOREIGN KEY(filing_id) REFERENCES sec_filings(id)
    )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_documents_ticker ON sec_documents(ticker)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_documents_filing_id ON sec_documents(filing_id)"
    )

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sec_digest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        window_hours INTEGER NOT NULL,
        window_start TEXT,
        window_end TEXT,
        filing_count INTEGER DEFAULT 0,
        document_count INTEGER DEFAULT 0,
        summary_zh TEXT,
        generated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, window_hours, window_end)
    )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_digest_ticker_window ON sec_digest(ticker, window_hours)"
    )

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
