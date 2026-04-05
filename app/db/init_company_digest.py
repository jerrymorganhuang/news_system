import sqlite3

conn = sqlite3.connect("data/news.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS company_digest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    article_count INTEGER,
    summary TEXT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, window_start, window_end)
)
""")

conn.commit()
conn.close()

print("company_digest table created.")