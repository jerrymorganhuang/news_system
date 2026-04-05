import sqlite3

conn = sqlite3.connect("data/news.db")
cursor = conn.cursor()

cursor.execute("""
    SELECT ticker, title, substr(content, 1, 300)
    FROM articles
    WHERE content IS NOT NULL AND content != ''
    ORDER BY id DESC
    LIMIT 5
""")

rows = cursor.fetchall()

for row in rows:
    print("=" * 80)
    print("Ticker:", row[0])
    print("Title:", row[1])
    print("Content Preview:", row[2])

conn.close()