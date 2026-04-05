import sqlite3

conn = sqlite3.connect("data/news.db")
cursor = conn.cursor()

cursor.execute("""
SELECT ticker, article_count, summary
FROM company_digest
ORDER BY generated_at DESC
""")

rows = cursor.fetchall()

for row in rows:
    print("=" * 80)
    print("Ticker:", row[0])
    print("Article count:", row[1])
    print("Summary:")
    print(row[2])
    print()

conn.close()