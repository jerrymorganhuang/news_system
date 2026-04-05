import sqlite3

conn = sqlite3.connect("data/news.db")
cursor = conn.cursor()

print("=== Recent 48h processing status ===")
cursor.execute("""
SELECT
    COALESCE(content_status, 'pending') as status,
    COALESCE(content_error, '-') as error,
    COUNT(*)
FROM articles
WHERE published_at >= datetime('now', '-48 hours')
GROUP BY COALESCE(content_status, 'pending'), COALESCE(content_error, '-')
ORDER BY COUNT(*) DESC
""")

rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()