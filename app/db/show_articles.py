# app/db/show_articles.py
import sqlite3

conn = sqlite3.connect("data/news.db")
cursor = conn.cursor()

cursor.execute("""
    SELECT ticker, title, source, published_at
    FROM articles
    ORDER BY id DESC
    LIMIT 20
""")

rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()