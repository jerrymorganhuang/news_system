import os
import sqlite3
import time
import trafilatura
from googlenewsdecoder import new_decoderv1

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "news.db")

LOOKBACK_HOURS = 48
PROCESS_LIMIT = 10
SLEEP_SECONDS = 3


def get_db_connection():
    return sqlite3.connect(DB_PATH)


def get_recent_articles_to_process(conn, limit=10):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT id, ticker, title, url, published_at
        FROM articles
        WHERE content_status IS NULL
          AND published_at >= datetime('now', '-{LOOKBACK_HOURS} hours')
        ORDER BY published_at DESC
        LIMIT ?
    """, (limit,))
    return cursor.fetchall()


def decode_google_news_url(google_url):
    try:
        result = new_decoderv1(google_url)
        if isinstance(result, dict) and result.get("status"):
            return result.get("decoded_url"), None
        return google_url, "decode_failed"
    except Exception:
        return google_url, "decode_failed"


def fetch_article_content(url):
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None, "fetch_none"

        text = trafilatura.extract(downloaded)
        if not text:
            return None, "extract_none"

        return text, None
    except Exception:
        return None, "fetch_error"


def update_article(conn, article_id, content, status, error):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE articles
        SET content = ?, content_status = ?, content_error = ?
        WHERE id = ?
    """, (content, status, error, article_id))
    conn.commit()


def process_articles():
    conn = get_db_connection()
    articles = get_recent_articles_to_process(conn, limit=PROCESS_LIMIT)

    if not articles:
        print("No recent articles need processing.")
        conn.close()
        return

    print(f"Found {len(articles)} recent articles to process.\n")

    for article_id, ticker, title, url, published_at in articles:
        print(f"Processing {ticker} | {title}")
        print(f"  Published at: {published_at}")

        real_url, decode_error = decode_google_news_url(url)

        if decode_error:
            update_article(conn, article_id, None, "failed", decode_error)
            print("  Failed: decode_failed\n")
            time.sleep(SLEEP_SECONDS)
            continue

        print(f"  Decoded URL: {real_url}")

        content, error = fetch_article_content(real_url)

        if content and content.strip():
            update_article(conn, article_id, content, "success", None)
            print("  Content saved.\n")
        else:
            update_article(conn, article_id, None, "failed", error)
            print(f"  Failed: {error}\n")

        time.sleep(SLEEP_SECONDS)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    process_articles()