import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from openai import OpenAI

# ===== 基本設定 =====
BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"
DB_PATH = BASE_DIR / "data" / "news.db"

# 載入 .env
load_dotenv(ENV_PATH)

MODEL = "gpt-5-mini"
MAX_ARTICLES_PER_TICKER = 20
MAX_CONTENT_CHARS = 1800


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            f"OPENAI_API_KEY not found. Please create {ENV_PATH} and add:\n"
            f"OPENAI_API_KEY=sk-..."
        )
    return OpenAI(api_key=api_key)


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    return conn


def get_recent_tickers(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT ticker
        FROM articles
        WHERE datetime(COALESCE(published_at, fetched_at)) >= datetime('now', '-48 hours')
        ORDER BY ticker
        """
    )
    rows = cursor.fetchall()
    return [row[0] for row in rows]


def get_all_digest_tickers(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM company_digest")
    return [row[0] for row in cursor.fetchall()]


def delete_digest_for_ticker(conn: sqlite3.Connection, ticker: str) -> None:
    cursor = conn.cursor()
    cursor.execute("DELETE FROM company_digest WHERE ticker = ?", (ticker,))
    conn.commit()


def get_articles_for_ticker(conn: sqlite3.Connection, ticker: str) -> List[Tuple]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT title, source, published_at, content, url
        FROM articles
        WHERE ticker = ?
          AND datetime(COALESCE(published_at, fetched_at)) >= datetime('now', '-48 hours')
        ORDER BY datetime(COALESCE(published_at, fetched_at)) DESC
        LIMIT ?
    """,
        (ticker, MAX_ARTICLES_PER_TICKER),
    )
    return cursor.fetchall()


def get_digest_generated_at(conn: sqlite3.Connection, ticker: str) -> Optional[str]:
    cursor = conn.cursor()
    row = cursor.execute(
        """
        SELECT datetime(generated_at)
        FROM company_digest
        WHERE ticker = ?
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()
    if not row:
        return None
    return row[0]


def parse_db_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    # Handle common SQLite and ISO formats consistently.
    normalized = normalized.replace("T", " ").replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            dt = datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    # Normalize timezone-aware values for safe comparison.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt


def get_latest_article_timestamp(conn: sqlite3.Connection, ticker: str) -> Optional[str]:
    cursor = conn.cursor()
    row = cursor.execute(
        """
        SELECT MAX(datetime(COALESCE(published_at, fetched_at)))
        FROM articles
        WHERE ticker = ?
          AND datetime(COALESCE(published_at, fetched_at)) >= datetime('now', '-48 hours')
        """,
        (ticker,),
    ).fetchone()
    if not row:
        return None
    return row[0]


def should_regenerate_digest(
    generated_at: Optional[str], latest_article_timestamp: Optional[str]
) -> bool:
    generated_dt = parse_db_timestamp(generated_at)
    latest_article_dt = parse_db_timestamp(latest_article_timestamp)

    if generated_dt is None:
        return True
    if latest_article_dt is None:
        return False

    return latest_article_dt > generated_dt


def build_articles_text(ticker: str, articles: List[Tuple]) -> str:
    blocks = []

    for i, article in enumerate(articles, start=1):
        title = (article[0] or "").strip()
        source = (article[1] or "").strip()
        published_at = (article[2] or "").strip()
        content = (article[3] or "").strip()
        url = (article[4] or "").strip()

        if content:
            content = content[:MAX_CONTENT_CHARS]
        else:
            content = "[No content fetched]"

        block = f"""Article {i}
Ticker: {ticker}
Title: {title}
Source: {source}
Published At: {published_at}
URL: {url}
Content:
{content}"""
        blocks.append(block)

    return "\n\n" + ("\n\n" + ("-" * 80) + "\n\n").join(blocks)


def generate_ai_summary(client: OpenAI, ticker: str, articles: List[Tuple]) -> str:
    if not articles:
        return f"{ticker} has no news in the last 48 hours."

    article_text = build_articles_text(ticker, articles)

    prompt = f"""

你是一位專業的財經新聞整合助手，正在為投資研究儀表板撰寫公司新聞摘要。

你會收到同一家公司（Ticker: {ticker}）在過去 48 小時內的多篇新聞。
請將這些新聞整合成「一段繁體中文的公司層級摘要」。

你的任務：
- 請整合所有新聞，寫成一段公司層級的整體摘要
- 重點放在：最近發生了什麼、主要事件是什麼、市場關注的重點是什麼
- 最前面放上判斷，正面 or 負面 or 中性
- 如果多篇新聞在講同一件事，請合併成一個主題，不要重複描述
- 不要逐篇摘要新聞
- 不要條列式
- 不要自己猜測或加入新聞中沒有的內容，保留重要數據
- 語氣保持客觀、像 sell-side analyst 的公司近況摘要
- 請使用「繁體中文」
- 長度大約 200–300 字，1 段為主，必要時可 2 小段

以下是新聞資料：
{article_text}
""".strip()

    response = client.responses.create(model=MODEL, input=prompt)

    summary = (response.output_text or "").strip()

    if not summary:
        return f"{ticker}: Summary generation returned empty output."

    return summary


def save_company_digest(
    conn: sqlite3.Connection,
    ticker: str,
    summary: str,
    article_count: int,
) -> None:
    cursor = conn.cursor()

    window_end = datetime.utcnow()
    window_start = window_end - timedelta(hours=48)

    cursor.execute(
        """
        INSERT OR REPLACE INTO company_digest (
            ticker,
            window_start,
            window_end,
            article_count,
            summary,
            generated_at
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """,
        (
            ticker,
            window_start.strftime("%Y-%m-%d %H:%M:%S"),
            window_end.strftime("%Y-%m-%d %H:%M:%S"),
            article_count,
            summary,
        ),
    )

    conn.commit()


def main() -> None:
    print(f"Loading .env from: {ENV_PATH}")
    print(f"Using database: {DB_PATH}")

    conn = get_db_connection()

    try:
        client = get_openai_client()

        recent_tickers = get_recent_tickers(conn)
        recent_set = set(recent_tickers)

        digest_tickers = get_all_digest_tickers(conn)
        stale_digest_tickers = sorted(set(digest_tickers) - recent_set)

        for ticker in stale_digest_tickers:
            delete_digest_for_ticker(conn, ticker)
            print(f"ticker={ticker} | article_count=0 | action=delete stale digest")

        if not recent_tickers:
            print("No recent tickers found in the last 48 hours.")
            return

        for ticker in recent_tickers:
            articles = get_articles_for_ticker(conn, ticker)
            article_count = len(articles)

            if article_count == 0:
                # Defensive branch; recent_tickers already filters this out.
                delete_digest_for_ticker(conn, ticker)
                print(f"ticker={ticker} | article_count=0 | action=delete stale digest")
                continue

            generated_at = get_digest_generated_at(conn, ticker)
            latest_article_timestamp = get_latest_article_timestamp(conn, ticker)

            if should_regenerate_digest(generated_at, latest_article_timestamp):
                try:
                    print(f"ticker={ticker} | article_count={article_count} | action=generate")
                    summary = generate_ai_summary(client, ticker, articles)
                    save_company_digest(
                        conn=conn,
                        ticker=ticker,
                        summary=summary,
                        article_count=article_count,
                    )
                except Exception as e:
                    print(f"  Failed to summarize {ticker}: {e}")
            else:
                print(f"ticker={ticker} | article_count={article_count} | action=skip")

        print("Done.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
