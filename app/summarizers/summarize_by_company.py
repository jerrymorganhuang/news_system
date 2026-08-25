import argparse
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
from openai import OpenAI

from app.db.company_digest_schema import (
    daily_window,
    default_report_date,
    ensure_company_digest_schema,
    utc_sql_timestamp,
)

ENV_PATH = BASE_DIR / ".env"
DB_PATH = BASE_DIR / "data" / "news.db"
load_dotenv(ENV_PATH)
MODEL = "gpt-5-mini"
MAX_ARTICLES_PER_TICKER = 20
MAX_CONTENT_CHARS = 1800


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(f"OPENAI_API_KEY not found. Please add it to {ENV_PATH}")
    return OpenAI(api_key=api_key)


def get_db_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def get_recent_tickers(conn: sqlite3.Connection, window_start: str, window_end: str) -> List[str]:
    rows = conn.execute(
        """SELECT DISTINCT ticker FROM articles
           WHERE datetime(COALESCE(published_at, fetched_at)) >= datetime(?)
             AND datetime(COALESCE(published_at, fetched_at)) < datetime(?)
           ORDER BY ticker""",
        (window_start, window_end),
    ).fetchall()
    return [row[0] for row in rows]


def get_articles_for_ticker(conn: sqlite3.Connection, ticker: str, window_start: str, window_end: str) -> List[Tuple]:
    return conn.execute(
        """SELECT title, source, published_at, content, url FROM articles
           WHERE ticker = ?
             AND datetime(COALESCE(published_at, fetched_at)) >= datetime(?)
             AND datetime(COALESCE(published_at, fetched_at)) < datetime(?)
           ORDER BY datetime(COALESCE(published_at, fetched_at)) DESC LIMIT ?""",
        (ticker, window_start, window_end, MAX_ARTICLES_PER_TICKER),
    ).fetchall()


def get_latest_article_timestamp(conn: sqlite3.Connection, ticker: str, window_start: str, window_end: str) -> Optional[str]:
    row = conn.execute(
        """SELECT MAX(datetime(COALESCE(published_at, fetched_at))) FROM articles
           WHERE ticker = ?
             AND datetime(COALESCE(published_at, fetched_at)) >= datetime(?)
             AND datetime(COALESCE(published_at, fetched_at)) < datetime(?)""",
        (ticker, window_start, window_end),
    ).fetchone()
    return row[0] if row else None


def build_articles_text(ticker: str, articles: List[Tuple]) -> str:
    blocks = []
    for i, article in enumerate(articles, 1):
        title, source, published_at, content, url = ((value or "").strip() for value in article)
        content = content[:MAX_CONTENT_CHARS] if content else "[No content fetched]"
        blocks.append(f"Article {i}\nTicker: {ticker}\nTitle: {title}\nSource: {source}\nPublished At: {published_at}\nURL: {url}\nContent:\n{content}")
    return "\n\n" + ("\n\n" + "-" * 80 + "\n\n").join(blocks)


def generate_ai_summary(client: OpenAI, ticker: str, articles: List[Tuple], report_date: date) -> str:
    if not articles:
        return f"{ticker} has no news for {report_date.isoformat()}."
    prompt = f"""
你是一位專業的財經新聞整合助手，正在為投資研究儀表板撰寫公司新聞摘要。
你會收到同一家公司（Ticker: {ticker}）在 {report_date.isoformat()} 日報固定 24 小時區間內的多篇新聞，請整合成一段繁體中文的公司層級摘要。
- 最前面先輸出：正面、負面或中性。
- 直接輸出單一緊湊段落，不要分段、不要條列。
- 整合相同事件，約150–250字，不要自行推論未提及內容。
- 若僅包含股價、估值、技術面、排行榜或缺乏公司層級資訊，僅輸出「來源內容無重大消息」。
- Reuters、Bloomberg、公司公告、SEC 及 Tier-1 sell-side 的重大內容，可至350–400字。
- 保留重要數字、產品、客戶、時程及管理層談話，語氣客觀。
以下是新聞資料：
{build_articles_text(ticker, articles)}
"""
    response = client.responses.create(model=MODEL, input=prompt)
    return (response.output_text or "").strip() or f"{ticker}: Summary generation returned empty output."


def save_company_digest(conn: sqlite3.Connection, report_date: date, ticker: str, window_start: str, window_end: str, summary: str, article_count: int) -> None:
    conn.execute(
        """INSERT INTO company_digest
           (report_date, ticker, window_start, window_end, article_count, summary, generated_at)
           VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(report_date, ticker) DO UPDATE SET
             window_start=excluded.window_start, window_end=excluded.window_end,
             article_count=excluded.article_count, summary=excluded.summary,
             generated_at=CURRENT_TIMESTAMP""",
        (report_date.isoformat(), ticker, window_start, window_end, article_count, summary),
    )
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a fixed daily company snapshot")
    parser.add_argument("--report-date", type=date.fromisoformat, default=None, help="Taipei report date (YYYY-MM-DD)")
    return parser.parse_args()


def main() -> None:
    report_date = parse_args().report_date or default_report_date()
    start_dt, end_dt = daily_window(report_date)
    window_start, window_end = utc_sql_timestamp(start_dt), utc_sql_timestamp(end_dt)
    print(f"Report date: {report_date}; fixed window: {window_start} UTC -> {window_end} UTC")
    conn = get_db_connection()
    try:
        ensure_company_digest_schema(conn)
        tickers = get_recent_tickers(conn, window_start, window_end)
        if not tickers:
            print("No articles found in the snapshot window.")
            return
        client = get_openai_client()
        for ticker in tickers:
            articles = get_articles_for_ticker(conn, ticker, window_start, window_end)
            try:
                print(f"ticker={ticker} | report_date={report_date} | article_count={len(articles)} | action=generate")
                summary = generate_ai_summary(client, ticker, articles, report_date)
                save_company_digest(conn, report_date, ticker, window_start, window_end, summary, len(articles))
            except Exception as exc:
                print(f"  Failed to summarize {ticker}: {exc}")
        print("Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
