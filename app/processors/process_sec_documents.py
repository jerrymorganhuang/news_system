import argparse
import hashlib
import os
import re
import sqlite3
import urllib.request
from urllib.parse import urljoin
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
import trafilatura
from dotenv import load_dotenv
from openai import OpenAI


BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "news.db"
ENV_PATH = BASE_DIR / ".env"

SEC_TIMEOUT_SECONDS = float(os.getenv("SEC_TIMEOUT_SECONDS", "20"))
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "news_system/1.0 (SEC document processing; contact: devnull@example.com)",
)
MODEL = "gpt-5-mini"
SUPPORTED_WINDOWS = {24, 48}
MAX_EXCERPT_CHARS = 1200
MAX_DOCS_PER_FILING = 3
EX99_FALLBACK_CHARS = 9000
EX99_PAGE_LIMIT = 2

EXHIBIT_RULES = [
    ("EX-99.1", re.compile(r"\b(?:EX\s*[-.]?\s*)?99\s*[-.]?\s*0?1\b", re.IGNORECASE)),
    ("EX-99.2", re.compile(r"\b(?:EX\s*[-.]?\s*)?99\s*[-.]?\s*0?2\b", re.IGNORECASE)),
]
EXHIBIT_URL_HINTS = {
    "EX-99.1": re.compile(r"(?:ex(?:hibit)?[\W_]*99[\W_]*0?1|99[\W_]*0?1)", re.IGNORECASE),
    "EX-99.2": re.compile(r"(?:ex(?:hibit)?[\W_]*99[\W_]*0?2|99[\W_]*0?2)", re.IGNORECASE),
}
EXHIBIT_NEARBY_HINTS = {
    "EX-99.1": re.compile(r"(earnings|press\s+release)", re.IGNORECASE),
    "EX-99.2": re.compile(r"(financial\s+information|segment\s+recast)", re.IGNORECASE),
}

BOILERPLATE_PATTERNS = [
    re.compile(r"^\s*UNITED\s+STATES\s+SECURITIES\s+AND\s+EXCHANGE\s+COMMISSION\s*$", re.IGNORECASE),
    re.compile(r"^\s*Washington,\s*D\.C\.?\s*20549\s*$", re.IGNORECASE),
    re.compile(r"^\s*FORM\s+8-K\s*$", re.IGNORECASE),
    re.compile(r"^\s*Commission\s+File\s+Number", re.IGNORECASE),
    re.compile(r"^\s*SIGNATURES?\s*$", re.IGNORECASE),
]


def get_openai_client() -> OpenAI:
    load_dotenv(ENV_PATH)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not found in .env")
    return OpenAI(api_key=api_key)


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def ensure_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
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
        """
    )
    cursor.execute(
        """
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
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sec_documents_ticker ON sec_documents(ticker)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sec_documents_filing_id ON sec_documents(filing_id)")

    cursor.execute(
        """
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
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_digest_ticker_window ON sec_digest(ticker, window_hours)"
    )
    conn.commit()


def parse_sec_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r"\d{14}", text):
        try:
            return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    normalized = text.replace("T", " ").replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def fetch_url_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": SEC_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=SEC_TIMEOUT_SECONDS) as resp:
        payload = resp.read()
        encoding = resp.headers.get_content_charset() or "utf-8"
    return payload.decode(encoding, errors="replace")


def detect_exhibit_type(*candidates: str) -> Optional[str]:
    joined = " | ".join((c or "") for c in candidates)
    for normalized, pattern in EXHIBIT_RULES:
        if pattern.search(joined):
            return normalized
    return None


def absolutize_sec_url(link: str, detail_url: str) -> str:
    link = (link or "").strip()
    if not link:
        return ""
    if link.startswith("http://") or link.startswith("https://"):
        return link
    if link.startswith("/"):
        return f"https://www.sec.gov{link}"
    return urljoin(detail_url, link)


def detect_exhibit_from_link(anchor_text: str, href: str, nearby_text: str = "") -> Optional[str]:
    by_text = detect_exhibit_type(anchor_text, nearby_text)
    if by_text:
        return by_text
    target = f"{href} {anchor_text} {nearby_text}".lower()
    for exhibit_type, pattern in EXHIBIT_URL_HINTS.items():
        if pattern.search(target):
            return exhibit_type
    for exhibit_type, pattern in EXHIBIT_NEARBY_HINTS.items():
        if pattern.search(nearby_text):
            return exhibit_type
    return None


def extract_exhibit_links(source_url: str) -> List[Dict[str, str]]:
    html = fetch_url_text(source_url)
    soup = BeautifulSoup(html, "html.parser")

    found: List[Dict[str, str]] = []
    seen = set()

    for row in soup.select("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        row_text = " | ".join(cell.get_text(" ", strip=True) for cell in cells)
        exhibit_type = detect_exhibit_type(row_text)
        if not exhibit_type:
            continue

        link_tag = row.find("a", href=True)
        if not link_tag:
            continue

        doc_url = absolutize_sec_url(link_tag.get("href", ""), source_url)
        if not doc_url or doc_url in seen:
            continue
        seen.add(doc_url)

        title_candidates = [
            cells[1].get_text(" ", strip=True) if len(cells) > 1 else "",
            cells[2].get_text(" ", strip=True) if len(cells) > 2 else "",
            link_tag.get_text(" ", strip=True),
        ]
        doc_title = next((t for t in title_candidates if t), "")

        found.append(
            {
                "document_type": exhibit_type,
                "document_title": doc_title,
                "document_url": doc_url,
            }
        )

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "")
        anchor_text = anchor.get_text(" ", strip=True)
        if not href:
            continue

        container_text = ""
        if anchor.parent:
            container_text = anchor.parent.get_text(" ", strip=True)
        nearby_text = f"{anchor_text} {container_text}".strip()
        exhibit_type = detect_exhibit_from_link(anchor_text, href, nearby_text)
        if not exhibit_type:
            continue

        doc_url = absolutize_sec_url(href, source_url)
        if not doc_url or doc_url in seen:
            continue
        seen.add(doc_url)

        title = anchor_text or container_text or exhibit_type
        found.append(
            {
                "document_type": exhibit_type,
                "document_title": title,
                "document_url": doc_url,
            }
        )

    found.sort(
        key=lambda row: (
            0 if row["document_type"] == "EX-99.1" else 1 if row["document_type"] == "EX-99.2" else 9,
            row["document_url"],
        )
    )
    return found


def clean_extracted_text(raw_text: str) -> str:
    if not raw_text:
        return ""

    lines = [ln.strip() for ln in raw_text.replace("\r", "\n").split("\n")]
    cleaned_lines: List[str] = []
    seen = set()

    for line in lines:
        if len(line) < 2:
            continue
        if re.fullmatch(r"[-_=*\s]{3,}", line):
            continue

        skip = False
        for pat in BOILERPLATE_PATTERNS:
            if pat.search(line):
                skip = True
                break
        if skip:
            continue

        norm = re.sub(r"\s+", " ", line)
        if norm in seen:
            continue
        seen.add(norm)
        cleaned_lines.append(norm)

    return "\n".join(cleaned_lines)


def extract_document_text(document_url: str) -> Dict[str, str]:
    lowered = document_url.lower()
    if lowered.endswith(".pdf"):
        return {
            "raw_text": "",
            "clean_text": "",
            "content_status": "unsupported_pdf",
            "content_error": "PDF OCR is disabled",
        }

    raw_html = fetch_url_text(document_url)

    extracted = trafilatura.extract(raw_html, include_comments=False, include_tables=True)
    if extracted and extracted.strip():
        raw_text = extracted.strip()
        clean_text = clean_extracted_text(raw_text)
        return {
            "raw_text": raw_text,
            "clean_text": clean_text,
            "content_status": "ok",
            "content_error": "",
        }

    soup = BeautifulSoup(raw_html, "html.parser")
    fallback_text = soup.get_text("\n", strip=True)
    clean_text = clean_extracted_text(fallback_text)
    return {
        "raw_text": fallback_text,
        "clean_text": clean_text,
        "content_status": "ok_fallback_bs4",
        "content_error": "",
    }


def upsert_sec_document(conn: sqlite3.Connection, filing: sqlite3.Row, document: Dict[str, str]) -> None:
    cursor = conn.cursor()
    existing = cursor.execute(
        """
        SELECT id, clean_text, content_status
        FROM sec_documents
        WHERE cik = ?
          AND accession_number = ?
          AND document_url = ?
        LIMIT 1
        """,
        (filing["cik"], filing["accession_number"], document["document_url"]),
    ).fetchone()

    try:
        extracted = extract_document_text(document["document_url"])
    except Exception as exc:
        extracted = {
            "raw_text": "",
            "clean_text": "",
            "content_status": "fetch_error",
            "content_error": str(exc),
        }

    if existing:
        if (existing[1] or "") == (extracted["clean_text"] or "") and (existing[2] or "") == extracted["content_status"]:
            cursor.execute(
                """
                UPDATE sec_documents
                SET document_type = ?,
                    document_title = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (document["document_type"], document["document_title"], existing[0]),
            )
        else:
            cursor.execute(
                """
                UPDATE sec_documents
                SET filing_id = ?,
                    ticker = ?,
                    document_type = ?,
                    document_title = ?,
                    raw_text = ?,
                    clean_text = ?,
                    content_status = ?,
                    content_error = ?,
                    fetched_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    filing["id"],
                    filing["ticker"],
                    document["document_type"],
                    document["document_title"],
                    extracted["raw_text"],
                    extracted["clean_text"],
                    extracted["content_status"],
                    extracted["content_error"],
                    existing[0],
                ),
            )
    else:
        cursor.execute(
            """
            INSERT INTO sec_documents (
                filing_id,
                ticker,
                cik,
                accession_number,
                document_type,
                document_title,
                document_url,
                raw_text,
                clean_text,
                content_status,
                content_error,
                fetched_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                filing["id"],
                filing["ticker"],
                filing["cik"],
                filing["accession_number"],
                document["document_type"],
                document["document_title"],
                document["document_url"],
                extracted["raw_text"],
                extracted["clean_text"],
                extracted["content_status"],
                extracted["content_error"],
            ),
        )


def get_recent_filings(conn: sqlite3.Connection, window_hours: int) -> List[sqlite3.Row]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, ticker, cik, accession_number, accepted_datetime, filing_date, item_numbers, title, filing_detail_url, primary_doc_url
        FROM sec_filings
        ORDER BY datetime(COALESCE(filing_date, accepted_datetime)) DESC
        """
    )

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    filings = []
    for row in cursor.fetchall():
        accepted_dt = parse_sec_datetime(row[4]) or parse_sec_datetime(row[5])
        if accepted_dt and accepted_dt >= cutoff:
            filings.append(row)
    return filings


def process_documents(conn: sqlite3.Connection, filings: List[sqlite3.Row]) -> None:
    for filing in filings:
        filing_dict = {
            "id": filing[0],
            "ticker": filing[1],
            "cik": filing[2],
            "accession_number": filing[3],
            "accepted_datetime": filing[4],
            "filing_date": filing[5],
            "item_numbers": filing[6],
            "title": filing[7],
            "filing_detail_url": filing[8],
            "primary_doc_url": filing[9],
        }
        if not filing_dict["filing_detail_url"] and not filing_dict["primary_doc_url"]:
            continue
        primary_url = filing_dict["primary_doc_url"] or filing_dict["filing_detail_url"]
        upsert_sec_document(
            conn,
            filing_dict,
            {
                "document_type": "8-K",
                "document_title": filing_dict["title"] or "Primary 8-K",
                "document_url": primary_url,
            },
        )

        exhibit_sources = [u for u in [filing_dict["primary_doc_url"], filing_dict["filing_detail_url"]] if u]
        seen_sources = set()
        exhibits: List[Dict[str, str]] = []
        for source_url in exhibit_sources:
            if source_url in seen_sources:
                continue
            seen_sources.add(source_url)
            try:
                exhibits.extend(extract_exhibit_links(source_url))
            except Exception as exc:
                print(
                    f"[SEC DOC] ticker={filing_dict['ticker']} accession={filing_dict['accession_number']} detect_error source={source_url} err={exc}"
                )

        deduped = {}
        for exhibit in exhibits:
            deduped[exhibit["document_url"]] = exhibit
        for exhibit in deduped.values():
            upsert_sec_document(conn, filing_dict, exhibit)

    conn.commit()


def get_tickers_for_window(conn: sqlite3.Connection, window_hours: int) -> List[str]:
    cursor = conn.cursor()
    rows = cursor.execute(
        """
        SELECT DISTINCT ticker
        FROM sec_filings
        """
    ).fetchall()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    tickers = []
    for (ticker,) in rows:
        latest = cursor.execute(
            """
            SELECT accepted_datetime, filing_date
            FROM sec_filings
            WHERE ticker = ?
            ORDER BY datetime(COALESCE(filing_date, accepted_datetime)) DESC
            LIMIT 1
            """,
            (ticker,),
        ).fetchone()
        if not latest:
            continue
        latest_dt = parse_sec_datetime(latest[0]) or parse_sec_datetime(latest[1])
        if latest_dt and latest_dt >= cutoff:
            tickers.append(ticker)

    return sorted(tickers)


def fetch_window_data(conn: sqlite3.Connection, ticker: str, window_hours: int) -> Dict[str, List[sqlite3.Row]]:
    cursor = conn.cursor()
    filings_all = cursor.execute(
        """
        SELECT id, ticker, accepted_datetime, filing_date, item_numbers, title
        FROM sec_filings
        WHERE ticker = ?
        ORDER BY datetime(COALESCE(filing_date, accepted_datetime)) DESC
        """,
        (ticker,),
    ).fetchall()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    filings = []
    filing_ids = []
    for row in filings_all:
        row_dt = parse_sec_datetime(row[2]) or parse_sec_datetime(row[3])
        if row_dt and row_dt >= cutoff:
            filings.append(row)
            filing_ids.append(row[0])

    documents = []
    if filing_ids:
        placeholders = ",".join("?" for _ in filing_ids)
        documents = cursor.execute(
            f"""
            SELECT filing_id, document_type, document_title, document_url, clean_text, content_status
            FROM sec_documents
            WHERE ticker = ?
              AND filing_id IN ({placeholders})
            ORDER BY CASE document_type WHEN 'EX-99.1' THEN 0 WHEN 'EX-99.2' THEN 1 ELSE 9 END, id DESC
            """,
            [ticker, *filing_ids],
        ).fetchall()

    return {"filings": filings, "documents": documents}


def compute_window_end(filings: List[sqlite3.Row], fallback_hours: int) -> str:
    timestamps = []
    for row in filings:
        dt = parse_sec_datetime(row[2]) or parse_sec_datetime(row[3])
        if dt:
            timestamps.append(dt)
    if timestamps:
        return max(timestamps).strftime("%Y-%m-%d %H:%M:%S")
    now_dt = datetime.now(timezone.utc)
    return (now_dt - timedelta(hours=fallback_hours)).strftime("%Y-%m-%d %H:%M:%S")


def get_latest_digest(conn: sqlite3.Connection, ticker: str, window_hours: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, window_end
        FROM sec_digest
        WHERE ticker = ?
          AND window_hours = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        (ticker, window_hours),
    ).fetchone()






def extract_first_meaningful_ex99_part(clean_text: str) -> str:
    text = (clean_text or "").strip()
    if not text:
        return ""

    pages = [p.strip() for p in re.split(r"\f+", text) if p.strip()]
    if len(pages) >= EX99_PAGE_LIMIT:
        return "\n\n".join(pages[:EX99_PAGE_LIMIT]).strip()

    marker_matches = list(re.finditer(r"(?im)^\s*(?:page|pg\.?)\s*\d+\s*(?:of\s*\d+)?\s*$", text))
    if marker_matches:
        cuts = [m.start() for m in marker_matches]
        if len(cuts) > EX99_PAGE_LIMIT:
            return text[: cuts[EX99_PAGE_LIMIT]].strip()

    return text[:EX99_FALLBACK_CHARS].strip()


def select_documents_for_summary(documents: List[sqlite3.Row]) -> List[Dict[str, str]]:
    primary_8k = None
    ex99_1 = None

    for doc in documents:
        doc_type = (doc[1] or "").upper().strip()
        if primary_8k is None and doc_type == "8-K":
            primary_8k = doc
        normalized_exhibit_type = detect_exhibit_type(doc_type, doc[2] or "", doc[3] or "")
        if ex99_1 is None and normalized_exhibit_type == "EX-99.1":
            ex99_1 = doc

    selected: List[Dict[str, str]] = []

    if primary_8k is not None:
        selected.append({
            "document_type": primary_8k[1],
            "document_title": primary_8k[2] or "N/A",
            "content_status": primary_8k[5] or "N/A",
            "clean_text_excerpt": (primary_8k[4] or "").strip() or "[No text extracted]",
        })

    if ex99_1 is not None and (ex99_1[4] or "").strip():
        excerpt = extract_first_meaningful_ex99_part(ex99_1[4] or "")
        selected.append({
            "document_type": ex99_1[1],
            "document_title": ex99_1[2] or "N/A",
            "content_status": ex99_1[5] or "N/A",
            "clean_text_excerpt": excerpt or "[No text extracted]",
        })

    return selected

def rank_document_for_digest(doc: sqlite3.Row) -> tuple:
    document_type = (doc[1] or '').upper()
    title = (doc[2] or '').lower()
    url = (doc[3] or '').lower()
    clean_text = (doc[4] or '').strip()

    if document_type == 'EX-99.1':
        priority = 0
    elif document_type == 'EX-99.2':
        priority = 1
    elif '8-k' in document_type or '8k' in document_type or '8-k' in title or '8-k' in url:
        priority = 2
    else:
        priority = 3

    has_text = 0 if clean_text else 1
    return (priority, has_text, -len(clean_text))


def select_documents_for_filing(documents: List[sqlite3.Row]) -> List[sqlite3.Row]:
    ranked = sorted(documents, key=rank_document_for_digest)
    return ranked[:MAX_DOCS_PER_FILING]

def build_digest_prompt(ticker: str, window_hours: int, filings: List[sqlite3.Row], documents: List[sqlite3.Row]) -> str:
    doc_by_filing: Dict[int, List[sqlite3.Row]] = {}
    for doc in documents:
        doc_by_filing.setdefault(doc[0], []).append(doc)

    filing_blocks = []
    for i, filing in enumerate(filings, start=1):
        filing_id, _, accepted_datetime, filing_date, item_numbers, title = filing
        accepted = accepted_datetime or filing_date or ""
        lines = [
            f"Filing {i}",
            f"accepted_time: {accepted}",
            f"item_numbers: {item_numbers or 'N/A'}",
            f"title: {title or 'N/A'}",
        ]
        selected_docs = select_documents_for_summary(doc_by_filing.get(filing_id, []))
        for doc in selected_docs:
            lines.extend(
                [
                    f"document_type: {doc['document_type']}",
                    f"document_title: {doc['document_title']}",
                    f"content_status: {doc['content_status']}",
                    f"clean_text_excerpt:\n{doc['clean_text_excerpt']}",
                ]
            )
        filing_blocks.append("\n".join(lines))

    filings_text = "\n\n" + "\n\n".join(filing_blocks)

    return f"""
你是專業的 SEC 8-K 申報摘要助手。請僅根據以下官方申報資料，為 {ticker} 產生一段繁體中文摘要。
時間窗：最近 {window_hours} 小時。

要求：
- 輸出僅一段，約 120–250 字。
- 第一個詞必須是「正面」或「中性」或「負面」。
- 語氣客觀，不可推測，不可排名。
- 聚焦：公司最近正式揭露事項、關鍵數字、指引調整、合約/M&A/融資/產品進展、管理層變動與風險。
- 若沒有實質內容，明確寫出「無重大實質揭露」。
- 若有 EX-99.1，優先使用其 clean_text_excerpt 中的財務數字（營收、EPS、YoY、指引）作摘要依據，不可忽略。

SEC filing data:
{filings_text}
""".strip()


def generate_sec_digest(client: OpenAI, ticker: str, window_hours: int, filings: List[sqlite3.Row], documents: List[sqlite3.Row]) -> str:
    if not filings:
        return ""
    prompt = build_digest_prompt(ticker, window_hours, filings, documents)
    response = client.responses.create(model=MODEL, input=prompt)
    return (response.output_text or "").strip()


def maybe_save_sec_digest(
    conn: sqlite3.Connection,
    client: OpenAI,
    ticker: str,
    window_hours: int,
    force: bool,
) -> str:
    window_start = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).strftime("%Y-%m-%d %H:%M:%S")
    data = fetch_window_data(conn, ticker, window_hours)
    filings = data["filings"]
    documents = data["documents"]

    if not filings:
        print(f"[SEC DIGEST] ticker={ticker} window={window_hours}h action=skip reason=no_new_sec_data")
        return "skip"

    window_end = compute_window_end(filings, window_hours)
    latest_digest = get_latest_digest(conn, ticker, window_hours)
    if latest_digest and (latest_digest[1] or "") == window_end and not force:
        print(f"[SEC DIGEST] ticker={ticker} window={window_hours}h action=skip reason=existing_same_window_end")
        return "skip"

    fingerprint_input = "\n".join((doc[4] or "") for doc in documents if (doc[4] or "").strip())
    fingerprint = hashlib.sha256(fingerprint_input.encode("utf-8")).hexdigest()

    prior_same_fingerprint = conn.execute(
        """
        SELECT id
        FROM sec_digest
        WHERE ticker = ?
          AND window_hours = ?
          AND summary_zh LIKE ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        (ticker, window_hours, f"%[fp:{fingerprint}]"),
    ).fetchone()
    if prior_same_fingerprint and not force:
        print(f"[SEC DIGEST] ticker={ticker} window={window_hours}h action=skip reason=clean_text_unchanged")
        return "skip"

    summary = generate_sec_digest(client, ticker, window_hours, filings, documents)
    if not summary:
        print(f"[SEC DIGEST] ticker={ticker} window={window_hours}h action=skip reason=empty_summary")
        return "skip"

    summary_with_fp = f"{summary}\n[fp:{fingerprint}]"

    conn.execute(
        """
        INSERT OR REPLACE INTO sec_digest (
            ticker,
            window_hours,
            window_start,
            window_end,
            filing_count,
            document_count,
            summary_zh,
            generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            ticker,
            window_hours,
            window_start,
            window_end,
            len(filings),
            len(documents),
            summary_with_fp,
        ),
    )
    conn.commit()
    print(
        f"[SEC DIGEST] ticker={ticker} window={window_hours}h action=generate filings={len(filings)} documents={len(documents)}"
    )
    return "generated"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process SEC exhibits and generate ticker-level SEC digest")
    parser.add_argument("--window-hours", type=int, default=24, help="Digest window in hours (24 or 48)")
    parser.add_argument("--limit", type=int, default=0, help="Limit tickers for digest generation")
    parser.add_argument("--force", action="store_true", help="Force regeneration even when unchanged")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.window_hours not in SUPPORTED_WINDOWS:
        raise ValueError(f"Unsupported --window-hours={args.window_hours}. Supported values: {sorted(SUPPORTED_WINDOWS)}")

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)

        filings = get_recent_filings(conn, args.window_hours)
        if not filings:
            print(f"No recent SEC filings found in last {args.window_hours}h.")
            return

        process_documents(conn, filings)

        tickers = get_tickers_for_window(conn, args.window_hours)
        if args.limit and args.limit > 0:
            tickers = tickers[: args.limit]

        if not tickers:
            print("No tickers eligible for SEC digest generation.")
            return

        client = get_openai_client()
        generated = 0
        for ticker in tickers:
            result = maybe_save_sec_digest(
                conn=conn,
                client=client,
                ticker=ticker,
                window_hours=args.window_hours,
                force=args.force,
            )
            if result == "generated":
                generated += 1

        print(f"Done. SEC digests generated: {generated}/{len(tickers)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
