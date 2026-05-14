import importlib
import sqlite3
import sys
import types
import unittest
from datetime import datetime, timezone


if "bs4" not in sys.modules:
    bs4_stub = types.ModuleType("bs4")
    bs4_stub.BeautifulSoup = object
    sys.modules["bs4"] = bs4_stub
sys.modules.setdefault("trafilatura", types.SimpleNamespace(extract=lambda *args, **kwargs: ""))
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))
if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")
    openai_stub.OpenAI = object
    sys.modules["openai"] = openai_stub

sec_docs = importlib.import_module("app.processors.process_sec_documents")


def doc(filing_id, document_type, title, url, clean_text, status="ok"):
    return (filing_id, document_type, title, url, clean_text, status)


class ProcessSecDocumentsSelectionTest(unittest.TestCase):
    def test_existing_8k_ex99_1_path_remains_unchanged(self):
        documents = [
            doc(1, "8-K", "Primary 8-K", "https://www.sec.gov/Archives/edgar/data/1/8k.htm", "primary text"),
            doc(1, "EX-99.1", "EX-99.1", "https://www.sec.gov/Archives/edgar/data/1/ex991.htm", "ex99 text"),
            doc(1, "EX-99.2", "EX-99.2", "https://www.sec.gov/Archives/edgar/data/1/ex992.htm", "other text"),
        ]

        selected = sec_docs.select_documents_for_summary("8-K", documents)

        self.assertEqual([row["document_type"] for row in selected], ["8-K", "EX-99.1"])
        self.assertEqual(selected[0]["clean_text_excerpt"], "primary text")
        self.assertEqual(selected[1]["clean_text_excerpt"], "ex99 text")


    def test_existing_8k_prompt_structure_remains_unchanged(self):
        filing = (1, "TST", "2026-05-14 12:00:00", "2026-05-14", "2.02", "Test 8-K", "8-K")
        documents = [
            doc(1, "8-K", "Primary 8-K", "https://www.sec.gov/Archives/edgar/data/1/8k.htm", "primary text"),
            doc(1, "EX-99.1", "EX-99.1", "https://www.sec.gov/Archives/edgar/data/1/ex991.htm", "ex99 text"),
        ]

        prompt = sec_docs.build_digest_prompt("TST", 24, [filing], documents)

        self.assertNotIn("form_type: 8-K", prompt)
        self.assertIn("Filing 1\naccepted_time: 2026-05-14 12:00:00", prompt)
        self.assertIn("document_type: 8-K", prompt)
        self.assertIn("document_type: EX-99.1", prompt)

    def test_existing_8k_lifecycle_does_not_use_new_empty_document_guard(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        sec_docs.ensure_schema(conn)
        accepted = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            INSERT INTO sec_filings (
                ticker, cik, accession_number, form_type, filing_date,
                accepted_datetime, report_date, item_numbers, primary_doc,
                primary_doc_url, filing_detail_url, title
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "TST", "0000000001", "0000000001-26-000001", "8-K",
                "2026-05-14", accepted, "2026-05-14", "2.02",
                "8k.htm", "https://www.sec.gov/Archives/edgar/data/1/8k.htm",
                "https://www.sec.gov/Archives/edgar/data/1/index.htm", "Test 8-K",
            ),
        )
        filing_id = conn.execute("SELECT id FROM sec_filings").fetchone()[0]
        conn.execute(
            """
            INSERT INTO sec_documents (
                filing_id, ticker, cik, accession_number, document_type,
                document_title, document_url, raw_text, clean_text,
                content_status, content_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                filing_id, "TST", "0000000001", "0000000001-26-000001",
                "EX-99.2", "EX-99.2",
                "https://www.sec.gov/Archives/edgar/data/1/ex992.htm",
                "other text", "other text", "ok", "",
            ),
        )
        conn.commit()

        original_generate = sec_docs.generate_sec_digest
        try:
            sec_docs.generate_sec_digest = lambda client, ticker, window_hours, filings, documents: "中性 test"
            result = sec_docs.maybe_save_sec_digest(conn, object(), "TST", 24, force=True)
        finally:
            sec_docs.generate_sec_digest = original_generate
            conn.close()

        self.assertEqual(result, "generated")

    def test_6k_with_ex99_1_uses_ex99_1(self):
        documents = [
            doc(1, "6-K", "Primary 6-K", "https://www.sec.gov/Archives/edgar/data/1/6k.htm", "primary text"),
            doc(1, "EX-99.1", "EX-99.1", "https://www.sec.gov/Archives/edgar/data/1/ex991.htm", "ex99 text"),
            doc(1, "EX-99.2", "EX-99.2", "https://www.sec.gov/Archives/edgar/data/1/ex992.htm", "other text"),
        ]

        selected = sec_docs.select_documents_for_summary("6-K", documents)

        self.assertEqual([row["document_type"] for row in selected], ["EX-99.1"])
        self.assertEqual(selected[0]["clean_text_excerpt"], "ex99 text")

    def test_6k_without_ex99_1_uses_fallback(self):
        fallback_text = "Revenue and earnings results " * 8
        documents = [
            doc(1, "6-K", "Primary 6-K", "https://www.sec.gov/Archives/edgar/data/1/6k.htm", "primary text"),
            doc(1, "EX-99.2", "EX-99.2", "https://www.sec.gov/Archives/edgar/data/1/ex992.htm", fallback_text),
        ]

        selected = sec_docs.select_documents_for_summary("6-K", documents)

        self.assertEqual([row["document_type"] for row in selected], ["EX-99.2"])
        self.assertEqual(selected[0]["clean_text_excerpt"], fallback_text.strip())

    def test_empty_6k_fallback_document_is_ignored(self):
        documents = [
            doc(1, "EX-99.2", "EX-99.2", "https://www.sec.gov/Archives/edgar/data/1/ex992.htm", ""),
            doc(1, "6-K", "Primary 6-K", "https://www.sec.gov/Archives/edgar/data/1/6k.htm", "tiny"),
        ]

        selected = sec_docs.select_documents_for_summary("6-K", documents)

        self.assertEqual(selected, [])


if __name__ == "__main__":
    unittest.main()
