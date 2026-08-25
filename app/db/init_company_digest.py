import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.company_digest_schema import ensure_company_digest_schema


conn = sqlite3.connect("data/news.db")
ensure_company_digest_schema(conn)
conn.close()
print("company_digest daily snapshot table is ready.")
