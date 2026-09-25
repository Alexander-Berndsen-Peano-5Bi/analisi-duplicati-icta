import os, re, sqlite3, gzip, base64, hashlib
from pathlib import Path

BASE = Path(__file__).resolve().parent

# Rebuild the baseline seed from compact repository parts.
seed_dir = BASE / "seed"
seed_dir.mkdir(parents=True, exist_ok=True)
parts = sorted(seed_dir.glob("baseline_bundle_*.b64"))
seed_tsv = seed_dir / "baseline_01.tsv"
if parts and not seed_tsv.exists():
    payload = "".join(p.read_text(encoding="ascii").strip() for p in parts)
    seed_tsv.write_bytes(gzip.decompress(base64.b64decode(payload)))

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

if DATABASE_URL:
    import psycopg2
    from psycopg2.extras import DictCursor

    class CursorCompat:
        def __init__(self, cur=None, lastrowid=None):
            self.cur = cur
            self.lastrowid = lastrowid
        def fetchone(self):
            return self.cur.fetchone() if self.cur else None
        def fetchall(self):
            return self.cur.fetchall() if self.cur else []
        def __iter__(self):
            return iter(self.cur) if self.cur else iter(())
        @property
        def rowcount(self):
            return self.cur.rowcount if self.cur else 0

    def translate(sql):
        q = re.sub(r"datetime\('now'\)", "CAST(CURRENT_TIMESTAMP AS TEXT)", sql, flags=re.I)
        q = q.replace("?", "%s")
        if re.match(r"^\s*PRAGMA\b", q, flags=re.I):
            return None
        if re.match(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b", q, flags=re.I):
            q = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", q, count=1, flags=re.I)
            q = q.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        if re.match(r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\b", q, flags=re.I):
            q = re.sub(r"INSERT\s+OR\s+REPLACE\s+INTO", "INSERT INTO", q, count=1, flags=re.I)
            q = q.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return q

    class PGConnection:
        def __init__(self):
            self.raw = psycopg2.connect(DATABASE_URL, sslmode="require")
            self.row_factory = None
        def execute(self, sql, params=()):
            q = translate(sql)
            if q is None:
                return CursorCompat()
            wants_id = bool(re.match(r"^\s*INSERT\s+INTO\s+runs\s*\(", q, flags=re.I))
            if wants_id and "RETURNING" not in q.upper():
                q = q.rstrip().rstrip(";") + " RETURNING id"
            cur = self.raw.cursor(cursor_factory=DictCursor)
            cur.execute(q, params)
            lastrowid = None
            if wants_id:
                row = cur.fetchone()
                lastrowid = row[0] if row else None
            return CursorCompat(cur, lastrowid)
        def executemany(self, sql, rows):
            q = translate(sql)
            cur = self.raw.cursor(cursor_factory=DictCursor)
            cur.executemany(q, rows)
            return CursorCompat(cur)
        def executescript(self, script):
            cur = self.raw.cursor(cursor_factory=DictCursor)
            for stmt in script.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                stmt = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "BIGSERIAL PRIMARY KEY", stmt, flags=re.I)
                stmt = re.sub(r"^CREATE\s+TABLE\s+", "CREATE TABLE IF NOT EXISTS ", stmt, count=1, flags=re.I)
                stmt = re.sub(r"^CREATE\s+INDEX\s+", "CREATE INDEX IF NOT EXISTS ", stmt, count=1, flags=re.I)
                cur.execute(stmt)
            return CursorCompat(cur)
        def commit(self):
            self.raw.commit()
        def rollback(self):
            self.raw.rollback()
        def close(self):
            self.raw.close()

    sqlite3.connect = lambda *args, **kwargs: PGConnection()

# Load the application implementation.
source_b64 = (BASE / "bundle" / "app_source.b64").read_text(encoding="ascii")
source = gzip.decompress(base64.b64decode(source_b64)).decode("utf-8")
exec(compile(source, str(BASE / "app_impl.py"), "exec"), globals())

# Store and compare only SHA-256 fingerprints of normalized summaries.
_plain_normalize_summary = normalize_summary
def normalize_summary(text, ignore_platform=False):
    normalized = _plain_normalize_summary(text, ignore_platform)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
