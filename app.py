import os, re, sqlite3, gzip, base64, json, tempfile
from pathlib import Path

BASE = Path(__file__).resolve().parent
REAL_SQLITE_CONNECT = sqlite3.connect
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

def _pg_schema_and_seed(url):
    import psycopg2
    from psycopg2.extras import execute_values
    conn = psycopg2.connect(url, sslmode="require")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS consolidated_tests(
      id BIGSERIAL PRIMARY KEY,
      system_key TEXT NOT NULL,
      ticket_key TEXT NOT NULL,
      summary TEXT NOT NULL,
      normalized TEXT NOT NULL,
      source_period TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(system_key,ticket_key)
    );
    CREATE INDEX IF NOT EXISTS idx_consolidated_sys_norm ON consolidated_tests(system_key,normalized);
    CREATE TABLE IF NOT EXISTS runs(
      id BIGSERIAL PRIMARY KEY, period TEXT NOT NULL, system_key TEXT NOT NULL,
      imported_count INTEGER NOT NULL, duplicate_history_count INTEGER NOT NULL DEFAULT 0,
      duplicate_month_count INTEGER NOT NULL DEFAULT 0, new_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL, consolidated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS run_rows(
      id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL, ticket_key TEXT, summary TEXT,
      normalized TEXT, status TEXT, group_id TEXT, primary_ticket TEXT, source_row INTEGER
    );
    CREATE TABLE IF NOT EXISTS duplicate_groups(
      id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL, group_id TEXT, primary_ticket TEXT,
      primary_summary TEXT, group_size INTEGER, duplicates INTEGER, group_type TEXT
    );
    CREATE TABLE IF NOT EXISTS duplicate_mappings(
      id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL, group_id TEXT, primary_ticket TEXT,
      primary_summary TEXT, duplicate_ticket TEXT, duplicate_summary TEXT, duplicate_type TEXT
    );
    CREATE TABLE IF NOT EXISTS monthly_imports(
      period TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS system_metrics(
      system_key TEXT PRIMARY KEY, baseline_gross INTEGER NOT NULL DEFAULT 0,
      baseline_duplicates INTEGER NOT NULL DEFAULT 0, automated_count INTEGER NOT NULL DEFAULT 0
    );
    """)
    metrics=[
      ("PA",1669,107,0),("NAW",3750,1871,0),("NUP",9148,2041,0),
      ("PWBE",3853,551,0),("FB",2450,392,0),("NSL",197,11,0),
      ("AMUW",0,0,0),("IR",0,0,0),("SMARTC",0,0,0),("AE",0,0,0),("PCE",0,0,0)
    ]
    execute_values(cur, """INSERT INTO system_metrics(system_key,baseline_gross,baseline_duplicates,automated_count)
      VALUES %s ON CONFLICT(system_key) DO NOTHING""", metrics)
    cur.execute("SELECT COUNT(*) FROM consolidated_tests")
    count=cur.fetchone()[0]
    if count == 0:
        records=[]
        seed_dir = BASE / "seed"
        for seed in sorted(seed_dir.glob("baseline_*.tsv")):
            for line in seed.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                parts=line.split("\t")
                if len(parts) >= 3:
                    system_key, ticket_key, normalized = parts[0], parts[1], parts[2]
                    records.append((system_key, ticket_key, normalized, normalized, "baseline", "2026-09-25"))
        if records:
            execute_values(cur, """INSERT INTO consolidated_tests
              (system_key,ticket_key,summary,normalized,source_period,created_at)
              VALUES %s ON CONFLICT(system_key,ticket_key) DO NOTHING""", records, page_size=1000)
        print(f"POSTGRES_SEED_IMPORTED={len(records)}", flush=True)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM consolidated_tests")
    print(f"POSTGRES_CONSOLIDATED_COUNT={cur.fetchone()[0]}", flush=True)
    conn.close()

if DATABASE_URL:
    _pg_schema_and_seed(DATABASE_URL)
    import psycopg2
    from psycopg2.extras import DictCursor

    class CursorCompat:
        def __init__(self, cur=None, lastrowid=None):
            self.cur=cur; self.lastrowid=lastrowid
        def fetchone(self): return self.cur.fetchone() if self.cur else None
        def fetchall(self): return self.cur.fetchall() if self.cur else []
        def __iter__(self): return iter(self.cur) if self.cur else iter(())
        @property
        def rowcount(self): return self.cur.rowcount if self.cur else 0

    def _translate(sql):
        q=re.sub(r"datetime\('now'\)", "CAST(CURRENT_TIMESTAMP AS TEXT)", sql, flags=re.I)
        q=q.replace("?", "%s")
        if re.match(r"^\s*PRAGMA\b", q, flags=re.I): return None
        if re.match(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b", q, flags=re.I):
            q=re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO","INSERT INTO",q,count=1,flags=re.I)
            q=q.rstrip().rstrip(";")+" ON CONFLICT DO NOTHING"
        if re.match(r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\b", q, flags=re.I):
            q=re.sub(r"INSERT\s+OR\s+REPLACE\s+INTO","INSERT INTO",q,count=1,flags=re.I)
            q=q.rstrip().rstrip(";")+" ON CONFLICT DO NOTHING"
        return q

    class PGConnection:
        def __init__(self):
            self.raw=psycopg2.connect(DATABASE_URL, sslmode="require")
            self.row_factory=None
        def execute(self,sql,params=()):
            q=_translate(sql)
            if q is None: return CursorCompat()
            wants_id=bool(re.match(r"^\s*INSERT\s+INTO\s+runs\s*\(",q,flags=re.I))
            if wants_id and "RETURNING" not in q.upper():
                q=q.rstrip().rstrip(";")+" RETURNING id"
            cur=self.raw.cursor(cursor_factory=DictCursor)
            cur.execute(q,params)
            last=None
            if wants_id:
                row=cur.fetchone(); last=row[0] if row else None
            return CursorCompat(cur,last)
        def executemany(self,sql,rows):
            cur=self.raw.cursor(cursor_factory=DictCursor); cur.executemany(_translate(sql),rows); return CursorCompat(cur)
        def executescript(self, script):
            # Schema is initialized above for PostgreSQL; SQLite bootstrap scripts can be skipped.
            return CursorCompat()
        def commit(self): self.raw.commit()
        def rollback(self): self.raw.rollback()
        def close(self): self.raw.close()

    sqlite3.connect=lambda *args,**kwargs: PGConnection()

source_b64=(BASE/"bundle"/"app_source.b64").read_text(encoding="ascii")
source=gzip.decompress(base64.b64decode(source_b64)).decode("utf-8")
exec(compile(source,str(BASE/"app_impl.py"),"exec"),globals())
