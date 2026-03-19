import sqlite3
import time

from tqdm import tqdm

from cloudfiles.lib import sip
from cloudfiles import CloudFiles

# the maximum value of a host parameter number is 
# SQLITE_MAX_VARIABLE_NUMBER, which defaults to 999 
# for SQLite versions prior to 3.32.0 (2020-05-22) or 
# 32766 for SQLite versions after 3.32.0. 
# https://www.sqlite.org/limits.html
SQLITE_MAX_PARAMS = 999 if sqlite3.sqlite_version_info <= (3,32,0) else 32766
BIND = '?'

def init_db(filename:str) -> sqlite3.Connection:
  """Create tables, indexes, and tune SQLite for bulk-insert performance."""
  conn = sqlite3.connect(filename)

  conn.execute("PRAGMA journal_mode=WAL")
  conn.execute("PRAGMA synchronous=NORMAL")
  conn.execute("PRAGMA cache_size=-131072") # 128 MB (negative = kilobytes)

  conn.execute("""
  CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,
    size INTEGER
  )
  """)

  conn.execute("""
  CREATE TABLE IF NOT EXISTS checkpoint (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_token TEXT,
    rows_done INTEGER DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
  """)

  conn.execute(
    """
    INSERT OR IGNORE 
    INTO checkpoint (id, page_token, rows_done)
    VALUES (1, "", 0)
    """
  );

  conn.execute("""
  CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    value TEXT
  )
  """)

  conn.execute(
    """
    INSERT OR IGNORE 
    INTO metadata (id, name, value)
    VALUES (1, "cloudpath", "")
    """
  );

  conn.commit()

  return conn

def get_cloudpath(conn:sqlite3.Connection) -> str:
  return conn.execute(
    f"SELECT value FROM metadata where name = 'cloudpath'"
  ).fetchone()[0]

def save_cloudpath(conn:sqlite3.Connection, cloudpath:str):
  conn.execute(
    f"UPDATE metadata SET value = ? where name = 'cloudpath'", [ cloudpath ]
  )
  conn.commit()

def save_checkpoint(
  conn:sqlite3.Connection,
  page_token:str|None,
  rows_done:int,
) -> None:
  """Persist the current GCS page token and running row count."""
  conn.execute("""
    UPDATE checkpoint 
    SET 
      page_token = ?,
      rows_done = rows_done + ?
  """, (page_token, rows_done))

def load_checkpoint(conn:sqlite3.Connection) -> tuple[str|None,int]:
  """Return (page_token, rows_done) for a bucket, or (None, 0) if none."""
  row = conn.execute(
    "SELECT page_token, rows_done FROM checkpoint WHERE id = 1"
  ).fetchone()
  return (row[0], row[1]) if row else (None, 0)

def write_batch(
  conn:sqlite3.Connection,
  batch:list[tuple[str,int]],
  page_token:str|None,
) -> None:
  """Write a batch of (path, size) rows and save the checkpoint atomically."""
  for rows in sip(batch, SQLITE_MAX_PARAMS):
    bindslist = ",".join([f"({BIND}, {BIND})"] * len(rows))
    flat_rows = [ x for row in rows for x in row ]
    conn.execute(
      f"INSERT INTO files (path,size) VALUES {bindslist}",
      flat_rows
    )

def list(
  cloudpath:str,
  db_name:str,
  prefix:str = "",
  flat:bool = False,
  progress:bool = False,
):
  with init_db(db_name) as conn:

    saved_cloudpath = get_cloudpath(conn)
    if saved_cloudpath not in ("", cloudpath):
      print(
        f"Cloudpath does not match database. Aborting.\n"
        f"Saved: {saved_cloudpath}\n"
        f"Argument: {cloudpath}"
      )
      return

    cf = CloudFiles(cloudpath)

    save_cloudpath(conn, cloudpath)
    page_token, rows_done = load_checkpoint(conn)

    if progress and page_token is not None:
      print(f"Resuming from {rows_done} rows.")

    iterator = cf.list(
      prefix=prefix,
      flat=flat,
      size=True,
      resume_token=page_token,
      return_resume_token=True,
    )

    pbar = tqdm(
      disable=(not progress), 
      desc="Files", 
      initial=rows_done,
    )

    with pbar:
      for batch in sip(iterator, 5000):
        latest_token = batch[-1][2]
        batch = [ (row[0], row[1]) for row in batch ]
        conn.execute("BEGIN TRANSACTION")
        write_batch(conn, batch, latest_token)
        save_checkpoint(conn, latest_token, len(batch))
        conn.commit()

        pbar.update(len(batch))

    if progress:
      print("Creating index.")

    s = time.monotonic()
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
    elapsed = time.monotonic() - s
    if progress:
      print(f"done in {elapsed:.3f} sec.")
