"""meta.sqlite schema + helpers (P1-A).

This is the minimum structured index for MU records.

Tables:
- mu: core fields (mu_id primary key)
- tag: tag dictionary
- mu_tag: many-to-many
- mu_fts: FTS5 over summary (and optional extra text)

We keep the schema intentionally small and migration-friendly.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS mu (
  mu_id TEXT PRIMARY KEY,
  time TEXT,
  summary TEXT,
  content_hash TEXT,
  mu_key TEXT,
  privacy_level TEXT,
  corrects_json TEXT,
  supersedes_json TEXT,
  duplicate_of_json TEXT,
  tombstone_json TEXT,
  source_kind TEXT,
  source_note TEXT,
  path TEXT,
  mtime REAL
);

CREATE TABLE IF NOT EXISTS tag (
  tag TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS mu_tag (
  mu_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  PRIMARY KEY (mu_id, tag),
  FOREIGN KEY (mu_id) REFERENCES mu(mu_id) ON DELETE CASCADE,
  FOREIGN KEY (tag) REFERENCES tag(tag) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE IF NOT EXISTS mu_fts USING fts5(
  mu_id UNINDEXED,
  summary,
  content='mu',
  content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS mu_ai AFTER INSERT ON mu BEGIN
  INSERT INTO mu_fts(rowid, mu_id, summary) VALUES (new.rowid, new.mu_id, coalesce(new.summary,''));
END;

CREATE TRIGGER IF NOT EXISTS mu_ad AFTER DELETE ON mu BEGIN
  INSERT INTO mu_fts(mu_fts, rowid, mu_id, summary) VALUES ('delete', old.rowid, old.mu_id, old.summary);
END;

CREATE TRIGGER IF NOT EXISTS mu_au AFTER UPDATE ON mu BEGIN
  INSERT INTO mu_fts(mu_fts, rowid, mu_id, summary) VALUES ('delete', old.rowid, old.mu_id, old.summary);
  INSERT INTO mu_fts(rowid, mu_id, summary) VALUES (new.rowid, new.mu_id, coalesce(new.summary,''));
END;

CREATE INDEX IF NOT EXISTS idx_mu_time ON mu(time);
CREATE INDEX IF NOT EXISTS idx_mu_privacy ON mu(privacy_level);

-- view cache (P1-D)
CREATE TABLE IF NOT EXISTS view_cache (
  view_id TEXT PRIMARY KEY,
  template TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  source_mu_ids_json TEXT NOT NULL,
  source_mu_hash TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  stale INTEGER NOT NULL DEFAULT 0,
  content_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_view_template ON view_cache(template);
CREATE INDEX IF NOT EXISTS idx_view_stale ON view_cache(stale);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(
    conn: sqlite3.Connection, table: str, col: str, coltype: str
) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    have = {r[1] for r in rows}
    if col not in have:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")


def init_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        # lightweight migrations (non-destructive)
        _ensure_column(conn, "mu", "supersedes_json", "TEXT")
        _ensure_column(conn, "mu", "duplicate_of_json", "TEXT")


def reset_db(db_path: Path) -> None:
    # Drop tables/virtual tables and rebuild.
    with connect(db_path) as conn:
        conn.executescript(
            """
            DROP TABLE IF EXISTS mu_tag;
            DROP TABLE IF EXISTS tag;
            DROP TABLE IF EXISTS mu;
            -- note: schema migrations are handled in init_db (ALTER TABLE ADD COLUMN)
            DROP TABLE IF EXISTS mu_fts;
            """
        )
    init_db(db_path)
