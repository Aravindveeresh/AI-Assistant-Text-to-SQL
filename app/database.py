"""
SQLite database setup for the Text-to-SQL project.

Normalized dimensions: periods, ports
Financials: balance_sheet, cash_flow, quarterly_pnl, consolidated_pnl, roce
Operations: volumes, containers, roro
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

DB_PATH = "data/company.db"

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """
    Create (if needed) and return a configured SQLite connection.

    - Ensures data/ dir exists
    - Enables WAL mode and foreign keys
    - Sets row_factory to sqlite3.Row for dict-like rows
    - check_same_thread=False allows usage in FastAPI background/threaded contexts
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Pragmas for correctness & reasonable performance
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def create_schema() -> None:
    """Create tables and indexes if absent."""
    conn = get_connection()
    try:

        conn.executescript(
            """
            ------------------------------------------------------------
            -- Dimension tables
            ------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS periods (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                label      TEXT UNIQUE,       -- e.g., '2024-25', '2023-24'
                year_start INTEGER,           -- optional parsed start year (nullable)
                year_end   INTEGER            -- optional parsed end year (nullable)
            );

            CREATE TABLE IF NOT EXISTS ports (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                name  TEXT NOT NULL,          -- e.g., 'Mundra'
                state TEXT                    -- optional, e.g., 'Gujarat'
            );

            -- Helpful uniqueness & lookup indexes
            CREATE UNIQUE INDEX IF NOT EXISTS idx_periods_label ON periods(label);
            CREATE INDEX IF NOT EXISTS idx_ports_name ON ports(name);

            ------------------------------------------------------------
            -- Financials
            ------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS balance_sheet (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id        INTEGER NOT NULL,
                line_item        TEXT,        -- e.g., 'Total Assets'
                category         TEXT,        -- 'Assets', 'Liabilities', etc.
                sub_category     TEXT,
                sub_sub_category TEXT,
                value            REAL,
                FOREIGN KEY (period_id) REFERENCES periods(id)
            );

            CREATE INDEX IF NOT EXISTS idx_balance_sheet_period
                ON balance_sheet(period_id);
            CREATE INDEX IF NOT EXISTS idx_balance_sheet_item
                ON balance_sheet(line_item);

            CREATE TABLE IF NOT EXISTS cash_flow (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id  INTEGER NOT NULL,
                item       TEXT,              -- e.g., 'Operating Cash Flow'
                category   TEXT,              -- 'Operating', 'Investing', 'Financing'
                value      REAL,
                FOREIGN KEY (period_id) REFERENCES periods(id)
            );

            CREATE INDEX IF NOT EXISTS idx_cash_flow_period
                ON cash_flow(period_id);
            CREATE INDEX IF NOT EXISTS idx_cash_flow_item
                ON cash_flow(item);

            CREATE TABLE IF NOT EXISTS quarterly_pnl (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id   INTEGER NOT NULL,
                item        TEXT,             -- e.g., 'Revenue', 'Net Income'
                category    TEXT,             -- optional classification
                value       REAL,
                period_type TEXT,             -- e.g., 'Quarterly', 'Annual'
                FOREIGN KEY (period_id) REFERENCES periods(id)
            );

            CREATE INDEX IF NOT EXISTS idx_quarterly_pnl_period
                ON quarterly_pnl(period_id);
            CREATE INDEX IF NOT EXISTS idx_quarterly_pnl_item
                ON quarterly_pnl(item);

            CREATE TABLE IF NOT EXISTS consolidated_pnl (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id  INTEGER NOT NULL,
                line_item  TEXT,              -- e.g., 'Revenue', 'EBIDTA', etc.
                value      REAL,
                FOREIGN KEY (period_id) REFERENCES periods(id)
            );

            CREATE INDEX IF NOT EXISTS idx_consolidated_pnl_period
                ON consolidated_pnl(period_id);
            CREATE INDEX IF NOT EXISTS idx_consolidated_pnl_item
                ON consolidated_pnl(line_item);

            -- Unified ROCE table (internal/external) with optional port dimension
            CREATE TABLE IF NOT EXISTS roce (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id  INTEGER NOT NULL,
                source     TEXT,              -- 'internal' or 'external'
                category   TEXT,              -- optional (e.g., domestic)
                port_id    INTEGER,           -- optional (present in internal)
                line_item  TEXT,              -- e.g., 'EBIDTA'
                value      REAL,
                FOREIGN KEY (period_id) REFERENCES periods(id),
                FOREIGN KEY (port_id) REFERENCES ports(id)
            );

            CREATE INDEX IF NOT EXISTS idx_roce_period
                ON roce(period_id);
            CREATE INDEX IF NOT EXISTS idx_roce_source
                ON roce(source);

            ------------------------------------------------------------
            -- Operations
            ------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS volumes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id  INTEGER NOT NULL,
                port_id    INTEGER NOT NULL,
                commodity  TEXT,              -- e.g., 'Crude', 'Coal'
                entity     TEXT,              -- e.g., 'IOCL', 'HMEL'
                type       TEXT,              -- 'Tied' / 'Non-Tied', etc.
                value      REAL,              -- quantity/tonnage
                FOREIGN KEY (period_id) REFERENCES periods(id),
                FOREIGN KEY (port_id) REFERENCES ports(id)
            );

            CREATE INDEX IF NOT EXISTS idx_volumes_period_port
                ON volumes(period_id, port_id);

            CREATE TABLE IF NOT EXISTS containers (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id  INTEGER NOT NULL,
                port_id    INTEGER NOT NULL,
                entity     TEXT,              -- e.g., 'CMA (MMT)'
                type       TEXT,              -- 'Tied' / 'Non-Tied', etc.
                value      REAL,              -- volume/TEUs/etc.
                FOREIGN KEY (period_id) REFERENCES periods(id),
                FOREIGN KEY (port_id) REFERENCES ports(id)
            );

            CREATE INDEX IF NOT EXISTS idx_containers_period_port
                ON containers(period_id, port_id);

            CREATE TABLE IF NOT EXISTS roro (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id       INTEGER NOT NULL,
                port_id         INTEGER NOT NULL,
                type            TEXT,          -- 'Tied' / 'Non-Tied', etc.
                value           REAL,          -- may represent share/ratio if present
                number_of_cars  INTEGER,       -- absolute count if provided
                FOREIGN KEY (period_id) REFERENCES periods(id),
                FOREIGN KEY (port_id) REFERENCES ports(id)
            );

            CREATE INDEX IF NOT EXISTS idx_roro_period_port
                ON roro(period_id, port_id);
            """
        )

        conn.commit()
        logger.info("Database schema ensured.")
    finally:
        conn.close()

def get_or_create_period(conn: sqlite3.Connection, label: str) -> int:
    """Fetch or insert a period by label and return its id."""
    cur = conn.execute("SELECT id FROM periods WHERE label = ?", (label,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur = conn.execute("INSERT INTO periods (label) VALUES (?)", (label,))
    return int(cur.lastrowid)


def get_or_create_port(
    conn: sqlite3.Connection, name: str, state: Optional[str] = None
) -> int:
    """Fetch or insert a port by name and return its id."""
    cur = conn.execute("SELECT id FROM ports WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur = conn.execute("INSERT INTO ports (name, state) VALUES (?, ?)", (name, state))
    return int(cur.lastrowid)
