"""CSV ingestion with normalization into the relational schema."""

from __future__ import annotations

import logging
import math
from typing import Optional

import pandas as pd

from app.database import (
    create_schema,
    get_connection,
    get_or_create_period,
    get_or_create_port,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# File names expected under ./data
BALANCE_SHEET_CSV = "data/BalanceSheet.csv"
CASH_FLOW_CSV = "data/CashFlowStatement.csv"
QUARTERLY_PNL_CSV = "data/Quarterly PnL.csv"
CONSOLIDATED_PNL_CSV = "data/Consolidated PnL.csv"
ROCE_INTERNAL_CSV = "data/ROCE Internal.csv"
ROCE_EXTERNAL_CSV = "data/ROCE External.csv"
VOLUMES_CSV = "data/Volumes.csv"
CONTAINERS_CSV = "data/Containers.csv"
RORO_CSV = "data/RORO.csv"


def to_float(value: object) -> Optional[float]:
    """Parse human-formatted numbers like '31,079.00', '(1,234)', '₹ 1,200', '12.5%'."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if s in {"", "-", "—", "N/A", "NA", "nil", "None"}:
        return None

    negative = s.startswith("(") and s.endswith(")")
    if negative:
        s = s[1:-1]

    # Remove currency symbols, commas and spaces
    for ch in ("₹", "$", ",", " "):
        s = s.replace(ch, "")

    is_percent = s.endswith("%")
    if is_percent:
        s = s[:-1]

    try:
        num = float(s)
    except ValueError:
        return None

    if negative:
        num = -num
    # If percentages should be treated as ratio, uncomment below:
    # if is_percent:
    #     num = num / 100.0
    return num


def to_int(value: object) -> Optional[int]:
    """Round a parsed float to int; return None if not numeric."""
    f = to_float(value)
    if f is None or math.isnan(f):
        return None
    return int(round(f))


def insert_balance_sheet() -> None:
    df = pd.read_csv(BALANCE_SHEET_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            conn.execute(
                """
                INSERT INTO balance_sheet (
                    period_id, line_item, category, sub_category, sub_sub_category, value
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    row["Line Item"],
                    row.get("Category"),
                    row.get("SubCategory"),
                    row.get("SubSubCategory"),
                    to_float(row["Value"]),
                ),
            )
    logger.info("Loaded balance_sheet.")


def insert_cash_flow() -> None:
    df = pd.read_csv(CASH_FLOW_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            conn.execute(
                """
                INSERT INTO cash_flow (period_id, item, category, value)
                VALUES (?, ?, ?, ?)
                """,
                (pid, row["Item"], row.get("Category"), to_float(row["Value"])),
            )
    logger.info("Loaded cash_flow.")


def insert_quarterly_pnl() -> None:
    df = pd.read_csv(QUARTERLY_PNL_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            conn.execute(
                """
                INSERT INTO quarterly_pnl (period_id, item, category, value, period_type)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    row["Item"],
                    row.get("Category"),
                    to_float(row["Value"]),
                    row.get("Period Type"),
                ),
            )
    logger.info("Loaded quarterly_pnl.")


def insert_consolidated_pnl() -> None:
    df = pd.read_csv(CONSOLIDATED_PNL_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            conn.execute(
                """
                INSERT INTO consolidated_pnl (period_id, line_item, value)
                VALUES (?, ?, ?)
                """,
                (pid, row["Line Item"], to_float(row["Value"])),
            )
    logger.info("Loaded consolidated_pnl.")


def insert_roce(source: str) -> None:
    csv_path = ROCE_INTERNAL_CSV if source == "internal" else ROCE_EXTERNAL_CSV
    df = pd.read_csv(csv_path)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            port_id = None
            if "Port" in df.columns and not pd.isna(row.get("Port")):
                port_id = get_or_create_port(conn, str(row["Port"]))
            conn.execute(
                """
                INSERT INTO roce (period_id, source, category, port_id, line_item, value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    source,
                    row.get("Category"),
                    port_id,
                    row.get("Line Item", row.get("Particular")),
                    to_float(row["Value"]),
                ),
            )
    logger.info("Loaded roce (%s).", source)


def insert_volumes() -> None:
    df = pd.read_csv(VOLUMES_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            port_id = get_or_create_port(conn, str(row["Port"]), row.get("State"))
            conn.execute(
                """
                INSERT INTO volumes (period_id, port_id, commodity, entity, type, value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    port_id,
                    row.get("Commodity"),
                    row.get("Entity"),
                    row.get("Type"),
                    to_float(row["Value"]),
                ),
            )
    logger.info("Loaded volumes.")


def insert_containers() -> None:
    df = pd.read_csv(CONTAINERS_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            port_id = get_or_create_port(conn, str(row["Port"]))
            conn.execute(
                """
                INSERT INTO containers (period_id, port_id, entity, type, value)
                VALUES (?, ?, ?, ?, ?)
                """,
                (pid, port_id, row.get("Entity"), row.get("Type"), to_float(row["Value"])),
            )
    logger.info("Loaded containers.")


def insert_roro() -> None:
    df = pd.read_csv(RORO_CSV)
    with get_connection() as conn:
        for _, row in df.iterrows():
            pid = get_or_create_period(conn, str(row["Period"]))
            port_id = get_or_create_port(conn, str(row["Port"]))
            conn.execute(
                """
                INSERT INTO roro (period_id, port_id, type, value, number_of_cars)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    port_id,
                    row.get("Type"),
                    to_float(row["Value"]),
                    to_int(row.get("Number of Cars")),
                ),
            )
    logger.info("Loaded roro.")


def load_all_data() -> None:
    """Create schema (if needed) and load all CSVs."""
    create_schema()
    insert_balance_sheet()
    insert_cash_flow()
    insert_quarterly_pnl()
    insert_consolidated_pnl()
    insert_roce("internal")
    insert_roce("external")
    insert_volumes()
    insert_containers()
    insert_roro()
    logger.info("All data loaded successfully.")


if __name__ == "__main__":
    load_all_data()
