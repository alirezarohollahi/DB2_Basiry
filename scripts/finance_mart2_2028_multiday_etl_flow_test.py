"""
Finance MART 2 multi-day / multi-phase ETL flow test.

Scenario requested:
  1) First-load data up to "day 1".
  2) Add day-2 data, run normal ETL for day 2, verify DW.
  3) Repeat for day 3 and day 4.
  4) Add two more days of data, run ETL once for that two-day range, verify DW.
  5) In every incremental phase, update at least 10 non-transaction source records.
  6) Never update previous transaction rows.
  7) Verify DW correctness: valid staging rows, dimensions, transaction facts,
     no duplicates, monthly snapshots, and lifecycle rows.

Important:
  - This script is intended for a TEST/EMPTY Finance MART environment.
  - It uses fixed high IDs starting at TEST_ID_BASE to avoid collision.
  - It assumes the current snapshot table is dw.fact_monthly_financial_snapshot
    with month_key. Therefore each phase uses a different month-end date so
    monthly snapshot append-only behavior can be validated cleanly.

Required package:
  pip install pyodbc
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable

try:
    import pyodbc
except ImportError:
    print("Missing package: pyodbc. Install with: pip install pyodbc")
    raise

# =============================================================================
# Configuration
# =============================================================================

DRIVER = "ODBC Driver 18 for SQL Server"
SERVER = r"localhost"                  # Example: r"GOD-PC" or r"GOD-PC\SQLEXPRESS"
TRUSTED_CONNECTION = True
SQL_USER = ""
SQL_PASSWORD = ""
TRUST_SERVER_CERTIFICATE = True

SOURCE_DB = "Source_FinanceOps_DB"
STG_DB = "Stg_FinanceOps_DB"
DW_DB = "Charity_DW_DB"

TEST_ID_BASE = 2_029_000
CLEAN_PREVIOUS_TEST_ROWS = True
ENSURE_MINIMAL_CONFORMED_DIMS = True
STOP_ON_FIRST_FAILURE = False

# The current DW has a monthly snapshot fact. To test append-only snapshots
# without updating previous periods, each logical "day" is a separate month-end date.
PHASES = [
    # phase, data_date, window_start, window_end
    (1, date(2028, 1, 31), "2028-01-01T00:00:00", "2028-02-01T00:00:00"),
    (2, date(2028, 2, 29), "2028-02-01T00:00:00", "2028-03-01T00:00:00"),
    (3, date(2028, 3, 31), "2028-03-01T00:00:00", "2028-04-01T00:00:00"),
    (4, date(2028, 4, 30), "2028-04-01T00:00:00", "2028-05-01T00:00:00"),
    (5, date(2028, 5, 31), "2028-05-01T00:00:00", "2028-06-01T00:00:00"),
    (6, date(2028, 6, 30), "2028-06-01T00:00:00", "2028-07-01T00:00:00"),
]
DIM_DATE_START = "2028-01-01"
DIM_DATE_END = "2028-07-31"

# =============================================================================
# Helpers
# =============================================================================


def connect() -> pyodbc.Connection:
    if TRUSTED_CONNECTION:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            "Trusted_Connection=yes;"
            f"TrustServerCertificate={'yes' if TRUST_SERVER_CERTIFICATE else 'no'};"
        )
    else:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            f"UID={SQL_USER};PWD={SQL_PASSWORD};"
            f"TrustServerCertificate={'yes' if TRUST_SERVER_CERTIFICATE else 'no'};"
        )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def exec_sql(conn: pyodbc.Connection, sql: str, params: Iterable[Any] | None = None) -> None:
    cur = conn.cursor()
    cur.execute(sql, tuple(params or ()))
    cur.close()


def fetch_all(conn: pyodbc.Connection, sql: str, params: Iterable[Any] | None = None) -> list[pyodbc.Row]:
    cur = conn.cursor()
    cur.execute(sql, tuple(params or ()))
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_one(conn: pyodbc.Connection, sql: str, params: Iterable[Any] | None = None) -> pyodbc.Row | None:
    cur = conn.cursor()
    cur.execute(sql, tuple(params or ()))
    row = cur.fetchone()
    cur.close()
    return row


def scalar(conn: pyodbc.Connection, sql: str, params: Iterable[Any] | None = None) -> Any:
    row = fetch_one(conn, sql, params)
    return None if row is None else row[0]


def object_exists(conn: pyodbc.Connection, three_part_name: str, obj_type: str | None = None) -> bool:
    if obj_type:
        return bool(scalar(conn, "SELECT CASE WHEN OBJECT_ID(?, ?) IS NULL THEN 0 ELSE 1 END", [three_part_name, obj_type]))
    return bool(scalar(conn, "SELECT CASE WHEN OBJECT_ID(?) IS NULL THEN 0 ELSE 1 END", [three_part_name]))


def table_exists(conn: pyodbc.Connection, three_part_name: str) -> bool:
    return object_exists(conn, three_part_name, "U")


def col_exists(conn: pyodbc.Connection, table_name: str, column_name: str) -> bool:
    return scalar(conn, "SELECT CASE WHEN COL_LENGTH(?, ?) IS NULL THEN 0 ELSE 1 END", [table_name, column_name]) == 1


def begin_identity_insert(conn: pyodbc.Connection, table: str) -> None:
    exec_sql(conn, f"SET IDENTITY_INSERT {table} ON;")


def end_identity_insert(conn: pyodbc.Connection, table: str) -> None:
    exec_sql(conn, f"SET IDENTITY_INSERT {table} OFF;")


def normalize_param(column_name: str, value: Any) -> Any:
    if value is None:
        return None
    c = column_name.lower()
    if c in {"is_active", "is_valid", "is_confirmed", "is_refunded", "is_approved", "is_paid"}:
        return int(value)
    if c in {"start_date", "end_date", "donation_date", "expense_date", "payment_date", "allocation_date", "transaction_date", "rate_date"} and isinstance(value, str):
        return date.fromisoformat(value[:10])
    if c.endswith("_at") and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    return value


def normalize_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return [tuple(normalize_param(col, val) for col, val in zip(columns, row)) for row in rows]


def insert_with_identity(conn: pyodbc.Connection, table: str, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)});"
    safe_rows = normalize_rows(columns, rows)
    begin_identity_insert(conn, table)
    try:
        cur = conn.cursor()
        cur.fast_executemany = False
        cur.executemany(sql, safe_rows)
        cur.close()
    finally:
        end_identity_insert(conn, table)


def dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"))


RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" :: {detail}" if detail else ""))
    if STOP_ON_FIRST_FAILURE and not ok:
        raise AssertionError(f"{name}: {detail}")


# =============================================================================
# Cleanup and prerequisites
# =============================================================================


def clean_previous_test_rows(conn: pyodbc.Connection) -> None:
    print("Cleaning previous multi-day test rows only...")
    source_deletes = [
        f"DELETE FROM {SOURCE_DB}.finance_ops.financial_transactions WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.budget_allocations WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.payments WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.expenses WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.donations WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.currency_rates WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.expense_categories WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.campaigns WHERE id >= ? AND id < ?",
        f"DELETE FROM {SOURCE_DB}.finance_ops.donors WHERE id >= ? AND id < ?",
    ]
    upper = TEST_ID_BASE + 100_000
    for sql in source_deletes:
        exec_sql(conn, sql, [TEST_ID_BASE, upper])

    for tbl in ["financial_transactions", "budget_allocations", "payments", "expenses", "donations", "currency_rates", "expense_categories", "campaigns", "donors"]:
        full = f"{STG_DB}.stg_finance_ops.{tbl}"
        if table_exists(conn, full):
            exec_sql(conn, f"DELETE FROM {full} WHERE id >= ? AND id < ?", [TEST_ID_BASE, upper])

    dw_deletes = [
        f"DELETE FROM {DW_DB}.dw.fact_budget_allocation_event WHERE source_allocation_id >= ? AND source_allocation_id < ?",
        f"DELETE FROM {DW_DB}.dw.fact_donation_lifecycle WHERE source_donation_id >= ? AND source_donation_id < ?",
        f"DELETE FROM {DW_DB}.dw.fact_donation_transaction WHERE source_donation_id >= ? AND source_donation_id < ?",
        f"DELETE FROM {DW_DB}.dw.fact_expense_transaction WHERE source_expense_id >= ? AND source_expense_id < ?",
        f"DELETE FROM {DW_DB}.dw.fact_payment_transaction WHERE source_payment_id >= ? AND source_payment_id < ?",
        f"DELETE FROM {DW_DB}.dw.dim_donor WHERE donor_id >= ? AND donor_id < ?",
        f"DELETE FROM {DW_DB}.dw.dim_campaign WHERE campaign_id >= ? AND campaign_id < ?",
        f"DELETE FROM {DW_DB}.dw.dim_category WHERE category_id >= ? AND category_id < ?",
    ]
    for sql in dw_deletes:
        if object_exists(conn, sql.split(" FROM ")[1].split(" WHERE ")[0], "U"):
            exec_sql(conn, sql, [TEST_ID_BASE, upper])

    if table_exists(conn, f"{DW_DB}.dw.fact_monthly_financial_snapshot"):
        exec_sql(
            conn,
            f"""
            DELETE FROM {DW_DB}.dw.fact_monthly_financial_snapshot
            WHERE month_key IN (20280131, 20280229, 20280331, 20280430, 20280531, 20280630)
            """,
        )
    conn.commit()


def ensure_dim_date(conn: pyodbc.Connection) -> None:
    print("Ensuring dim_date covers 2028 test range...")
    proc = f"{DW_DB}.etl_admin.usp_fill_dw_dim_date"
    if not object_exists(conn, proc, "P"):
        raise RuntimeError(f"Missing required procedure: {proc}. Run dim_date fill procedure script first.")
    exec_sql(conn, f"EXEC {proc} @start_date = ?, @end_date = ?;", [DIM_DATE_START, DIM_DATE_END])
    conn.commit()


def table_columns(conn: pyodbc.Connection, full_table: str) -> set[str]:
    rows = fetch_all(conn, f"SELECT name FROM {DW_DB}.sys.columns WHERE object_id = OBJECT_ID(?)", [full_table])
    return {str(r[0]) for r in rows}


def ensure_conformed_dim_table(conn: pyodbc.Connection, table: str, key_col: str, id_col: str, name_col: str) -> None:
    full = f"{DW_DB}.dw.{table}"
    if table_exists(conn, full):
        return
    if not ENSURE_MINIMAL_CONFORMED_DIMS:
        raise RuntimeError(f"Missing required conformed dimension: {full}")
    print(f"Creating minimal test-only {full}...")
    exec_sql(conn, f"USE {DW_DB}; IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'dw') EXEC(N'CREATE SCHEMA dw');")
    exec_sql(
        conn,
        f"""
        USE {DW_DB};
        CREATE TABLE dw.{table} (
            {key_col} INT IDENTITY(1,1) NOT NULL,
            {id_col} INT NULL,
            {name_col} NVARCHAR(200) NULL,
            source_system NVARCHAR(100) NULL,
            created_at DATETIME2(0) NULL,
            updated_at DATETIME2(0) NULL
        );
        """,
    )
    conn.commit()


def insert_conformed_dim_row(conn: pyodbc.Connection, table: str, id_col: str, id_value: int, values: dict[str, Any]) -> None:
    full = f"{DW_DB}.dw.{table}"
    if scalar(conn, f"SELECT COUNT(1) FROM {full} WHERE {id_col} = ?", [id_value]):
        return
    cols = table_columns(conn, full)
    insert_cols: list[str] = []
    insert_vals: list[Any] = []
    for col, val in {id_col: id_value, **values}.items():
        if col in cols:
            insert_cols.append(col)
            insert_vals.append(normalize_param(col, val))
    exec_sql(conn, f"INSERT INTO {full} ({', '.join(insert_cols)}) VALUES ({', '.join('?' for _ in insert_cols)});", insert_vals)


def ensure_conformed_dims(conn: pyodbc.Connection) -> None:
    print("Ensuring dim_center and dim_child prerequisites...")
    ensure_conformed_dim_table(conn, "dim_center", "center_key", "center_id", "center_name")
    ensure_conformed_dim_table(conn, "dim_child", "child_key", "child_id", "child_name")
    for center_id, center_name in [(1, "Smoke Center 1"), (2, "Smoke Center 2")]:
        insert_conformed_dim_row(conn, "dim_center", "center_id", center_id, {
            "center_name": center_name,
            "name": center_name,
            "source_system": "PROGRAM_OPS",
            "created_at": "2028-01-01T00:00:00",
            "updated_at": None,
        })
    for child_id, child_name in [(1, "Smoke Child 1"), (2, "Smoke Child 2")]:
        insert_conformed_dim_row(conn, "dim_child", "child_id", child_id, {
            "child_name": child_name,
            "full_name": child_name,
            "name": child_name,
            "source_system": "PROGRAM_OPS",
            "created_at": "2028-01-01T00:00:00",
            "updated_at": None,
        })
    conn.commit()


# =============================================================================
# Source deterministic data
# =============================================================================


def dstr(d: date) -> str:
    return d.isoformat()


def dtstr(d: date, hour: int = 9) -> str:
    return f"{d.isoformat()}T{hour:02d}:00:00"


def insert_base_non_transaction_data(conn: pyodbc.Connection) -> None:
    print("Inserting base dimension/source master data...")
    donor_cols = ["id", "full_name", "national_id", "phone", "email", "donor_type", "is_active", "created_at", "updated_at"]
    donors = []
    for i in range(1, 13):
        donors.append((TEST_ID_BASE + i, f"MD Smoke Donor {i:02d}", f"MD-NID-{TEST_ID_BASE+i}", f"09128{i:06d}", f"md-smoke-{i}@example.com", "individual" if i % 3 else "organization", 1, "2028-01-01T08:00:00", None))
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.donors", donor_cols, donors)

    campaign_cols = ["id", "title", "description", "target_amount", "start_date", "end_date", "status", "created_at", "updated_at"]
    campaigns = [
        (TEST_ID_BASE + 101, "MD Smoke Campaign A", "Multi-day test campaign A", Decimal("100000.00"), "2028-01-01", "2028-12-31", "active", "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 102, "MD Smoke Campaign B", "Multi-day test campaign B", Decimal("200000.00"), "2028-01-01", "2028-12-31", "active", "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 103, "MD Smoke Campaign C", "Multi-day test campaign C", Decimal("300000.00"), "2028-01-01", "2028-12-31", "active", "2028-01-01T09:00:00", None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.campaigns", campaign_cols, campaigns)

    category_cols = ["id", "name", "parent_id", "is_active", "created_at", "updated_at"]
    categories = [
        (TEST_ID_BASE + 201, "MD Education", None, 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 202, "MD Books", TEST_ID_BASE + 201, 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 203, "MD Meals", TEST_ID_BASE + 201, 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 204, "MD Health", None, 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 205, "MD Transport", None, 1, "2028-01-01T09:00:00", None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.expense_categories", category_cols, categories)

    rate_cols = ["id", "from_currency", "to_currency", "rate", "rate_date"]
    rates = [(TEST_ID_BASE + 601, "USD", "IRR", Decimal("500000.00000000"), "2028-01-01")]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.currency_rates", rate_cols, rates)
    conn.commit()


def update_at_least_10_non_transaction_rows(conn: pyodbc.Connection, phase: int, d: date) -> None:
    """Affects 10 source master/dimension rows; transaction rows are never updated."""
    for i in range(1, 11):
        exec_sql(
            conn,
            f"""
            UPDATE {SOURCE_DB}.finance_ops.donors
               SET full_name = ?, updated_at = ?
             WHERE id = ?;
            """,
            [f"MD Smoke Donor {i:02d} - P{phase}", dtstr(d, 8), TEST_ID_BASE + i],
        )
    # Two extra non-transaction updates, so every phase affects at least 10 rows even if a donor update were skipped.
    exec_sql(conn, f"UPDATE {SOURCE_DB}.finance_ops.campaigns SET title=?, updated_at=? WHERE id=?", [f"MD Smoke Campaign A - P{phase}", dtstr(d, 8), TEST_ID_BASE + 101])
    exec_sql(conn, f"UPDATE {SOURCE_DB}.finance_ops.expense_categories SET name=?, updated_at=? WHERE id=?", [f"MD Books - P{phase}", dtstr(d, 8), TEST_ID_BASE + 202])


def insert_transaction_data_for_phase(conn: pyodbc.Connection, phase: int, d: date) -> dict[str, Decimal]:
    """Insert only new transaction/event rows for this phase. No previous transaction rows are touched."""
    c1_don = Decimal(700 + 10 * phase)
    c2_don = Decimal(300 + 10 * phase)
    c1_exp = Decimal(100 + 5 * phase)
    c2_exp = Decimal(50 + 5 * phase)
    c1_pay = Decimal(300 + 5 * phase)
    c2_pay = Decimal(75 + 5 * phase)
    donation_amount = c1_don + c2_don

    donation_id = TEST_ID_BASE + 1000 + phase
    expense1_id = TEST_ID_BASE + 3000 + phase * 10 + 1
    expense2_id = TEST_ID_BASE + 3000 + phase * 10 + 2
    payment1_id = TEST_ID_BASE + 4000 + phase * 10 + 1
    payment2_id = TEST_ID_BASE + 4000 + phase * 10 + 2
    alloc1_id = TEST_ID_BASE + 2000 + phase * 10 + 1
    alloc2_id = TEST_ID_BASE + 2000 + phase * 10 + 2

    donation_cols = ["id", "donor_id", "campaign_id", "amount", "currency", "donation_type", "donation_date", "status", "reference_code", "created_at", "updated_at"]
    donations = [(donation_id, TEST_ID_BASE + ((phase - 1) % 12) + 1, TEST_ID_BASE + 101, donation_amount, "IRR", "online", dstr(d), "confirmed", f"MD-DON-{phase:03d}", dtstr(d, 10), None)]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.donations", donation_cols, donations)

    expense_cols = ["id", "center_id", "child_id", "category_id", "amount", "currency", "expense_date", "description", "approved_by_user_id", "status", "created_at", "updated_at"]
    expenses = [
        (expense1_id, 1, 1, TEST_ID_BASE + 202, c1_exp, "IRR", dstr(d), f"MD phase {phase} center 1 expense", 10, "approved", dtstr(d, 11), None),
        (expense2_id, 2, 2, TEST_ID_BASE + 203, c2_exp, "IRR", dstr(d), f"MD phase {phase} center 2 expense", 10, "approved", dtstr(d, 11), None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.expenses", expense_cols, expenses)

    payment_cols = ["id", "payment_type", "teacher_id", "center_id", "amount", "currency", "payment_date", "status", "created_at", "updated_at"]
    payments = [
        (payment1_id, "salary", 1001, 1, c1_pay, "IRR", dstr(d), "paid", dtstr(d, 12), None),
        (payment2_id, "vendor", None, 2, c2_pay, "IRR", dstr(d), "paid", dtstr(d, 12), None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.payments", payment_cols, payments)

    allocation_cols = ["id", "source_type", "source_id", "center_id", "child_id", "category_id", "allocated_amount", "allocation_date", "reason", "created_at"]
    allocations = [
        (alloc1_id, "donation", donation_id, 1, 1, TEST_ID_BASE + 202, c1_don, dstr(d), f"MD phase {phase} allocation center 1", dtstr(d, 13)),
        (alloc2_id, "donation", donation_id, 2, 2, TEST_ID_BASE + 203, c2_don, dstr(d), f"MD phase {phase} allocation center 2", dtstr(d, 13)),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.budget_allocations", allocation_cols, allocations)

    tx_cols = ["id", "entity_type", "entity_id", "transaction_type", "amount", "transaction_date", "created_at"]
    tx_rows = [
        (TEST_ID_BASE + 5000 + phase * 10 + 1, "donation", donation_id, "credit", donation_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 10 + 2, "expense", expense1_id, "debit", c1_exp, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 10 + 3, "expense", expense2_id, "debit", c2_exp, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 10 + 4, "payment", payment1_id, "debit", c1_pay, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 10 + 5, "payment", payment2_id, "debit", c2_pay, dstr(d), dtstr(d, 14)),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.financial_transactions", tx_cols, tx_rows)
    conn.commit()

    return {
        "center1_donation": c1_don,
        "center2_donation": c2_don,
        "center1_expense": c1_exp,
        "center2_expense": c2_exp,
        "center1_payment": c1_pay,
        "center2_payment": c2_pay,
    }


# =============================================================================
# ETL runners
# =============================================================================


def run_source_to_staging(conn: pyodbc.Connection, to_date: str) -> None:
    print(f"Running Source -> Staging ETL to_date={to_date}...")
    exec_sql(conn, f"EXEC {STG_DB}.etl_admin.usp_run_stg_finance_ops_all @to_date = ?;", [to_date])
    conn.commit()


def run_dw_first_load(conn: pyodbc.Connection, start_time: str, end_time: str) -> None:
    print(f"Running DW FIRST LOAD {start_time} -> {end_time}...")
    exec_sql(
        conn,
        f"""
        EXEC {DW_DB}.etl_admin.usp_first_load_dw_finance_mart2_all
             @start_time = ?,
             @end_time = ?,
             @run_staging = 0;
        """,
        [start_time, end_time],
    )
    conn.commit()


def run_dw_incremental(conn: pyodbc.Connection, start_time: str, end_time: str) -> None:
    print(f"Running DW NORMAL/INCREMENTAL {start_time} -> {end_time}...")
    exec_sql(
        conn,
        f"""
        EXEC {DW_DB}.etl_admin.usp_load_dw_finance_mart2_daily
             @start_time = ?,
             @end_time = ?,
             @run_staging = 0;
        """,
        [start_time, end_time],
    )
    conn.commit()


# =============================================================================
# Verification
# =============================================================================


def get_center_key(conn: pyodbc.Connection, center_id: int) -> int:
    val = scalar(conn, f"SELECT TOP (1) center_key FROM {DW_DB}.dw.dim_center WHERE center_id = ?", [center_id])
    return int(val) if val is not None else -1


def month_key_for(d: date) -> int:
    # d is chosen as month-end, so YYYYMMDD is the month_key in dim_date.
    return int(d.strftime("%Y%m%d"))


def verify_staging_validity(conn: pyodbc.Connection, phase_label: str) -> None:
    for tbl in ["donors", "campaigns", "expense_categories", "donations", "expenses", "payments", "budget_allocations", "financial_transactions", "currency_rates"]:
        full = f"{STG_DB}.stg_finance_ops.{tbl}"
        if not table_exists(conn, full):
            check(f"{phase_label}: staging table exists {tbl}", False, full)
            continue
        invalid_count = scalar(conn, f"SELECT COUNT(1) FROM {full} WHERE id >= ? AND id < ? AND ISNULL(is_valid, 0) <> 1", [TEST_ID_BASE, TEST_ID_BASE + 100_000])
        check(f"{phase_label}: staging valid {tbl}", int(invalid_count or 0) == 0, f"invalid_count={invalid_count}")


def verify_dimensions(conn: pyodbc.Connection, phase: int) -> None:
    donor_count = scalar(conn, f"SELECT COUNT(1) FROM {DW_DB}.dw.dim_donor WHERE donor_id >= ? AND donor_id < ?", [TEST_ID_BASE, TEST_ID_BASE + 100])
    check(f"P{phase}: dim_donor has 12 test donors", int(donor_count or 0) == 12, f"count={donor_count}")

    updated_count = scalar(
        conn,
        f"SELECT COUNT(1) FROM {DW_DB}.dw.dim_donor WHERE donor_id BETWEEN ? AND ? AND full_name LIKE ?",
        [TEST_ID_BASE + 1, TEST_ID_BASE + 10, f"%- P{phase}"],
    )
    check(f"P{phase}: at least 10 non-transaction donor updates reached DW", int(updated_count or 0) == 10, f"updated_count={updated_count}")

    campaign_title = scalar(conn, f"SELECT title FROM {DW_DB}.dw.dim_campaign WHERE campaign_id = ?", [TEST_ID_BASE + 101])
    check(f"P{phase}: dim_campaign Type1 title updated", campaign_title == f"MD Smoke Campaign A - P{phase}", f"title={campaign_title}")

    category_name = scalar(conn, f"SELECT category_name FROM {DW_DB}.dw.dim_category WHERE category_id = ?", [TEST_ID_BASE + 202])
    check(f"P{phase}: dim_category Type1 name updated", category_name == f"MD Books - P{phase}", f"category_name={category_name}")


def verify_fact_counts(conn: pyodbc.Connection, cumulative_phases: int) -> None:
    checks = [
        ("donations", "fact_donation_transaction", "source_donation_id", cumulative_phases),
        ("expenses", "fact_expense_transaction", "source_expense_id", cumulative_phases * 2),
        ("payments", "fact_payment_transaction", "source_payment_id", cumulative_phases * 2),
        ("allocations", "fact_budget_allocation_event", "source_allocation_id", cumulative_phases * 2),
        ("lifecycle", "fact_donation_lifecycle", "source_donation_id", cumulative_phases),
    ]
    for label, table, col, expected in checks:
        cnt = scalar(conn, f"SELECT COUNT(1) FROM {DW_DB}.dw.{table} WHERE {col} >= ? AND {col} < ?", [TEST_ID_BASE, TEST_ID_BASE + 100_000])
        check(f"facts cumulative {label} count", int(cnt or 0) == expected, f"count={cnt}, expected={expected}")

    # No duplicate by source ID in append-only facts.
    dup_queries = [
        ("donation", "fact_donation_transaction", "source_donation_id"),
        ("expense", "fact_expense_transaction", "source_expense_id"),
        ("payment", "fact_payment_transaction", "source_payment_id"),
        ("allocation", "fact_budget_allocation_event", "source_allocation_id"),
    ]
    for label, table, col in dup_queries:
        dup = scalar(
            conn,
            f"""
            SELECT COUNT(1)
            FROM (
                SELECT {col}
                FROM {DW_DB}.dw.{table}
                WHERE {col} >= ? AND {col} < ?
                GROUP BY {col}
                HAVING COUNT(*) > 1
            ) d
            """,
            [TEST_ID_BASE, TEST_ID_BASE + 100_000],
        )
        check(f"append-only {label} facts no duplicate", int(dup or 0) == 0, f"duplicate_source_ids={dup}")

    for table, col in [
        ("fact_donation_transaction", "source_donation_id"),
        ("fact_expense_transaction", "source_expense_id"),
        ("fact_payment_transaction", "source_payment_id"),
        ("fact_budget_allocation_event", "source_allocation_id"),
    ]:
        bad = scalar(conn, f"SELECT COUNT(1) FROM {DW_DB}.dw.{table} WHERE {col} >= ? AND {col} < ? AND date_key = -1", [TEST_ID_BASE, TEST_ID_BASE + 100_000])
        check(f"{table}: no date_key=-1 for test rows", int(bad or 0) == 0, f"bad_count={bad}")


def verify_snapshot_for_phase(conn: pyodbc.Connection, phase: int, d: date, expected: dict[str, Decimal]) -> None:
    center1 = get_center_key(conn, 1)
    center2 = get_center_key(conn, 2)
    check(f"P{phase}: center 1 resolves", center1 != -1, f"center_key={center1}")
    check(f"P{phase}: center 2 resolves", center2 != -1, f"center_key={center2}")
    mk = month_key_for(d)

    expected_by_center = {
        center1: {
            "total_donation_amount": expected["center1_donation"],
            "total_expense_amount": expected["center1_expense"],
            "total_payment_amount": expected["center1_payment"],
            "net_balance": expected["center1_donation"] - expected["center1_expense"] - expected["center1_payment"],
            "donation_count": 1,
            "expense_count": 1,
            "payment_count": 1,
            "allocation_count": 1,
        },
        center2: {
            "total_donation_amount": expected["center2_donation"],
            "total_expense_amount": expected["center2_expense"],
            "total_payment_amount": expected["center2_payment"],
            "net_balance": expected["center2_donation"] - expected["center2_expense"] - expected["center2_payment"],
            "donation_count": 1,
            "expense_count": 1,
            "payment_count": 1,
            "allocation_count": 1,
        },
    }

    for center_key, exp in expected_by_center.items():
        row = fetch_one(
            conn,
            f"""
            SELECT total_donation_amount, total_expense_amount, total_payment_amount,
                   net_balance, donation_count, expense_count, payment_count, allocation_count
            FROM {DW_DB}.dw.fact_monthly_financial_snapshot
            WHERE month_key = ? AND center_key = ?
            """,
            [mk, center_key],
        )
        if row is None:
            check(f"P{phase}: snapshot exists month={mk} center={center_key}", False, "missing row")
            continue
        ok = (
            dec(row.total_donation_amount) == dec(exp["total_donation_amount"]) and
            dec(row.total_expense_amount) == dec(exp["total_expense_amount"]) and
            dec(row.total_payment_amount) == dec(exp["total_payment_amount"]) and
            dec(row.net_balance) == dec(exp["net_balance"]) and
            int(row.donation_count or 0) == exp["donation_count"] and
            int(row.expense_count or 0) == exp["expense_count"] and
            int(row.payment_count or 0) == exp["payment_count"] and
            int(row.allocation_count or 0) == exp["allocation_count"]
        )
        detail = f"actual=(don={row.total_donation_amount}, exp={row.total_expense_amount}, pay={row.total_payment_amount}, net={row.net_balance}, dc={row.donation_count}, ec={row.expense_count}, pc={row.payment_count}, ac={row.allocation_count}) expected={exp}"
        check(f"P{phase}: snapshot month={mk} center={center_key}", ok, detail)


def verify_previous_transaction_source_unchanged(conn: pyodbc.Connection, max_phase: int, expected_by_phase: dict[int, dict[str, Decimal]]) -> None:
    """The script must not change transaction rows from previous days. Verify source amounts still match expected."""
    bad = 0
    for phase in range(1, max_phase + 1):
        exp = expected_by_phase[phase]
        donation_id = TEST_ID_BASE + 1000 + phase
        donation_amt = scalar(conn, f"SELECT amount FROM {SOURCE_DB}.finance_ops.donations WHERE id = ?", [donation_id])
        if dec(donation_amt) != dec(exp["center1_donation"] + exp["center2_donation"]):
            bad += 1
        for center_num, key in [(1, "center1_expense"), (2, "center2_expense")]:
            expense_id = TEST_ID_BASE + 3000 + phase * 10 + center_num
            amt = scalar(conn, f"SELECT amount FROM {SOURCE_DB}.finance_ops.expenses WHERE id = ?", [expense_id])
            if dec(amt) != dec(exp[key]):
                bad += 1
        for center_num, key in [(1, "center1_payment"), (2, "center2_payment")]:
            payment_id = TEST_ID_BASE + 4000 + phase * 10 + center_num
            amt = scalar(conn, f"SELECT amount FROM {SOURCE_DB}.finance_ops.payments WHERE id = ?", [payment_id])
            if dec(amt) != dec(exp[key]):
                bad += 1
    check(f"transaction source rows unchanged through phase {max_phase}", bad == 0, f"bad_amount_rows={bad}")


def verify_phase(conn: pyodbc.Connection, phase: int, d: date, cumulative_phases: int, expected_by_phase: dict[int, dict[str, Decimal]]) -> None:
    label = f"P{phase}"
    verify_staging_validity(conn, label)
    verify_dimensions(conn, phase)
    verify_fact_counts(conn, cumulative_phases)
    for p in range(1, cumulative_phases + 1):
        pd = PHASES[p - 1][1]
        verify_snapshot_for_phase(conn, p, pd, expected_by_phase[p])
    verify_previous_transaction_source_unchanged(conn, cumulative_phases, expected_by_phase)


# =============================================================================
# Main flow
# =============================================================================


def print_summary() -> None:
    total = len(RESULTS)
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    print("\n" + "=" * 80)
    print(f"MULTI-DAY ETL FLOW TEST SUMMARY: {passed}/{total} passed")
    if passed != total:
        print("Failed checks:")
        for name, ok, detail in RESULTS:
            if not ok:
                print(f" - {name}: {detail}")
    else:
        print("All checks passed.")


def main() -> int:
    print("Finance MART 2 multi-day ETL flow test")
    print(f"Server: {SERVER}")
    print(f"Test ID base: {TEST_ID_BASE}")
    print("This script is intended for an empty/test database or an isolated 2028 test range.")

    expected_by_phase: dict[int, dict[str, Decimal]] = {}
    conn = connect()
    try:
        if CLEAN_PREVIOUS_TEST_ROWS:
            clean_previous_test_rows(conn)
        ensure_dim_date(conn)
        ensure_conformed_dims(conn)

        # Phase 1: first load up to day 1.
        phase, d, start_time, end_time = PHASES[0]
        print("\n" + "-" * 80)
        print(f"PHASE {phase}: FIRST LOAD through {d}")
        insert_base_non_transaction_data(conn)
        update_at_least_10_non_transaction_rows(conn, phase, d)
        expected_by_phase[phase] = insert_transaction_data_for_phase(conn, phase, d)
        run_source_to_staging(conn, end_time)
        run_dw_first_load(conn, start_time, end_time)
        verify_phase(conn, phase, d, 1, expected_by_phase)

        # Phases 2, 3, 4: one-day normal runs.
        for phase, d, start_time, end_time in PHASES[1:4]:
            print("\n" + "-" * 80)
            print(f"PHASE {phase}: NORMAL daily load for {d}")
            update_at_least_10_non_transaction_rows(conn, phase, d)
            expected_by_phase[phase] = insert_transaction_data_for_phase(conn, phase, d)
            run_source_to_staging(conn, end_time)
            run_dw_incremental(conn, start_time, end_time)
            verify_phase(conn, phase, d, phase, expected_by_phase)

        # Final phase: add two days, run one two-day ETL window.
        print("\n" + "-" * 80)
        print("FINAL PHASE: add two days of data and run one two-period ETL window")
        for phase, d, _, _ in PHASES[4:6]:
            update_at_least_10_non_transaction_rows(conn, phase, d)
            expected_by_phase[phase] = insert_transaction_data_for_phase(conn, phase, d)
        final_start = PHASES[4][2]
        final_end = PHASES[5][3]
        run_source_to_staging(conn, final_end)
        run_dw_incremental(conn, final_start, final_end)
        verify_phase(conn, 6, PHASES[5][1], 6, expected_by_phase)

        print_summary()
        return 0 if all(ok for _, ok, _ in RESULTS) else 1
    except Exception:
        print("\nERROR: Multi-day ETL flow test failed before completion.")
        import traceback
        traceback.print_exc()
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
