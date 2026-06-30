"""
Finance MART 2 multi-day / multi-phase ETL flow test.

Synced with the newest DW/STG version:
  - DW facts no longer contain source_* columns.
  - DW dimensions/facts no longer contain source_system.
  - fact_donation_lifecycle uses min_donation / max_donation / avg_donation.
  - fact_budget_allocation_event is fact-less and has no measure/source/text columns.
  - finance_ops.expense_categories no longer has parent_id.

Scenario:
  1) First-load data up to phase/month 1.
  2) Add phase/month 2 data, run normal ETL, verify DW.
  3) Repeat for phase/month 3 and phase/month 4.
  4) Add two more phases/months, run ETL once for that two-period range, verify DW.
  5) In every incremental phase, update at least 10 non-transaction source rows.
  6) Never update previous transaction rows.
  7) Verify DW correctness: staging rows, dimensions, transaction facts, no duplicates,
     monthly snapshots, lifecycle rows, status handling, architecture contract, and edge cases.

Important:
  - This script is intended for a TEST/EMPTY Finance MART environment.
  - It uses fixed high IDs starting at TEST_ID_BASE to avoid collision.
  - Because facts no longer keep source IDs, this script validates idempotency and
    source-row lineage through Charity_DW_DB.etl_work.fact_source_load_map.

Required package:
  pip install pyodbc
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
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

VALID_DONATION_STATUSES = ("confirmed", "rejected", "refunded")
VALID_EXPENSE_STATUSES = ("approved", "rejected")
VALID_PAYMENT_STATUSES = ("approved", "paid", "cancelled", "rejected")

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


def table_columns(conn: pyodbc.Connection, full_table: str, db_name: str = DW_DB) -> set[str]:
    rows = fetch_all(conn, f"SELECT name FROM {db_name}.sys.columns WHERE object_id = OBJECT_ID(?)", [full_table])
    return {str(r[0]) for r in rows}


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
    if c in {
        "start_date", "end_date", "donation_date", "expense_date", "payment_date",
        "allocation_date", "transaction_date", "rate_date"
    } and isinstance(value, str):
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
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def dstr(d: date) -> str:
    return d.isoformat()


def dtstr(d: date, hour: int = 9) -> str:
    return f"{d.isoformat()}T{hour:02d}:00:00"


def placeholders(values: Iterable[Any]) -> str:
    vals = list(values)
    if not vals:
        return "NULL"
    return ", ".join("?" for _ in vals)


RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" :: {detail}" if detail else ""))
    if STOP_ON_FIRST_FAILURE and not ok:
        raise AssertionError(f"{name}: {detail}")


# =============================================================================
# Source ID helpers
# =============================================================================


def donation_ids_for_phase(phase: int) -> dict[str, int]:
    base = TEST_ID_BASE + 1000 + phase * 10
    return {
        "confirmed": base + 1,
        "pending": base + 2,
        "rejected": base + 3,
        "refunded": base + 4,
    }


def expense_ids_for_phase(phase: int) -> dict[str, int]:
    base = TEST_ID_BASE + 3000 + phase * 10
    return {
        "approved_c1": base + 1,
        "approved_c2": base + 2,
        "pending": base + 3,
        "rejected": base + 4,
    }


def payment_ids_for_phase(phase: int) -> dict[str, int]:
    base = TEST_ID_BASE + 4000 + phase * 10
    return {
        "paid_c1": base + 1,
        "paid_c2": base + 2,
        "pending": base + 3,
        "approved": base + 4,
        "cancelled": base + 5,
        "rejected": base + 6,
    }


def allocation_ids_for_phase(phase: int) -> dict[str, int]:
    base = TEST_ID_BASE + 2000 + phase * 10
    return {
        "donation_c1": base + 1,
        "donation_c2": base + 2,
        "internal_budget_c1": base + 3,
    }


# =============================================================================
# Cleanup and prerequisites
# =============================================================================


def map_batch_ids_for_test_rows(conn: pyodbc.Connection, fact_name: str, source_table: str) -> list[int]:
    if not table_exists(conn, f"{DW_DB}.etl_work.fact_source_load_map"):
        return []
    rows = fetch_all(
        conn,
        f"""
        SELECT DISTINCT loaded_etl_batch_id
        FROM {DW_DB}.etl_work.fact_source_load_map
        WHERE fact_name = ?
          AND source_table = ?
          AND source_id >= ? AND source_id < ?
          AND loaded_etl_batch_id IS NOT NULL
        """,
        [fact_name, source_table, TEST_ID_BASE, TEST_ID_BASE + 100_000],
    )
    return [int(r[0]) for r in rows if r[0] is not None]


def delete_fact_rows_by_map(conn: pyodbc.Connection, fact_table: str, fact_name: str, source_table: str) -> None:
    batches = map_batch_ids_for_test_rows(conn, fact_name, source_table)
    if not batches or not table_exists(conn, f"{DW_DB}.dw.{fact_table}"):
        return
    ph = placeholders(batches)
    exec_sql(conn, f"DELETE FROM {DW_DB}.dw.{fact_table} WHERE etl_batch_id IN ({ph});", batches)


def clean_previous_test_rows(conn: pyodbc.Connection) -> None:
    print("Cleaning previous multi-day test rows only...")
    upper = TEST_ID_BASE + 100_000

    # Delete DW facts first. Facts no longer keep source IDs, so use the ETL source map.
    delete_fact_rows_by_map(conn, "fact_donation_transaction", "fact_donation_transaction", "donations")
    delete_fact_rows_by_map(conn, "fact_expense_transaction", "fact_expense_transaction", "expenses")
    delete_fact_rows_by_map(conn, "fact_payment_transaction", "fact_payment_transaction", "payments")
    delete_fact_rows_by_map(conn, "fact_budget_allocation_event", "fact_budget_allocation_event", "budget_allocations")

    if table_exists(conn, f"{DW_DB}.dw.fact_donation_lifecycle"):
        exec_sql(
            conn,
            f"""
            DELETE f
            FROM {DW_DB}.dw.fact_donation_lifecycle f
            INNER JOIN {DW_DB}.dw.dim_donor d ON d.donor_key = f.donor_key
            WHERE d.donor_id >= ? AND d.donor_id < ?
            """,
            [TEST_ID_BASE, upper],
        )

    if table_exists(conn, f"{DW_DB}.dw.fact_monthly_financial_snapshot"):
        exec_sql(
            conn,
            f"""
            DELETE FROM {DW_DB}.dw.fact_monthly_financial_snapshot
            WHERE month_key IN (20280131, 20280229, 20280331, 20280430, 20280531, 20280630)
            """,
        )

    if table_exists(conn, f"{DW_DB}.etl_work.fact_source_load_map"):
        exec_sql(
            conn,
            f"""
            DELETE FROM {DW_DB}.etl_work.fact_source_load_map
            WHERE source_id >= ? AND source_id < ?
              AND source_table IN (N'donations', N'expenses', N'payments', N'budget_allocations')
            """,
            [TEST_ID_BASE, upper],
        )

    dw_deletes = [
        f"DELETE FROM {DW_DB}.dw.dim_donor WHERE donor_id >= ? AND donor_id < ?",
        f"DELETE FROM {DW_DB}.dw.dim_campaign WHERE campaign_id >= ? AND campaign_id < ?",
        f"DELETE FROM {DW_DB}.dw.dim_category WHERE category_id >= ? AND category_id < ?",
    ]
    for sql in dw_deletes:
        full_table = sql.split(" FROM ")[1].split(" WHERE ")[0]
        if object_exists(conn, full_table, "U"):
            exec_sql(conn, sql, [TEST_ID_BASE, upper])

    for tbl in [
        "financial_transactions", "budget_allocations", "payments", "expenses", "donations",
        "currency_rates", "expense_categories", "campaigns", "donors",
    ]:
        full = f"{STG_DB}.stg_finance_ops.{tbl}"
        if table_exists(conn, full):
            exec_sql(conn, f"DELETE FROM {full} WHERE id >= ? AND id < ?", [TEST_ID_BASE, upper])

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
    for sql in source_deletes:
        exec_sql(conn, sql, [TEST_ID_BASE, upper])

    conn.commit()


def ensure_dim_date(conn: pyodbc.Connection) -> None:
    print("Ensuring dim_date covers 2028 test range...")
    proc = f"{DW_DB}.etl_admin.usp_fill_dw_dim_date"
    if not object_exists(conn, proc, "P"):
        raise RuntimeError(f"Missing required procedure: {proc}. Run dim_date fill procedure script first.")
    exec_sql(conn, f"EXEC {proc} @start_date = ?, @end_date = ?;", [DIM_DATE_START, DIM_DATE_END])
    conn.commit()


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
            "created_at": "2028-01-01T00:00:00",
            "updated_at": None,
        })
    for child_id, child_name in [(1, "Smoke Child 1"), (2, "Smoke Child 2")]:
        insert_conformed_dim_row(conn, "dim_child", "child_id", child_id, {
            "child_name": child_name,
            "full_name": child_name,
            "name": child_name,
            "created_at": "2028-01-01T00:00:00",
            "updated_at": None,
        })
    conn.commit()


# =============================================================================
# Source deterministic data
# =============================================================================


def insert_base_non_transaction_data(conn: pyodbc.Connection) -> None:
    print("Inserting base dimension/source master data...")
    donor_cols = ["id", "full_name", "national_id", "phone", "email", "donor_type", "is_active", "created_at", "updated_at"]
    donors = []
    for i in range(1, 13):
        donors.append((
            TEST_ID_BASE + i,
            f"MD Smoke Donor {i:02d}",
            f"MD-NID-{TEST_ID_BASE + i}",
            f"09128{i:06d}",
            f"md-smoke-{i}@example.com",
            "individual" if i % 3 else "organization",
            1,
            "2028-01-01T08:00:00",
            None,
        ))
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.donors", donor_cols, donors)

    campaign_cols = ["id", "title", "description", "target_amount", "start_date", "end_date", "status", "created_at", "updated_at"]
    campaigns = [
        (TEST_ID_BASE + 101, "MD Smoke Campaign A", "Multi-day test campaign A", Decimal("100000.00"), "2028-01-01", "2028-12-31", "active", "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 102, "MD Smoke Campaign B", "Multi-day test campaign B", Decimal("200000.00"), "2028-01-01", "2028-12-31", "active", "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 103, "MD Smoke Campaign C", "Multi-day test campaign C", Decimal("300000.00"), "2028-01-01", "2028-12-31", "active", "2028-01-01T09:00:00", None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.campaigns", campaign_cols, campaigns)

    # New source architecture: expense_categories has no parent_id.
    category_cols = ["id", "name", "is_active", "created_at", "updated_at"]
    categories = [
        (TEST_ID_BASE + 201, "MD Education", 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 202, "MD Books", 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 203, "MD Meals", 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 204, "MD Health", 1, "2028-01-01T09:00:00", None),
        (TEST_ID_BASE + 205, "MD Transport", 1, "2028-01-01T09:00:00", None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.expense_categories", category_cols, categories)

    rate_cols = ["id", "from_currency", "to_currency", "rate", "rate_date"]
    rates = [(TEST_ID_BASE + 601, "USD", "IRR", Decimal("500000.00000000"), "2028-01-01")]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.currency_rates", rate_cols, rates)
    conn.commit()


def update_at_least_10_non_transaction_rows(conn: pyodbc.Connection, phase: int, d: date) -> None:
    """Affects 10+ source master/dimension rows; transaction rows are never updated."""
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


def insert_transaction_data_for_phase(conn: pyodbc.Connection, phase: int, d: date) -> dict[str, Any]:
    """Insert only new transaction/event rows for this phase. No previous transaction rows are touched."""
    c1_don = Decimal(700 + 10 * phase)
    c2_don = Decimal(300 + 10 * phase)
    c1_exp = Decimal(100 + 5 * phase)
    c2_exp = Decimal(50 + 5 * phase)
    c1_pay = Decimal(300 + 5 * phase)
    c2_pay = Decimal(75 + 5 * phase)
    confirmed_donation_amount = c1_don + c2_don

    donation_ids = donation_ids_for_phase(phase)
    expense_ids = expense_ids_for_phase(phase)
    payment_ids = payment_ids_for_phase(phase)
    allocation_ids = allocation_ids_for_phase(phase)

    rejected_donation_amount = Decimal(110 + phase)
    refunded_donation_amount = Decimal(220 + phase)
    pending_donation_amount = Decimal(900 + phase)
    rejected_expense_amount = Decimal(25 + phase)
    pending_expense_amount = Decimal(35 + phase)
    approved_payment_amount = Decimal(40 + phase)
    cancelled_payment_amount = Decimal(50 + phase)
    rejected_payment_amount = Decimal(60 + phase)
    pending_payment_amount = Decimal(70 + phase)
    internal_budget_amount = Decimal(15 + phase)

    donation_cols = ["id", "donor_id", "campaign_id", "amount", "currency", "donation_type", "donation_date", "status", "reference_code", "created_at", "updated_at"]
    donations = [
        (donation_ids["confirmed"], TEST_ID_BASE + 1, TEST_ID_BASE + 101, confirmed_donation_amount, "IRR", "online", dstr(d), "confirmed", f"MD-DON-{phase:03d}-CONF", dtstr(d, 10), None),
        (donation_ids["pending"], TEST_ID_BASE + 1, TEST_ID_BASE + 101, pending_donation_amount, "IRR", "online", dstr(d), "pending", f"MD-DON-{phase:03d}-PEND", dtstr(d, 10), None),
        (donation_ids["rejected"], TEST_ID_BASE + 2, TEST_ID_BASE + 102, rejected_donation_amount, "IRR", "cash", dstr(d), "rejected", f"MD-DON-{phase:03d}-REJ", dtstr(d, 10), None),
        (donation_ids["refunded"], TEST_ID_BASE + 2, TEST_ID_BASE + 102, refunded_donation_amount, "IRR", "bank_transfer", dstr(d), "refunded", f"MD-DON-{phase:03d}-REF", dtstr(d, 10), None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.donations", donation_cols, donations)

    expense_cols = ["id", "center_id", "child_id", "category_id", "amount", "currency", "expense_date", "description", "approved_by_user_id", "status", "created_at", "updated_at"]
    expenses = [
        (expense_ids["approved_c1"], 1, 1, TEST_ID_BASE + 202, c1_exp, "IRR", dstr(d), f"MD phase {phase} center 1 approved expense", 10, "approved", dtstr(d, 11), None),
        (expense_ids["approved_c2"], 2, 2, TEST_ID_BASE + 203, c2_exp, "IRR", dstr(d), f"MD phase {phase} center 2 approved expense", 10, "approved", dtstr(d, 11), None),
        (expense_ids["pending"], 1, 1, TEST_ID_BASE + 204, pending_expense_amount, "IRR", dstr(d), f"MD phase {phase} pending expense", 10, "pending", dtstr(d, 11), None),
        # Edge: child_id is NULL. It should map to unknown child_key if loaded.
        (expense_ids["rejected"], 1, None, TEST_ID_BASE + 204, rejected_expense_amount, "IRR", dstr(d), f"MD phase {phase} rejected null-child expense", 10, "rejected", dtstr(d, 11), None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.expenses", expense_cols, expenses)

    payment_cols = ["id", "payment_type", "teacher_id", "center_id", "amount", "currency", "payment_date", "status", "created_at", "updated_at"]
    payments = [
        (payment_ids["paid_c1"], "salary", 1001, 1, c1_pay, "IRR", dstr(d), "paid", dtstr(d, 12), None),
        (payment_ids["paid_c2"], "vendor", None, 2, c2_pay, "IRR", dstr(d), "paid", dtstr(d, 12), None),
        (payment_ids["pending"], "bonus", 1001, 1, pending_payment_amount, "IRR", dstr(d), "pending", dtstr(d, 12), None),
        (payment_ids["approved"], "bonus", 1001, 1, approved_payment_amount, "IRR", dstr(d), "approved", dtstr(d, 12), None),
        (payment_ids["cancelled"], "refund", None, 1, cancelled_payment_amount, "IRR", dstr(d), "cancelled", dtstr(d, 12), None),
        (payment_ids["rejected"], "vendor", None, 2, rejected_payment_amount, "IRR", dstr(d), "rejected", dtstr(d, 12), None),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.payments", payment_cols, payments)

    allocation_cols = ["id", "source_type", "source_id", "center_id", "child_id", "category_id", "allocated_amount", "allocation_date", "reason", "created_at"]
    allocations = [
        (allocation_ids["donation_c1"], "donation", donation_ids["confirmed"], 1, 1, TEST_ID_BASE + 202, c1_don, dstr(d), f"MD phase {phase} allocation center 1", dtstr(d, 13)),
        (allocation_ids["donation_c2"], "donation", donation_ids["confirmed"], 2, 2, TEST_ID_BASE + 203, c2_don, dstr(d), f"MD phase {phase} allocation center 2", dtstr(d, 13)),
        # Edge: internal budget allocation should load as a relationship row with unknown donor/campaign.
        (allocation_ids["internal_budget_c1"], "internal_budget", None, 1, None, TEST_ID_BASE + 205, internal_budget_amount, dstr(d), f"MD phase {phase} internal budget allocation", dtstr(d, 13)),
    ]
    insert_with_identity(conn, f"{SOURCE_DB}.finance_ops.budget_allocations", allocation_cols, allocations)

    tx_cols = ["id", "entity_type", "entity_id", "transaction_type", "amount", "transaction_date", "created_at"]
    tx_rows = [
        (TEST_ID_BASE + 5000 + phase * 100 + 1, "donation", donation_ids["confirmed"], "credit", confirmed_donation_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 2, "donation", donation_ids["rejected"], "credit", rejected_donation_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 3, "donation", donation_ids["refunded"], "credit", refunded_donation_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 4, "expense", expense_ids["approved_c1"], "debit", c1_exp, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 5, "expense", expense_ids["approved_c2"], "debit", c2_exp, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 6, "expense", expense_ids["rejected"], "debit", rejected_expense_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 7, "payment", payment_ids["paid_c1"], "debit", c1_pay, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 8, "payment", payment_ids["paid_c2"], "debit", c2_pay, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 9, "payment", payment_ids["approved"], "debit", approved_payment_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 10, "payment", payment_ids["cancelled"], "debit", cancelled_payment_amount, dstr(d), dtstr(d, 14)),
        (TEST_ID_BASE + 5000 + phase * 100 + 11, "payment", payment_ids["rejected"], "debit", rejected_payment_amount, dstr(d), dtstr(d, 14)),
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
        "rejected_donation_amount": rejected_donation_amount,
        "refunded_donation_amount": refunded_donation_amount,
        "pending_donation_amount": pending_donation_amount,
        "rejected_expense_amount": rejected_expense_amount,
        "pending_expense_amount": pending_expense_amount,
        "approved_payment_amount": approved_payment_amount,
        "cancelled_payment_amount": cancelled_payment_amount,
        "rejected_payment_amount": rejected_payment_amount,
        "pending_payment_amount": pending_payment_amount,
        "internal_budget_amount": internal_budget_amount,
        "donation_ids": donation_ids,
        "expense_ids": expense_ids,
        "payment_ids": payment_ids,
        "allocation_ids": allocation_ids,
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
# Verification helpers
# =============================================================================


def get_center_key(conn: pyodbc.Connection, center_id: int) -> int:
    val = scalar(conn, f"SELECT TOP (1) center_key FROM {DW_DB}.dw.dim_center WHERE center_id = ?", [center_id])
    return int(val) if val is not None else -1


def month_key_for(d: date) -> int:
    # d is chosen as month-end, so YYYYMMDD is the month_key in dim_date.
    return int(d.strftime("%Y%m%d"))


def fact_map_count(conn: pyodbc.Connection, fact_name: str, source_table: str) -> int:
    if not table_exists(conn, f"{DW_DB}.etl_work.fact_source_load_map"):
        return 0
    return int(scalar(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {DW_DB}.etl_work.fact_source_load_map
        WHERE fact_name = ?
          AND source_table = ?
          AND source_id >= ? AND source_id < ?
        """,
        [fact_name, source_table, TEST_ID_BASE, TEST_ID_BASE + 100_000],
    ) or 0)


def fact_count_by_map_batches(conn: pyodbc.Connection, fact_table: str, fact_name: str, source_table: str) -> int:
    batches = map_batch_ids_for_test_rows(conn, fact_name, source_table)
    if not batches:
        return 0
    ph = placeholders(batches)
    return int(scalar(conn, f"SELECT COUNT(1) FROM {DW_DB}.dw.{fact_table} WHERE etl_batch_id IN ({ph})", batches) or 0)


def source_id_loaded(conn: pyodbc.Connection, fact_name: str, source_table: str, source_id: int) -> bool:
    if not table_exists(conn, f"{DW_DB}.etl_work.fact_source_load_map"):
        return False
    cnt = scalar(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {DW_DB}.etl_work.fact_source_load_map
        WHERE fact_name = ? AND source_table = ? AND source_id = ?
        """,
        [fact_name, source_table, source_id],
    )
    return int(cnt or 0) > 0


def status_counts_for_fact(conn: pyodbc.Connection, fact_table: str, fact_name: str, source_table: str) -> dict[str, int]:
    batches = map_batch_ids_for_test_rows(conn, fact_name, source_table)
    if not batches:
        return {}
    ph = placeholders(batches)
    rows = fetch_all(
        conn,
        f"""
        SELECT st.status_type, st.code, COUNT(1) AS cnt
        FROM {DW_DB}.dw.{fact_table} f
        INNER JOIN {DW_DB}.dw.dim_status st ON st.status_key = f.status_key
        WHERE f.etl_batch_id IN ({ph})
        GROUP BY st.status_type, st.code
        """,
        batches,
    )
    return {str(r.code): int(r.cnt) for r in rows}


def test_lifecycle_count(conn: pyodbc.Connection) -> int:
    if not table_exists(conn, f"{DW_DB}.dw.fact_donation_lifecycle"):
        return 0
    return int(scalar(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {DW_DB}.dw.fact_donation_lifecycle f
        INNER JOIN {DW_DB}.dw.dim_donor d ON d.donor_key = f.donor_key
        INNER JOIN {DW_DB}.dw.dim_campaign c ON c.campaign_key = f.campaign_key
        WHERE d.donor_id >= ? AND d.donor_id < ?
          AND c.campaign_id >= ? AND c.campaign_id < ?
          AND f.created_date_key BETWEEN 20280101 AND 20280630
        """,
        [TEST_ID_BASE, TEST_ID_BASE + 100, TEST_ID_BASE + 100, TEST_ID_BASE + 200],
    ) or 0)


def capture_fact_state(conn: pyodbc.Connection) -> dict[str, int]:
    return {
        "donation_map": fact_map_count(conn, "fact_donation_transaction", "donations"),
        "expense_map": fact_map_count(conn, "fact_expense_transaction", "expenses"),
        "payment_map": fact_map_count(conn, "fact_payment_transaction", "payments"),
        "allocation_map": fact_map_count(conn, "fact_budget_allocation_event", "budget_allocations"),
        "donation_fact": fact_count_by_map_batches(conn, "fact_donation_transaction", "fact_donation_transaction", "donations"),
        "expense_fact": fact_count_by_map_batches(conn, "fact_expense_transaction", "fact_expense_transaction", "expenses"),
        "payment_fact": fact_count_by_map_batches(conn, "fact_payment_transaction", "fact_payment_transaction", "payments"),
        "allocation_fact": fact_count_by_map_batches(conn, "fact_budget_allocation_event", "fact_budget_allocation_event", "budget_allocations"),
        "lifecycle_fact": test_lifecycle_count(conn),
    }


# =============================================================================
# Verification
# =============================================================================


def verify_architecture_contract(conn: pyodbc.Connection) -> None:
    print("Verifying latest DW/STG architecture contract...")

    mart2_dims = ["dim_donor", "dim_campaign", "dim_category", "dim_donation_type", "dim_status", "dim_currency"]
    facts = [
        "fact_donation_transaction", "fact_expense_transaction", "fact_payment_transaction",
        "fact_monthly_financial_snapshot", "fact_donation_lifecycle", "fact_budget_allocation_event",
    ]

    for table in mart2_dims + facts:
        full = f"{DW_DB}.dw.{table}"
        check(f"architecture: {table} has no source_system", not col_exists(conn, full, "source_system"))

    removed_fact_columns = {
        "fact_donation_transaction": ["source_donation_id", "source_donation_code", "source_system"],
        "fact_expense_transaction": ["source_expense_id", "source_expense_code", "description", "source_system"],
        "fact_payment_transaction": ["source_payment_id", "source_payment_code", "source_system"],
        "fact_donation_lifecycle": ["source_donation_id", "days_to_confirm", "days_to_allocate", "source_system"],
        "fact_budget_allocation_event": ["source_allocation_id", "allocated_amount", "reason", "allocation_type_key", "source_system"],
    }
    for table, cols in removed_fact_columns.items():
        full = f"{DW_DB}.dw.{table}"
        for col in cols:
            check(f"architecture: {table}.{col} removed", not col_exists(conn, full, col))

    for col in ["min_donation", "max_donation", "avg_donation"]:
        check(f"architecture: fact_donation_lifecycle.{col} exists", col_exists(conn, f"{DW_DB}.dw.fact_donation_lifecycle", col))

    check("architecture: dim_allocation_type removed", not table_exists(conn, f"{DW_DB}.dw.dim_allocation_type"))
    check("architecture: source expense_categories.parent_id removed", not col_exists(conn, f"{SOURCE_DB}.finance_ops.expense_categories", "parent_id"))
    check("architecture: staging expense_categories.parent_id removed", not col_exists(conn, f"{STG_DB}.stg_finance_ops.expense_categories", "parent_id"))
    check("architecture: fact_source_load_map exists", table_exists(conn, f"{DW_DB}.etl_work.fact_source_load_map"))


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

    missing_status_rows = []
    expected = [
        *[("donation", s) for s in ("pending", *VALID_DONATION_STATUSES)],
        *[("expense", s) for s in ("pending", *VALID_EXPENSE_STATUSES)],
        *[("payment", s) for s in ("pending", *VALID_PAYMENT_STATUSES)],
    ]
    for status_type, code in expected:
        cnt = scalar(conn, f"SELECT COUNT(1) FROM {DW_DB}.dw.dim_status WHERE status_type=? AND code=?", [status_type, code])
        if int(cnt or 0) != 1:
            missing_status_rows.append(f"{status_type}:{code}={cnt}")
    check(f"P{phase}: dim_status has explicit donation/expense/payment status domains", not missing_status_rows, "; ".join(missing_status_rows))


def verify_fact_counts(conn: pyodbc.Connection, cumulative_phases: int) -> None:
    expected_map_counts = [
        ("donations", "fact_donation_transaction", "donations", cumulative_phases * 3),
        ("expenses", "fact_expense_transaction", "expenses", cumulative_phases * 3),
        ("payments", "fact_payment_transaction", "payments", cumulative_phases * 5),
        ("allocations", "fact_budget_allocation_event", "budget_allocations", cumulative_phases * 3),
    ]
    for label, fact_name, source_table, expected in expected_map_counts:
        cnt = fact_map_count(conn, fact_name, source_table)
        check(f"source-map cumulative {label} count", cnt == expected, f"count={cnt}, expected={expected}")

    expected_fact_counts = [
        ("donations", "fact_donation_transaction", "fact_donation_transaction", "donations", cumulative_phases * 3),
        ("expenses", "fact_expense_transaction", "fact_expense_transaction", "expenses", cumulative_phases * 3),
        ("payments", "fact_payment_transaction", "fact_payment_transaction", "payments", cumulative_phases * 5),
        ("allocations", "fact_budget_allocation_event", "fact_budget_allocation_event", "budget_allocations", cumulative_phases * 3),
    ]
    for label, fact_table, fact_name, source_table, expected in expected_fact_counts:
        cnt = fact_count_by_map_batches(conn, fact_table, fact_name, source_table)
        check(f"facts cumulative {label} count via source-map batches", cnt == expected, f"count={cnt}, expected={expected}")

    # Lifecycle is now an aggregate snapshot at donor/campaign/current_stage grain.
    # The deterministic test data creates exactly three lifecycle groups:
    #   1) donor 1 / campaign A / allocated
    #   2) donor 2 / campaign B / rejected
    #   3) donor 2 / campaign B / refunded
    life_cnt = test_lifecycle_count(conn)
    expected_lifecycle_groups = 3 if cumulative_phases > 0 else 0
    check("facts cumulative lifecycle aggregate-group count", life_cnt == expected_lifecycle_groups, f"count={life_cnt}, expected={expected_lifecycle_groups}")

    for fact_table, fact_name, source_table in [
        ("fact_donation_transaction", "fact_donation_transaction", "donations"),
        ("fact_expense_transaction", "fact_expense_transaction", "expenses"),
        ("fact_payment_transaction", "fact_payment_transaction", "payments"),
        ("fact_budget_allocation_event", "fact_budget_allocation_event", "budget_allocations"),
    ]:
        batches = map_batch_ids_for_test_rows(conn, fact_name, source_table)
        if not batches:
            check(f"{fact_table}: no date_key=-1 for test rows", False, "no test ETL batches found")
            continue
        ph = placeholders(batches)
        bad = scalar(conn, f"SELECT COUNT(1) FROM {DW_DB}.dw.{fact_table} WHERE etl_batch_id IN ({ph}) AND date_key = -1", batches)
        check(f"{fact_table}: no date_key=-1 for test rows", int(bad or 0) == 0, f"bad_count={bad}")


def verify_status_rules(conn: pyodbc.Connection, cumulative_phases: int, expected_by_phase: dict[int, dict[str, Any]]) -> None:
    donation_counts = status_counts_for_fact(conn, "fact_donation_transaction", "fact_donation_transaction", "donations")
    expected_donation_counts = {"confirmed": cumulative_phases, "rejected": cumulative_phases, "refunded": cumulative_phases}
    for status, expected in expected_donation_counts.items():
        check(f"status: donation {status} loaded", donation_counts.get(status, 0) == expected, f"counts={donation_counts}")
    check("status: donation pending not loaded", donation_counts.get("pending", 0) == 0, f"counts={donation_counts}")

    expense_counts = status_counts_for_fact(conn, "fact_expense_transaction", "fact_expense_transaction", "expenses")
    expected_expense_counts = {"approved": cumulative_phases * 2, "rejected": cumulative_phases}
    for status, expected in expected_expense_counts.items():
        check(f"status: expense {status} loaded", expense_counts.get(status, 0) == expected, f"counts={expense_counts}")
    check("status: expense pending not loaded", expense_counts.get("pending", 0) == 0, f"counts={expense_counts}")

    payment_counts = status_counts_for_fact(conn, "fact_payment_transaction", "fact_payment_transaction", "payments")
    expected_payment_counts = {
        "approved": cumulative_phases,
        "paid": cumulative_phases * 2,
        "cancelled": cumulative_phases,
        "rejected": cumulative_phases,
    }
    for status, expected in expected_payment_counts.items():
        check(f"status: payment {status} loaded", payment_counts.get(status, 0) == expected, f"counts={payment_counts}")
    check("status: payment pending not loaded", payment_counts.get("pending", 0) == 0, f"counts={payment_counts}")

    # Check pending source IDs are not inserted into the source-map for their facts.
    pending_failures = []
    for phase in range(1, cumulative_phases + 1):
        exp = expected_by_phase[phase]
        if source_id_loaded(conn, "fact_donation_transaction", "donations", exp["donation_ids"]["pending"]):
            pending_failures.append(f"donation_p{phase}")
        if source_id_loaded(conn, "fact_expense_transaction", "expenses", exp["expense_ids"]["pending"]):
            pending_failures.append(f"expense_p{phase}")
        if source_id_loaded(conn, "fact_payment_transaction", "payments", exp["payment_ids"]["pending"]):
            pending_failures.append(f"payment_p{phase}")
    check("status: pending source IDs absent from fact_source_load_map", not pending_failures, ", ".join(pending_failures))


def verify_status_flags(conn: pyodbc.Connection, cumulative_phases: int) -> None:
    # Donation flags.
    batches = map_batch_ids_for_test_rows(conn, "fact_donation_transaction", "donations")
    if batches:
        ph = placeholders(batches)
        rows = fetch_all(
            conn,
            f"""
            SELECT st.code,
                   SUM(CASE WHEN f.is_confirmed = 1 THEN 1 ELSE 0 END) AS confirmed_flags,
                   SUM(CASE WHEN f.is_refunded = 1 THEN 1 ELSE 0 END) AS refunded_flags
            FROM {DW_DB}.dw.fact_donation_transaction f
            INNER JOIN {DW_DB}.dw.dim_status st ON st.status_key = f.status_key
            WHERE f.etl_batch_id IN ({ph})
            GROUP BY st.code
            """,
            batches,
        )
        data = {str(r.code): (int(r.confirmed_flags or 0), int(r.refunded_flags or 0)) for r in rows}
        check("flags: confirmed donations have is_confirmed=1", data.get("confirmed") == (cumulative_phases, 0), f"flags={data}")
        check("flags: refunded donations have is_refunded=1", data.get("refunded") == (0, cumulative_phases), f"flags={data}")
        check("flags: rejected donations have no confirmed/refunded flag", data.get("rejected") == (0, 0), f"flags={data}")

    # Expense flags.
    batches = map_batch_ids_for_test_rows(conn, "fact_expense_transaction", "expenses")
    if batches:
        ph = placeholders(batches)
        rows = fetch_all(
            conn,
            f"""
            SELECT st.code,
                   SUM(CASE WHEN f.is_approved = 1 THEN 1 ELSE 0 END) AS approved_flags,
                   SUM(CASE WHEN f.is_rejected = 1 THEN 1 ELSE 0 END) AS rejected_flags
            FROM {DW_DB}.dw.fact_expense_transaction f
            INNER JOIN {DW_DB}.dw.dim_status st ON st.status_key = f.status_key
            WHERE f.etl_batch_id IN ({ph})
            GROUP BY st.code
            """,
            batches,
        )
        data = {str(r.code): (int(r.approved_flags or 0), int(r.rejected_flags or 0)) for r in rows}
        check("flags: approved expenses have is_approved=1", data.get("approved") == (cumulative_phases * 2, 0), f"flags={data}")
        check("flags: rejected expenses have is_rejected=1", data.get("rejected") == (0, cumulative_phases), f"flags={data}")

    # Payment flags.
    batches = map_batch_ids_for_test_rows(conn, "fact_payment_transaction", "payments")
    if batches:
        ph = placeholders(batches)
        rows = fetch_all(
            conn,
            f"""
            SELECT st.code,
                   SUM(CASE WHEN f.is_paid = 1 THEN 1 ELSE 0 END) AS paid_flags,
                   SUM(CASE WHEN f.is_cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flags
            FROM {DW_DB}.dw.fact_payment_transaction f
            INNER JOIN {DW_DB}.dw.dim_status st ON st.status_key = f.status_key
            WHERE f.etl_batch_id IN ({ph})
            GROUP BY st.code
            """,
            batches,
        )
        data = {str(r.code): (int(r.paid_flags or 0), int(r.cancelled_flags or 0)) for r in rows}
        check("flags: paid payments have is_paid=1", data.get("paid") == (cumulative_phases * 2, 0), f"flags={data}")
        check("flags: cancelled payments have is_cancelled=1", data.get("cancelled") == (0, cumulative_phases), f"flags={data}")
        check("flags: approved payments have no paid/cancelled flag", data.get("approved") == (0, 0), f"flags={data}")
        check("flags: rejected payments have no paid/cancelled flag", data.get("rejected") == (0, 0), f"flags={data}")


def verify_edge_cases(conn: pyodbc.Connection, cumulative_phases: int) -> None:
    # Null-child rejected expense should load, but child_key should be unknown (-1).
    batches = map_batch_ids_for_test_rows(conn, "fact_expense_transaction", "expenses")
    if batches:
        ph = placeholders(batches)
        cnt = scalar(
            conn,
            f"""
            SELECT COUNT(1)
            FROM {DW_DB}.dw.fact_expense_transaction f
            INNER JOIN {DW_DB}.dw.dim_status st ON st.status_key = f.status_key
            WHERE f.etl_batch_id IN ({ph})
              AND st.status_type=N'expense'
              AND st.code=N'rejected'
              AND f.child_key = -1
            """,
            batches,
        )
        check("edge: rejected NULL-child expenses map to unknown child_key", int(cnt or 0) == cumulative_phases, f"count={cnt}")

    # Internal-budget allocation is fact-less and should have unknown donor/campaign.
    batches = map_batch_ids_for_test_rows(conn, "fact_budget_allocation_event", "budget_allocations")
    if batches:
        ph = placeholders(batches)
        cnt = scalar(
            conn,
            f"""
            SELECT COUNT(1)
            FROM {DW_DB}.dw.fact_budget_allocation_event f
            WHERE f.etl_batch_id IN ({ph})
              AND f.donor_key = -1
              AND f.campaign_key = -1
            """,
            batches,
        )
        check("edge: internal_budget allocation loads as unknown donor/campaign relationship", int(cnt or 0) == cumulative_phases, f"count={cnt}")

    # Fact-less fact must not have measure/text/source columns.
    for col in ["allocated_amount", "reason", "source_allocation_id"]:
        check(f"edge: allocation event remains fact-less without {col}", not col_exists(conn, f"{DW_DB}.dw.fact_budget_allocation_event", col))


def verify_lifecycle_measures(conn: pyodbc.Connection, cumulative_phases: int, expected_by_phase: dict[int, dict[str, Any]]) -> None:
    rows = fetch_all(
        conn,
        f"""
        SELECT d.donor_id, c.campaign_id, f.donation_amount, f.min_donation, f.max_donation, f.avg_donation, f.current_stage
        FROM {DW_DB}.dw.fact_donation_lifecycle f
        INNER JOIN {DW_DB}.dw.dim_donor d ON d.donor_key = f.donor_key
        INNER JOIN {DW_DB}.dw.dim_campaign c ON c.campaign_key = f.campaign_key
        WHERE d.donor_id >= ? AND d.donor_id < ?
          AND c.campaign_id >= ? AND c.campaign_id < ?
          AND f.created_date_key BETWEEN 20280101 AND 20280630
        """,
        [TEST_ID_BASE, TEST_ID_BASE + 100, TEST_ID_BASE + 100, TEST_ID_BASE + 200],
    )

    # Lifecycle donation_amount must be SUM(donation amount), not the amount of one source donation.
    # Because lifecycle_status/current_stage is still present, the correct aggregate grain is:
    # donor_key + campaign_key + current_stage.
    expected_groups: dict[tuple[int, int, str], list[Decimal]] = {}
    for phase in range(1, cumulative_phases + 1):
        exp = expected_by_phase[phase]
        main_key = (TEST_ID_BASE + 1, TEST_ID_BASE + 101, "allocated")
        rejected_key = (TEST_ID_BASE + 2, TEST_ID_BASE + 102, "rejected")
        refunded_key = (TEST_ID_BASE + 2, TEST_ID_BASE + 102, "refunded")
        expected_groups.setdefault(main_key, []).append(exp["center1_donation"] + exp["center2_donation"])
        expected_groups.setdefault(rejected_key, []).append(exp["rejected_donation_amount"])
        expected_groups.setdefault(refunded_key, []).append(exp["refunded_donation_amount"])

    actual_by_group: dict[tuple[int, int, str], tuple[Decimal, Decimal, Decimal, Decimal]] = {}
    duplicate_groups: dict[tuple[int, int, str], int] = {}
    for r in rows:
        key = (int(r.donor_id), int(r.campaign_id), str(r.current_stage))
        if key in actual_by_group:
            duplicate_groups[key] = duplicate_groups.get(key, 1) + 1
        actual_by_group[key] = (dec(r.donation_amount), dec(r.min_donation), dec(r.max_donation), dec(r.avg_donation))

    missing = [key for key in expected_groups if key not in actual_by_group]
    unexpected = [key for key in actual_by_group if key not in expected_groups]
    check("lifecycle: expected donor/campaign/stage aggregate groups exist", not missing, f"missing={missing}")
    check("lifecycle: no unexpected donor/campaign/stage groups", not unexpected, f"unexpected={unexpected}")
    check("lifecycle: one row per donor/campaign/stage aggregate group", not duplicate_groups, f"duplicates={duplicate_groups}")

    for key, values in expected_groups.items():
        if key not in actual_by_group:
            continue
        expected_sum = dec(sum(values))
        expected_min = dec(min(values))
        expected_max = dec(max(values))
        expected_avg = dec(sum(values) / Decimal(len(values)))
        actual_sum, actual_min, actual_max, actual_avg = actual_by_group[key]
        ok = (
            actual_sum == expected_sum and
            actual_min == expected_min and
            actual_max == expected_max and
            actual_avg == expected_avg
        )
        check(
            f"lifecycle: SUM/min/max/avg correct for {key}",
            ok,
            f"actual=({actual_sum},{actual_min},{actual_max},{actual_avg}) expected=({expected_sum},{expected_min},{expected_max},{expected_avg})",
        )

    stage_counts = {}
    for _, _, stage in actual_by_group:
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
    check("lifecycle: allocated aggregate stage exists", stage_counts.get("allocated", 0) == 1, f"stages={stage_counts}")
    check("lifecycle: rejected aggregate stage exists", stage_counts.get("rejected", 0) == 1, f"stages={stage_counts}")
    check("lifecycle: refunded aggregate stage exists", stage_counts.get("refunded", 0) == 1, f"stages={stage_counts}")
    check("lifecycle: pending stage absent", stage_counts.get("pending", 0) == 0, f"stages={stage_counts}")

def verify_snapshot_for_phase(conn: pyodbc.Connection, phase: int, d: date, expected: dict[str, Any]) -> None:
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
            # confirmed donation allocation + internal_budget allocation
            "allocation_count": 2,
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


def verify_previous_transaction_source_unchanged(conn: pyodbc.Connection, max_phase: int, expected_by_phase: dict[int, dict[str, Any]]) -> None:
    """The script must not change transaction rows from previous days. Verify source amounts still match expected."""
    bad = 0
    for phase in range(1, max_phase + 1):
        exp = expected_by_phase[phase]
        dids = exp["donation_ids"]
        eids = exp["expense_ids"]
        pids = exp["payment_ids"]

        donation_expected = {
            dids["confirmed"]: exp["center1_donation"] + exp["center2_donation"],
            dids["pending"]: exp["pending_donation_amount"],
            dids["rejected"]: exp["rejected_donation_amount"],
            dids["refunded"]: exp["refunded_donation_amount"],
        }
        for source_id, expected_amount in donation_expected.items():
            amount = scalar(conn, f"SELECT amount FROM {SOURCE_DB}.finance_ops.donations WHERE id = ?", [source_id])
            if dec(amount) != dec(expected_amount):
                bad += 1

        expense_expected = {
            eids["approved_c1"]: exp["center1_expense"],
            eids["approved_c2"]: exp["center2_expense"],
            eids["pending"]: exp["pending_expense_amount"],
            eids["rejected"]: exp["rejected_expense_amount"],
        }
        for source_id, expected_amount in expense_expected.items():
            amount = scalar(conn, f"SELECT amount FROM {SOURCE_DB}.finance_ops.expenses WHERE id = ?", [source_id])
            if dec(amount) != dec(expected_amount):
                bad += 1

        payment_expected = {
            pids["paid_c1"]: exp["center1_payment"],
            pids["paid_c2"]: exp["center2_payment"],
            pids["pending"]: exp["pending_payment_amount"],
            pids["approved"]: exp["approved_payment_amount"],
            pids["cancelled"]: exp["cancelled_payment_amount"],
            pids["rejected"]: exp["rejected_payment_amount"],
        }
        for source_id, expected_amount in payment_expected.items():
            amount = scalar(conn, f"SELECT amount FROM {SOURCE_DB}.finance_ops.payments WHERE id = ?", [source_id])
            if dec(amount) != dec(expected_amount):
                bad += 1

    check(f"transaction source rows unchanged through phase {max_phase}", bad == 0, f"bad_amount_rows={bad}")


def verify_phase(conn: pyodbc.Connection, phase: int, d: date, cumulative_phases: int, expected_by_phase: dict[int, dict[str, Any]]) -> None:
    label = f"P{phase}"
    verify_architecture_contract(conn)
    verify_staging_validity(conn, label)
    verify_dimensions(conn, phase)
    verify_fact_counts(conn, cumulative_phases)
    verify_status_rules(conn, cumulative_phases, expected_by_phase)
    verify_status_flags(conn, cumulative_phases)
    verify_edge_cases(conn, cumulative_phases)
    verify_lifecycle_measures(conn, cumulative_phases, expected_by_phase)
    for p in range(1, cumulative_phases + 1):
        pd = PHASES[p - 1][1]
        verify_snapshot_for_phase(conn, p, pd, expected_by_phase[p])
    verify_previous_transaction_source_unchanged(conn, cumulative_phases, expected_by_phase)


def verify_idempotent_rerun(conn: pyodbc.Connection, start_time: str, end_time: str, expected_by_phase: dict[int, dict[str, Any]]) -> None:
    print("Running idempotency edge test: rerun same final ETL window and verify no duplicates...")
    before = capture_fact_state(conn)
    run_dw_incremental(conn, start_time, end_time)
    after = capture_fact_state(conn)
    check("edge: rerun does not duplicate mapped/fact rows", before == after, f"before={before}, after={after}")
    verify_status_rules(conn, len(expected_by_phase), expected_by_phase)
    verify_lifecycle_measures(conn, len(expected_by_phase), expected_by_phase)


# =============================================================================
# Main flow
# =============================================================================


def print_summary() -> None:
    total = len(RESULTS)
    passed = sum(1 for _, ok, _ in RESULTS)
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
    print("Finance MART 2 multi-day ETL flow test - fixed for latest DW/STG")
    print(f"Server: {SERVER}")
    print(f"Test ID base: {TEST_ID_BASE}")
    print("This script is intended for an empty/test database or an isolated 2028 test range.")

    expected_by_phase: dict[int, dict[str, Any]] = {}
    conn = connect()
    try:
        if CLEAN_PREVIOUS_TEST_ROWS:
            clean_previous_test_rows(conn)
        ensure_dim_date(conn)
        ensure_conformed_dims(conn)

        # Phase 1: first load up to day/month 1.
        phase, d, start_time, end_time = PHASES[0]
        print("\n" + "-" * 80)
        print(f"PHASE {phase}: FIRST LOAD through {d}")
        insert_base_non_transaction_data(conn)
        update_at_least_10_non_transaction_rows(conn, phase, d)
        expected_by_phase[phase] = insert_transaction_data_for_phase(conn, phase, d)
        run_source_to_staging(conn, end_time)
        run_dw_first_load(conn, start_time, end_time)
        verify_phase(conn, phase, d, 1, expected_by_phase)

        # Phases 2, 3, 4: one-day/month normal runs.
        for phase, d, start_time, end_time in PHASES[1:4]:
            print("\n" + "-" * 80)
            print(f"PHASE {phase}: NORMAL daily/monthly load for {d}")
            update_at_least_10_non_transaction_rows(conn, phase, d)
            expected_by_phase[phase] = insert_transaction_data_for_phase(conn, phase, d)
            run_source_to_staging(conn, end_time)
            run_dw_incremental(conn, start_time, end_time)
            verify_phase(conn, phase, d, phase, expected_by_phase)

        # Final phase: add two periods, run one two-period ETL window.
        print("\n" + "-" * 80)
        print("FINAL PHASE: add two periods of data and run one two-period ETL window")
        for phase, d, _, _ in PHASES[4:6]:
            update_at_least_10_non_transaction_rows(conn, phase, d)
            expected_by_phase[phase] = insert_transaction_data_for_phase(conn, phase, d)
        final_start = PHASES[4][2]
        final_end = PHASES[5][3]
        run_source_to_staging(conn, final_end)
        run_dw_incremental(conn, final_start, final_end)
        verify_phase(conn, 6, PHASES[5][1], 6, expected_by_phase)

        # Extra edge test: rerunning the same final window must not create duplicates.
        verify_idempotent_rerun(conn, final_start, final_end, expected_by_phase)

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
