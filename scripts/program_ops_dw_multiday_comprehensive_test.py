#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ProgramOps DW Multi-Day Comprehensive Flow Test
===============================================

This script tests the revised ETL rules over multiple daily runs and validates warehouse record correctness, aggregate correctness, append-only behavior, and important edge cases.

Flow:
    1. Reset Source / Staging / DW except dim_date.
    2. Seed Day 1 data.
    3. Run Source -> Staging.
    4. Run DW FIRST LOAD for Day 1 only.
    5. Validate DW.
    6. For Day 2, Day 3, Day 4, Day 5:
          - Add at least 10 non-transaction source changes.
          - Add only new transaction rows for that day.
          - Never update previous-day transactions.
          - Run Source -> Staging.
          - Run DW INCREMENTAL for that single day.
          - Validate DW.
    7. Final step:
          - Add Day 6 and Day 7 data.
          - Each day has at least 10 non-transaction source changes.
          - Add only new transaction rows for those two days.
          - Run one 2-day DW INCREMENTAL window.
          - Validate DW.

Revised ETL assumptions:
    - Type 1 dimensions may use truncate + insert internally.
    - Type 2 dimensions close old current rows and insert new versions.
    - Transaction facts are append-only. They do not update old fact rows.
    - Factless/event facts are append-only.
    - Daily snapshot fact creates daily rows with a loop and does not update old days.
    - Lifecycle/accumulating fact may rebuild itself from work tables.

Install:
    pip install pyodbc

Run:
    python program_ops_dw_multiday_flow_test.py --server localhost --trusted

or:
    python program_ops_dw_multiday_flow_test.py --server localhost --user sa --password "YourPassword"
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable

try:
    import pyodbc
except ImportError as exc:
    raise SystemExit(
        "pyodbc is not installed. Install it with:\n\n"
        "    pip install pyodbc\n"
    ) from exc


SOURCE_DB = "Source_ProgramOps_DB"
STG_DB = "Stg_ProgramOps_DB"
DW_DB = "Charity_DW_DB"

BASE_DATE = dt.date(2028, 1, 1)
FINAL_END_DATE = dt.date(2028, 1, 8)


@dataclass
class TestState:
    center_ids: list[int] = field(default_factory=list)
    teacher_ids: list[int] = field(default_factory=list)
    user_ids: list[int] = field(default_factory=list)
    child_ids: list[int] = field(default_factory=list)
    domain_ids: list[int] = field(default_factory=list)
    score_scale_ids: list[int] = field(default_factory=list)
    task_template_ids: list[int] = field(default_factory=list)
    no_score_reason_ids: list[int] = field(default_factory=list)
    absence_reason_ids: list[int] = field(default_factory=list)
    closure_reason_ids: list[int] = field(default_factory=list)
    child_plans: dict[int, list[int]] = field(default_factory=dict)
    teacher_user_by_teacher: dict[int, int] = field(default_factory=dict)

    expected_assignments: int = 0
    expected_assessments: int = 0
    expected_event_rows: int = 0
    expected_non_transaction_changes: dict[dt.date, int] = field(default_factory=dict)

    assignment_ids_by_day: dict[dt.date, list[int]] = field(default_factory=dict)
    assessment_ids_by_day: dict[dt.date, list[int]] = field(default_factory=dict)

    # Stored after each validation to prove previous-day facts/snapshots do not change later.
    tran_signature_by_date_key: dict[int, tuple[Any, ...]] = field(default_factory=dict)
    event_signature_by_date_key: dict[int, tuple[Any, ...]] = field(default_factory=dict)
    snapshot_signature_by_date_key: dict[int, tuple[Any, ...]] = field(default_factory=dict)


# -----------------------------------------------------------------------------
# SQL helpers
# -----------------------------------------------------------------------------

def build_connection_string(args: argparse.Namespace) -> str:
    parts = [
        f"DRIVER={{{args.driver}}}",
        f"SERVER={args.server}",
        "DATABASE=master",
        "TrustServerCertificate=yes",
    ]
    if args.trusted:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={args.user}")
        parts.append(f"PWD={args.password}")
    return ";".join(parts) + ";"


def connect(args: argparse.Namespace):
    cn = pyodbc.connect(build_connection_string(args), autocommit=False)
    cn.timeout = args.timeout
    return cn


def drain_cursor(cur) -> None:
    while True:
        try:
            more = cur.nextset()
        except pyodbc.ProgrammingError:
            break
        if not more:
            break


def exec_sql(cn, sql: str, params: Iterable[Any] | None = None) -> None:
    cur = cn.cursor()
    cur.execute(sql, tuple(params or ()))
    drain_cursor(cur)
    cur.close()


def fetch_one(cn, sql: str, params: Iterable[Any] | None = None):
    cur = cn.cursor()
    cur.execute(sql, tuple(params or ()))
    row = cur.fetchone()
    cur.close()
    return row


def scalar(cn, sql: str, params: Iterable[Any] | None = None, default: Any = None) -> Any:
    row = fetch_one(cn, sql, params)
    if row is None:
        return default
    return row[0]


def insert_row(
    cn,
    db: str,
    schema: str,
    table: str,
    columns: list[str],
    values: list[Any],
    output_identity: bool = True,
) -> int | None:
    col_sql = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    output_sql = "OUTPUT INSERTED.id " if output_identity else ""
    sql = f"INSERT INTO {db}.{schema}.{table} ({col_sql}) {output_sql}VALUES ({placeholders});"

    cur = cn.cursor()
    cur.execute(sql, values)
    inserted_id = None
    if output_identity:
        inserted_id = int(cur.fetchone()[0])
    cur.close()
    return inserted_id


def assert_equal(cn, label: str, sql: str, expected: Any, params: Iterable[Any] | None = None) -> None:
    actual = scalar(cn, sql, params)
    if actual != expected:
        raise AssertionError(f"[FAIL] {label}: expected {expected!r}, got {actual!r}")
    print(f"[PASS] {label}: {actual!r}")


def assert_zero(cn, label: str, sql: str, params: Iterable[Any] | None = None) -> None:
    assert_equal(cn, label, sql, 0, params)


def assert_ge(cn, label: str, sql: str, minimum: int, params: Iterable[Any] | None = None) -> None:
    actual = scalar(cn, sql, params)
    if actual is None or actual < minimum:
        raise AssertionError(f"[FAIL] {label}: expected >= {minimum!r}, got {actual!r}")
    print(f"[PASS] {label}: {actual!r} >= {minimum!r}")


def assert_true(cn, label: str, sql: str, params: Iterable[Any] | None = None) -> None:
    actual = scalar(cn, sql, params)
    if not actual:
        raise AssertionError(f"[FAIL] {label}: expected true/non-zero, got {actual!r}")
    print(f"[PASS] {label}: {actual!r}")


def date_key(day: dt.date) -> int:
    return int(day.strftime("%Y%m%d"))


def day_start(day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(0, 0, 0))


def day_end(day: dt.date) -> dt.datetime:
    return day_start(day + dt.timedelta(days=1))


def day_stamp(day: dt.date, hour: int = 8, minute: int = 0) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(hour, minute, 0))


# -----------------------------------------------------------------------------
# Object checks
# -----------------------------------------------------------------------------

def check_required_objects(cn) -> None:
    required = [
        f"{STG_DB}.etl_admin.usp_run_stg_program_ops_all",

        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_center",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_domain",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_score_scale",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_no_score_reason",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_assessment_status",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_child",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_teacher",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_task",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_tran_student_task_progress",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_child_task_event",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_daily_student_task_progress",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation",

        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_center",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_domain",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_score_scale",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_no_score_reason",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_assessment_status",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_child",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_teacher",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_task",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_child_task_event",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation",
    ]

    missing = []
    for obj in required:
        exists = scalar(cn, "SELECT CASE WHEN OBJECT_ID(?, 'P') IS NULL THEN 0 ELSE 1 END;", [obj])
        if exists != 1:
            missing.append(obj)

    if missing:
        print("\nMissing required procedures:")
        for item in missing:
            print("  -", item)
        raise RuntimeError("Install all staging and DW ETL procedures before running this test.")

    print("[PASS] Required stored procedures exist.")

    etl_work_exists = scalar(cn, f"SELECT CASE WHEN SCHEMA_ID(N'etl_work') IS NULL THEN 0 ELSE 1 END;", default=0)
    if etl_work_exists != 1:
        print("[WARN] Charity_DW_DB.etl_work schema was not found. If your reworked ETL uses etl_work tables, run 27_create_dw_etl_work_tables.sql first.")
    else:
        print("[PASS] etl_work schema exists.")


# -----------------------------------------------------------------------------
# Reset helpers
# -----------------------------------------------------------------------------

def reset_source(cn) -> None:
    print("\nResetting source tables...")
    tables = [
        "audit_logs",
        "note_batch_items",
        "note_batches",
        "notes",
        "task_assessments",
        "assessment_sessions",
        "daily_task_assignments",
        "child_task_plans",
        "child_daily_status",
        "center_daily_status",
        "task_templates",
        "users",
        "teachers",
        "children",
        "no_score_reasons",
        "absence_reasons",
        "closure_reasons",
        "score_scales",
        "domains",
        "centers",
    ]

    for table in tables:
        exec_sql(
            cn,
            f"""
            USE {SOURCE_DB};
            DELETE FROM program_ops.{table};
            IF OBJECTPROPERTY(OBJECT_ID(N'program_ops.{table}'), 'TableHasIdentity') = 1
                DBCC CHECKIDENT ('program_ops.{table}', RESEED, 0) WITH NO_INFOMSGS;
            """
        )

    cn.commit()
    print("[PASS] Source tables reset.")


def reset_staging(cn) -> None:
    print("\nResetting staging tables and logs...")
    tables = [
        "audit_logs",
        "note_batch_items",
        "note_batches",
        "notes",
        "task_assessments",
        "assessment_sessions",
        "daily_task_assignments",
        "child_task_plans",
        "child_daily_status",
        "center_daily_status",
        "task_templates",
        "no_score_reasons",
        "absence_reasons",
        "closure_reasons",
        "score_scales",
        "domains",
        "users",
        "teachers",
        "children",
        "centers",
    ]

    for table in tables:
        exec_sql(
            cn,
            f"""
            USE {STG_DB};
            DELETE FROM stg_program_ops.{table};
            IF OBJECTPROPERTY(OBJECT_ID(N'stg_program_ops.{table}'), 'TableHasIdentity') = 1
                DBCC CHECKIDENT ('stg_program_ops.{table}', RESEED, 0) WITH NO_INFOMSGS;
            """
        )

    exec_sql(
        cn,
        f"""
        USE {STG_DB};
        DELETE FROM etl_admin.etl_load_log;
        DELETE FROM etl_admin.etl_batch;
        DBCC CHECKIDENT ('etl_admin.etl_load_log', RESEED, 0) WITH NO_INFOMSGS;
        DBCC CHECKIDENT ('etl_admin.etl_batch', RESEED, 0) WITH NO_INFOMSGS;
        """
    )

    cn.commit()
    print("[PASS] Staging tables/logs reset.")


def reset_dw_except_dim_date(cn) -> None:
    print("\nResetting DW facts/dimensions/logs except dim_date...")

    fact_tables = [
        "fact_child_task_event",
        "fact_child_snapshot_accumulation",
        "fact_daily_student_task_progress",
        "fact_tran_student_task_progress",
    ]

    dim_tables = [
        "dim_no_score_reason",
        "dim_assessment_status",
        "dim_score_scale",
        "dim_task",
        "dim_domain",
        "dim_child",
        "dim_teacher",
        "dim_center",
    ]

    for table in fact_tables:
        exec_sql(
            cn,
            f"""
            USE {DW_DB};
            DELETE FROM dw.{table};
            IF OBJECTPROPERTY(OBJECT_ID(N'dw.{table}'), 'TableHasIdentity') = 1
                DBCC CHECKIDENT ('dw.{table}', RESEED, 0) WITH NO_INFOMSGS;
            """
        )

    for table in dim_tables:
        exec_sql(
            cn,
            f"""
            USE {DW_DB};
            DELETE FROM dw.{table};
            IF OBJECTPROPERTY(OBJECT_ID(N'dw.{table}'), 'TableHasIdentity') = 1
                DBCC CHECKIDENT ('dw.{table}', RESEED, 0) WITH NO_INFOMSGS;
            """
        )

    exec_sql(
        cn,
        f"""
        USE {DW_DB};
        DELETE FROM etl_admin.etl_load_log;
        DELETE FROM etl_admin.etl_batch;
        DBCC CHECKIDENT ('etl_admin.etl_load_log', RESEED, 0) WITH NO_INFOMSGS;
        DBCC CHECKIDENT ('etl_admin.etl_batch', RESEED, 0) WITH NO_INFOMSGS;
        """
    )

    # Truncate etl_work tables if the schema exists.
    work_schema_exists = scalar(cn, f"SELECT CASE WHEN SCHEMA_ID(N'etl_work') IS NULL THEN 0 ELSE 1 END;")
    if work_schema_exists == 1:
        rows = []
        cur = cn.cursor()
        cur.execute(
            f"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM {DW_DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = N'etl_work'
              AND TABLE_TYPE = N'BASE TABLE'
            ORDER BY TABLE_NAME;
            """
        )
        rows = cur.fetchall()
        cur.close()

        for row in rows:
            exec_sql(cn, f"USE {DW_DB}; TRUNCATE TABLE etl_work.{row.TABLE_NAME};")

    cn.commit()
    print("[PASS] DW reset completed. dim_date was preserved.")


def ensure_dim_date_rows(cn, start_date: dt.date, end_date: dt.date) -> None:
    print("\nEnsuring required dim_date rows exist...")

    exec_sql(
        cn,
        f"""
        USE {DW_DB};

        IF NOT EXISTS (SELECT 1 FROM dw.dim_date WHERE TimeKey = -1)
        BEGIN
            INSERT INTO dw.dim_date
                (TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey,
                 DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek,
                 DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear,
                 WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName,
                 MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter,
                 CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester)
            VALUES
                (-1, CONVERT(DATE, '19000101'), N'نامشخص',
                 0, 0, N'Unknown', N'نامشخص',
                 0, 0, 0, 0,
                 0, 0, N'Unknown', N'نامشخص',
                 0, 0, 0, 0,
                 1900, 0, 0, 0);
        END;
        """
    )

    current = start_date
    while current <= end_date:
        key = date_key(current)
        iso_week = int(current.strftime("%V"))
        quarter = ((current.month - 1) // 3) + 1
        semester = 1 if current.month <= 6 else 2

        exec_sql(
            cn,
            f"""
            USE {DW_DB};

            IF NOT EXISTS (SELECT 1 FROM dw.dim_date WHERE TimeKey = ?)
            BEGIN
                INSERT INTO dw.dim_date
                    (TimeKey, FullDateAlternateKey,
                     DayNumberOfWeek, EnglishDayNameOfWeek,
                     DayNumberOfMonth, DayNumberOfYear, WeekNumberOfYear,
                     EnglishMonthName, MonthNumberOfYear,
                     CalendarQuarter, CalendarYear, CalendarSemester)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            END;
            """,
            [
                key,
                key,
                current,
                current.isoweekday(),
                current.strftime("%A"),
                current.day,
                int(current.strftime("%j")),
                iso_week,
                current.strftime("%B"),
                current.month,
                quarter,
                current.year,
                semester,
            ],
        )

        current += dt.timedelta(days=1)

    cn.commit()
    print(f"[PASS] dim_date covers {start_date} through {end_date}.")


# -----------------------------------------------------------------------------
# Source seeding
# -----------------------------------------------------------------------------

def seed_day1_base_data(cn, state: TestState) -> None:
    print("\nSeeding Day 1 base non-transaction data...")
    stamp = day_stamp(BASE_DATE, 8)

    for name, city, address in [
        ("Alpha Autism Center", "Tehran", "Alpha Street 1"),
        ("Beta Rehab Center", "Isfahan", "Beta Street 2"),
        ("Gamma Learning Center", "Shiraz", "Gamma Street 3"),
    ]:
        state.center_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "centers",
                ["name", "city", "address", "is_active", "created_at", "updated_at"],
                [name, city, address, 1, stamp, None],
            )
        )

    children_seed = [
        (state.center_ids[0], "Ali", "Ahmadi", "NC1001", dt.date(2016, 3, 5), "male"),
        (state.center_ids[0], "Sara", "Karimi", "NC1002", dt.date(2017, 6, 12), "female"),
        (state.center_ids[1], "Reza", "Mohammadi", "NC1003", dt.date(2015, 9, 20), "male"),
        (state.center_ids[1], "Nika", "Rahimi", "NC1004", dt.date(2018, 2, 2), "female"),
        (state.center_ids[2], "Mina", "Hosseini", "NC1005", dt.date(2016, 11, 15), "female"),
        (state.center_ids[2], "Arman", "Jafari", "NC1006", dt.date(2017, 1, 19), "male"),
    ]

    for center_id, first_name, last_name, nc, birth, gender in children_seed:
        child_id = insert_row(
            cn, SOURCE_DB, "program_ops", "children",
            ["center_id", "first_name", "last_name", "national_code", "birth_date",
             "gender", "enrollment_date", "status", "created_at", "updated_at"],
            [center_id, first_name, last_name, nc, birth, gender,
             dt.date(2027, 9, 1), "active", stamp, None],
        )
        state.child_ids.append(child_id)
        state.child_plans[child_id] = []

    teachers_seed = [
        (state.center_ids[0], "Maryam", "Teacher", "09120000001", "maryam.teacher@example.com"),
        (state.center_ids[0], "Hamed", "Coach", "09120000002", "hamed.coach@example.com"),
        (state.center_ids[1], "Leila", "Trainer", "09120000003", "leila.trainer@example.com"),
        (state.center_ids[2], "Omid", "Mentor", "09120000004", "omid.mentor@example.com"),
    ]

    for idx, (center_id, first_name, last_name, phone, email) in enumerate(teachers_seed, start=1):
        teacher_id = insert_row(
            cn, SOURCE_DB, "program_ops", "teachers",
            ["center_id", "first_name", "last_name", "phone", "email",
             "employment_status", "is_active", "created_at", "updated_at"],
            [center_id, first_name, last_name, phone, email,
             "active", 1, stamp, None],
        )
        state.teacher_ids.append(teacher_id)

        user_id = insert_row(
            cn, SOURCE_DB, "program_ops", "users",
            ["username", "password_hash", "role", "teacher_id", "is_active", "created_at", "updated_at"],
            [f"teacher_user_{idx}", "not-real-hash", "teacher", teacher_id, 1, stamp, None],
        )
        state.user_ids.append(user_id)
        state.teacher_user_by_teacher[teacher_id] = user_id

    for name, desc in [
        ("Communication", "Speech and communication tasks"),
        ("Motor Skills", "Fine and gross motor tasks"),
        ("Cognitive", "Attention and cognitive tasks"),
    ]:
        state.domain_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "domains",
                ["name", "description", "is_active", "created_at", "updated_at"],
                [name, desc, 1, stamp, None],
            )
        )

    for name, min_score, max_score, desc in [
        ("Five Point Scale", 0, 5, "0 to 5 scoring"),
        ("Ten Point Scale", 0, 10, "0 to 10 scoring"),
        ("Percent Scale", 0, 100, "0 to 100 scoring"),
    ]:
        state.score_scale_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "score_scales",
                ["name", "min_score", "max_score", "description", "is_active", "created_at", "updated_at"],
                [name, min_score, max_score, desc, 1, stamp, None],
            )
        )

    for title, desc in [
        ("Child absent", "The child was absent"),
        ("Child refused", "The child refused the activity"),
        ("Center closed", "The center was closed"),
        ("System issue", "System or recording issue"),
    ]:
        state.no_score_reason_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "no_score_reasons",
                ["title", "description", "is_active", "created_at", "updated_at"],
                [title, desc, 1, stamp, None],
            )
        )

    state.absence_reason_ids.append(
        insert_row(
            cn, SOURCE_DB, "program_ops", "absence_reasons",
            ["title", "description", "is_active", "created_at", "updated_at"],
            ["Illness", "Child illness", 1, stamp, None],
        )
    )

    state.closure_reason_ids.append(
        insert_row(
            cn, SOURCE_DB, "program_ops", "closure_reasons",
            ["title", "description", "is_active", "created_at", "updated_at"],
            ["Holiday", "Center holiday", 1, stamp, None],
        )
    )

    templates = [
        (state.domain_ids[0], "Eye Contact Practice", "Practice short eye contact", state.score_scale_ids[0]),
        (state.domain_ids[0], "Request With Words", "Request an object verbally", state.score_scale_ids[1]),
        (state.domain_ids[1], "Fine Motor Blocks", "Stack small blocks", state.score_scale_ids[0]),
        (state.domain_ids[1], "Balance Walk", "Walk on a line", state.score_scale_ids[1]),
        (state.domain_ids[2], "Color Matching", "Match colors", state.score_scale_ids[2]),
        (state.domain_ids[2], "Attention Game", "Sustain attention", state.score_scale_ids[2]),
    ]

    for domain_id, title, desc, scale_id in templates:
        state.task_template_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "task_templates",
                ["domain_id", "title", "description", "default_score_scale_id",
                 "is_active", "created_by", "created_at", "updated_at"],
                [domain_id, title, desc, scale_id, 1, state.user_ids[0], stamp, None],
            )
        )

    for child_idx, child_id in enumerate(state.child_ids):
        for offset in [0, 1]:
            template_id = state.task_template_ids[(child_idx + offset) % len(state.task_template_ids)]
            template = fetch_one(
                cn,
                f"""
                SELECT domain_id, title, default_score_scale_id
                FROM {SOURCE_DB}.program_ops.task_templates
                WHERE id = ?;
                """,
                [template_id],
            )
            plan_id = insert_row(
                cn, SOURCE_DB, "program_ops", "child_task_plans",
                ["child_id", "task_template_id", "domain_id", "task_title", "score_scale_id",
                 "start_date", "end_date", "is_active", "created_by", "created_at", "updated_at"],
                [child_id, template_id, template.domain_id, template.title, template.default_score_scale_id,
                 BASE_DATE, None, 1, state.user_ids[0], stamp, None],
            )
            state.child_plans[child_id].append(plan_id)

    state.expected_non_transaction_changes[BASE_DATE] = (
        len(state.center_ids)
        + len(state.child_ids)
        + len(state.teacher_ids)
        + len(state.user_ids)
        + len(state.domain_ids)
        + len(state.score_scale_ids)
        + len(state.no_score_reason_ids)
        + len(state.task_template_ids)
        + sum(len(v) for v in state.child_plans.values())
        + len(state.absence_reason_ids)
        + len(state.closure_reason_ids)
    )

    cn.commit()
    print(f"[PASS] Day 1 base non-transaction records affected: {state.expected_non_transaction_changes[BASE_DATE]}")


def pick_teacher_for_center(cn, state: TestState, center_id: int) -> tuple[int, int]:
    for teacher_id in state.teacher_ids:
        row_center = scalar(cn, f"SELECT center_id FROM {SOURCE_DB}.program_ops.teachers WHERE id = ?;", [teacher_id])
        if row_center == center_id:
            return teacher_id, state.teacher_user_by_teacher[teacher_id]

    # fallback
    teacher_id = state.teacher_ids[0]
    return teacher_id, state.teacher_user_by_teacher[teacher_id]


def apply_non_transaction_changes_for_day(cn, state: TestState, business_day: dt.date) -> int:
    """
    Creates or updates at least 10 non-transaction source records.
    It intentionally does not update daily_task_assignments or task_assessments.
    """
    stamp = day_stamp(business_day, 8)
    n = business_day.day
    affected = 0

    # 1 update center
    center_id = state.center_ids[(n - 1) % len(state.center_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.centers
        SET city = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"City_Updated_D{n}", stamp, center_id],
    )
    affected += 1

    # 2 update teacher
    teacher_id = state.teacher_ids[(n - 1) % len(state.teacher_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.teachers
        SET employment_status = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"active_d{n}", stamp, teacher_id],
    )
    affected += 1

    # 3 update child
    child_id = state.child_ids[(n - 1) % len(state.child_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.children
        SET status = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"active_d{n}", stamp, child_id],
    )
    affected += 1

    # 4 update domain
    domain_id = state.domain_ids[(n - 1) % len(state.domain_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.domains
        SET description = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"Domain updated on day {n}", stamp, domain_id],
    )
    affected += 1

    # 5 update score scale
    scale_id = state.score_scale_ids[(n - 1) % len(state.score_scale_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.score_scales
        SET description = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"Scale updated on day {n}", stamp, scale_id],
    )
    affected += 1

    # 6 update no score reason
    reason_id = state.no_score_reason_ids[(n - 1) % len(state.no_score_reason_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.no_score_reasons
        SET description = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"Reason updated on day {n}", stamp, reason_id],
    )
    affected += 1

    # 7 update task template
    template_id = state.task_template_ids[(n - 1) % len(state.task_template_ids)]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.task_templates
        SET description = ?, updated_at = ?
        WHERE id = ?;
        """,
        [f"Template updated on day {n}", stamp, template_id],
    )
    affected += 1

    # 8 update child task plan
    plan_child_id = state.child_ids[(n + 1) % len(state.child_ids)]
    plan_id = state.child_plans[plan_child_id][0]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.child_task_plans
        SET task_title = task_title, updated_at = ?
        WHERE id = ?;
        """,
        [stamp, plan_id],
    )
    affected += 1

    # 9 insert new child
    new_child_id = insert_row(
        cn, SOURCE_DB, "program_ops", "children",
        ["center_id", "first_name", "last_name", "national_code", "birth_date",
         "gender", "enrollment_date", "status", "created_at", "updated_at"],
        [
            state.center_ids[n % len(state.center_ids)],
            f"NewChild{n}",
            "MultiDay",
            f"NCMD{n:04d}",
            dt.date(2017, min(12, n), min(28, n)),
            "female" if n % 2 == 0 else "male",
            business_day,
            "active",
            stamp,
            None,
        ],
    )
    state.child_ids.append(new_child_id)
    state.child_plans[new_child_id] = []
    affected += 1

    # 10 insert new child task plan for new child
    template_id = state.task_template_ids[n % len(state.task_template_ids)]
    template = fetch_one(
        cn,
        f"""
        SELECT domain_id, title, default_score_scale_id
        FROM {SOURCE_DB}.program_ops.task_templates
        WHERE id = ?;
        """,
        [template_id],
    )
    new_plan_id = insert_row(
        cn, SOURCE_DB, "program_ops", "child_task_plans",
        ["child_id", "task_template_id", "domain_id", "task_title", "score_scale_id",
         "start_date", "end_date", "is_active", "created_by", "created_at", "updated_at"],
        [
            new_child_id,
            template_id,
            template.domain_id,
            template.title,
            template.default_score_scale_id,
            business_day,
            None,
            1,
            state.user_ids[0],
            stamp,
            None,
        ],
    )
    state.child_plans[new_child_id].append(new_plan_id)
    affected += 1

    # 11 insert new teacher
    new_teacher_id = insert_row(
        cn, SOURCE_DB, "program_ops", "teachers",
        ["center_id", "first_name", "last_name", "phone", "email",
         "employment_status", "is_active", "created_at", "updated_at"],
        [
            state.center_ids[n % len(state.center_ids)],
            f"Teacher{n}",
            "MultiDay",
            f"09129999{n:04d}",
            f"teacher{n}.multiday@example.com",
            "active",
            1,
            stamp,
            None,
        ],
    )
    state.teacher_ids.append(new_teacher_id)
    affected += 1

    # 12 insert new user for new teacher
    new_user_id = insert_row(
        cn, SOURCE_DB, "program_ops", "users",
        ["username", "password_hash", "role", "teacher_id", "is_active", "created_at", "updated_at"],
        [f"teacher_multiday_{n}", "not-real-hash", "teacher", new_teacher_id, 1, stamp, None],
    )
    state.user_ids.append(new_user_id)
    state.teacher_user_by_teacher[new_teacher_id] = new_user_id
    affected += 1

    # 13 insert a new no-score reason
    new_reason_id = insert_row(
        cn, SOURCE_DB, "program_ops", "no_score_reasons",
        ["title", "description", "is_active", "created_at", "updated_at"],
        [f"Extra reason day {n}", f"Extra reason created on day {n}", 1, stamp, None],
    )
    state.no_score_reason_ids.append(new_reason_id)
    affected += 1

    # 14 insert child daily status, not a transaction fact row but affects transaction enrichment.
    insert_row(
        cn, SOURCE_DB, "program_ops", "child_daily_status",
        ["child_id", "date", "status", "absence_reason_id", "note", "created_by", "created_at", "updated_at"],
        [
            state.child_ids[n % len(state.child_ids)],
            business_day,
            "present" if n % 2 else "absent",
            state.absence_reason_ids[0],
            f"Daily status for day {n}",
            state.user_ids[0],
            stamp,
            None,
        ],
    )
    affected += 1

    # 15 insert center daily status.
    insert_row(
        cn, SOURCE_DB, "program_ops", "center_daily_status",
        ["center_id", "date", "status", "closure_reason_id", "note", "created_by", "created_at", "updated_at"],
        [
            state.center_ids[n % len(state.center_ids)],
            business_day,
            "open",
            state.closure_reason_ids[0],
            f"Center status for day {n}",
            state.user_ids[0],
            stamp,
            None,
        ],
    )
    affected += 1

    state.expected_non_transaction_changes[business_day] = affected
    if affected < 10:
        raise AssertionError(f"Day {business_day}: expected at least 10 non-transaction source changes, got {affected}")

    cn.commit()
    print(f"[PASS] {business_day}: non-transaction source records affected = {affected}")
    return affected


def add_transactions_for_day(
    cn,
    state: TestState,
    business_day: dt.date,
    assignment_count: int,
    assessment_count: int,
) -> None:
    """
    Append-only transaction creation for one business day.
    This never updates previous transaction rows.
    """
    stamp = day_stamp(business_day, 10)
    state.assignment_ids_by_day.setdefault(business_day, [])
    state.assessment_ids_by_day.setdefault(business_day, [])

    created_assignments: list[tuple[int, int, int, int, int]] = []
    # tuple: assignment_id, child_id, teacher_id, center_id, score_scale_id

    for i in range(assignment_count):
        child_id = state.child_ids[(business_day.day + i) % len(state.child_ids)]
        child_center_id = scalar(cn, f"SELECT center_id FROM {SOURCE_DB}.program_ops.children WHERE id = ?;", [child_id])
        teacher_id, planned_by_user_id = pick_teacher_for_center(cn, state, child_center_id)

        if not state.child_plans.get(child_id):
            # If a child somehow has no plan, create one for safety.
            template_id = state.task_template_ids[(business_day.day + i) % len(state.task_template_ids)]
            template = fetch_one(
                cn,
                f"""
                SELECT domain_id, title, default_score_scale_id
                FROM {SOURCE_DB}.program_ops.task_templates
                WHERE id = ?;
                """,
                [template_id],
            )
            plan_id = insert_row(
                cn, SOURCE_DB, "program_ops", "child_task_plans",
                ["child_id", "task_template_id", "domain_id", "task_title", "score_scale_id",
                 "start_date", "end_date", "is_active", "created_by", "created_at", "updated_at"],
                [child_id, template_id, template.domain_id, template.title, template.default_score_scale_id,
                 business_day, None, 1, planned_by_user_id, stamp, None],
            )
            state.child_plans.setdefault(child_id, []).append(plan_id)

        plan_id = state.child_plans[child_id][(business_day.day + i) % len(state.child_plans[child_id])]
        plan = fetch_one(
            cn,
            f"""
            SELECT task_template_id, domain_id, task_title, score_scale_id
            FROM {SOURCE_DB}.program_ops.child_task_plans
            WHERE id = ?;
            """,
            [plan_id],
        )

        status = "completed" if i % 3 == 0 else "planned"

        assignment_id = insert_row(
            cn, SOURCE_DB, "program_ops", "daily_task_assignments",
            ["child_id", "date", "child_task_plan_id", "task_template_id", "domain_id",
             "task_title", "score_scale_id", "planned_by", "status", "created_at", "updated_at"],
            [
                child_id,
                business_day,
                plan_id,
                plan.task_template_id,
                plan.domain_id,
                plan.task_title,
                plan.score_scale_id,
                planned_by_user_id,
                status,
                stamp,
                None,
            ],
        )

        state.assignment_ids_by_day[business_day].append(assignment_id)
        created_assignments.append((assignment_id, child_id, teacher_id, child_center_id, plan.score_scale_id))

    for i, (assignment_id, child_id, teacher_id, center_id, score_scale_id) in enumerate(created_assignments[:assessment_count]):
        session_id = insert_row(
            cn, SOURCE_DB, "program_ops", "assessment_sessions",
            ["child_id", "teacher_id", "center_id", "date", "started_at", "ended_at",
             "session_status", "general_note", "created_at", "updated_at"],
            [
                child_id,
                teacher_id,
                center_id,
                business_day,
                dt.datetime.combine(business_day, dt.time(11, 0)),
                dt.datetime.combine(business_day, dt.time(11, 25)),
                "closed",
                f"Session for {business_day}",
                stamp,
                None,
            ],
        )

        if i % 5 == 0:
            assessment_status = "not_scored"
            score = None
            no_score_reason_id = state.no_score_reason_ids[0]
        elif i % 7 == 0:
            assessment_status = "refused"
            score = None
            no_score_reason_id = state.no_score_reason_ids[1]
        else:
            assessment_status = "scored"
            max_score = scalar(cn, f"SELECT max_score FROM {SOURCE_DB}.program_ops.score_scales WHERE id = ?;", [score_scale_id])
            score = float(min(max_score, 1 + ((business_day.day + i) % max(2, int(max_score)))))
            no_score_reason_id = None

        assessment_id = insert_row(
            cn, SOURCE_DB, "program_ops", "task_assessments",
            ["daily_task_assignment_id", "assessment_session_id", "child_id", "teacher_id",
             "date", "score", "normalized_score", "assessment_status", "no_score_reason_id",
             "attempt_no", "note", "created_at", "updated_at"],
            [
                assignment_id,
                session_id,
                child_id,
                teacher_id,
                business_day,
                score,
                None,
                assessment_status,
                no_score_reason_id,
                1,
                f"Assessment for {business_day}",
                stamp,
                None,
            ],
        )
        state.assessment_ids_by_day[business_day].append(assessment_id)

    state.expected_assignments += assignment_count
    state.expected_assessments += assessment_count
    state.expected_event_rows = state.expected_assignments + state.expected_assessments

    cn.commit()
    print(f"[PASS] {business_day}: appended transactions assignments={assignment_count}, assessments={assessment_count}")


# -----------------------------------------------------------------------------
# ETL execution
# -----------------------------------------------------------------------------

DW_FIRST_LOAD_PROCS = [
    "etl_admin.usp_first_load_dw_dim_center",
    "etl_admin.usp_first_load_dw_dim_domain",
    "etl_admin.usp_first_load_dw_dim_score_scale",
    "etl_admin.usp_first_load_dw_dim_no_score_reason",
    "etl_admin.usp_first_load_dw_dim_assessment_status",
    "etl_admin.usp_first_load_dw_dim_child",
    "etl_admin.usp_first_load_dw_dim_teacher",
    "etl_admin.usp_first_load_dw_dim_task",
    "etl_admin.usp_first_load_dw_fact_tran_student_task_progress",
    "etl_admin.usp_first_load_dw_fact_child_task_event",
    "etl_admin.usp_first_load_dw_fact_daily_student_task_progress",
    "etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation",
]

DW_INCREMENTAL_PROCS = [
    "etl_admin.usp_incremental_load_dw_dim_center",
    "etl_admin.usp_incremental_load_dw_dim_domain",
    "etl_admin.usp_incremental_load_dw_dim_score_scale",
    "etl_admin.usp_incremental_load_dw_dim_no_score_reason",
    "etl_admin.usp_incremental_load_dw_dim_assessment_status",
    "etl_admin.usp_incremental_load_dw_dim_child",
    "etl_admin.usp_incremental_load_dw_dim_teacher",
    "etl_admin.usp_incremental_load_dw_dim_task",
    "etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress",
    "etl_admin.usp_incremental_load_dw_fact_child_task_event",
    "etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress",
    "etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation",
]


def run_staging_etl(cn, to_date: dt.datetime) -> None:
    print(f"\nRunning Source -> Staging ETL to_date={to_date.isoformat()}...")
    exec_sql(cn, f"EXEC {STG_DB}.etl_admin.usp_run_stg_program_ops_all @to_date = ?;", [to_date])
    cn.commit()

    status = scalar(
        cn,
        f"""
        SELECT TOP (1) batch_status
        FROM {STG_DB}.etl_admin.etl_batch
        ORDER BY etl_batch_id DESC;
        """
    )
    if status != "succeeded":
        raise RuntimeError(f"Staging ETL failed or did not succeed. Last status={status!r}")

    print("[PASS] Source -> Staging ETL succeeded.")


def run_dw_procedure_list(cn, procs: list[str], start_time: dt.datetime, end_time: dt.datetime) -> None:
    for proc in procs:
        print(f"  EXEC {DW_DB}.{proc}")
        exec_sql(cn, f"EXEC {DW_DB}.{proc} @start_time = ?, @end_time = ?;", [start_time, end_time])
        cn.commit()

        status = scalar(
            cn,
            f"""
            SELECT TOP (1) batch_status
            FROM {DW_DB}.etl_admin.etl_batch
            ORDER BY etl_batch_id DESC;
            """
        )
        if status != "succeeded":
            raise RuntimeError(f"DW procedure {proc} failed or did not succeed. Last status={status!r}")


def run_dw_first_load(cn, start_time: dt.datetime, end_time: dt.datetime) -> None:
    print(f"\nRunning DW FIRST LOAD {start_time.isoformat()} <= source_time < {end_time.isoformat()}...")
    run_dw_procedure_list(cn, DW_FIRST_LOAD_PROCS, start_time, end_time)
    print("[PASS] DW first-load ETL succeeded.")


def run_dw_incremental(cn, start_time: dt.datetime, end_time: dt.datetime) -> None:
    print(f"\nRunning DW INCREMENTAL {start_time.isoformat()} <= source_time < {end_time.isoformat()}...")
    run_dw_procedure_list(cn, DW_INCREMENTAL_PROCS, start_time, end_time)
    print("[PASS] DW incremental ETL succeeded.")


# -----------------------------------------------------------------------------
# Validations
# -----------------------------------------------------------------------------

def validate_no_previous_transaction_updates(cn, current_start: dt.datetime, window_start_date: dt.date) -> None:
    """
    User rule:
        The user is not allowed to change transactions of previous days.

    This test checks Source transaction tables and fails if previous-day
    transaction rows were updated in the current load window.
    """
    assert_zero(
        cn,
        f"no previous daily_task_assignments updated at/after {current_start.date()}",
        f"""
        SELECT COUNT(*)
        FROM {SOURCE_DB}.program_ops.daily_task_assignments
        WHERE [date] < ?
          AND updated_at >= ?;
        """,
        [window_start_date, current_start],
    )

    assert_zero(
        cn,
        f"no previous task_assessments updated at/after {current_start.date()}",
        f"""
        SELECT COUNT(*)
        FROM {SOURCE_DB}.program_ops.task_assessments
        WHERE [date] < ?
          AND updated_at >= ?;
        """,
        [window_start_date, current_start],
    )


def row_signature(cn, sql: str, params: Iterable[Any] | None = None) -> tuple[Any, ...]:
    row = fetch_one(cn, sql, params)
    if row is None:
        return tuple()
    return tuple(row)


def assert_signature_stable(store: dict[int, tuple[Any, ...]], key: int, new_signature: tuple[Any, ...], label: str) -> None:
    old_signature = store.get(key)
    if old_signature is None:
        store[key] = new_signature
        print(f"[PASS] stored baseline signature for {label}: {new_signature}")
        return

    if old_signature != new_signature:
        raise AssertionError(
            f"[FAIL] append-only/stability check failed for {label}: "
            f"old signature={old_signature!r}, new signature={new_signature!r}"
        )

    print(f"[PASS] unchanged signature for {label}: {new_signature}")


def transaction_signature_for_date(cn, d: dt.date) -> tuple[Any, ...]:
    return row_signature(
        cn,
        f"""
        SELECT
            COUNT_BIG(*) AS row_count,
            SUM(CONVERT(BIGINT, ISNULL(source_daily_task_assignment_id, 0))) AS sum_assignment_id,
            SUM(CONVERT(BIGINT, ISNULL(source_task_assessment_id, 0))) AS sum_assessment_id,
            SUM(CONVERT(BIGINT, ISNULL(is_scored, 0))) AS sum_is_scored,
            SUM(CONVERT(BIGINT, ISNULL(is_not_scored, 0))) AS sum_is_not_scored,
            SUM(CONVERT(BIGINT, ISNULL(is_completed, 0))) AS sum_is_completed,
            SUM(CONVERT(BIGINT, ISNULL(is_assessed, 0))) AS sum_is_assessed,
            SUM(CONVERT(BIGINT, CAST(ISNULL(raw_score, 0) * 100 AS DECIMAL(18,0)))) AS sum_raw_score_x100,
            SUM(CONVERT(BIGINT, CAST(ISNULL(normalized_score, 0) * 10000 AS DECIMAL(18,0)))) AS sum_normalized_score_x10000
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE date_key = ?;
        """,
        [date_key(d)],
    )


def event_signature_for_date(cn, d: dt.date) -> tuple[Any, ...]:
    return row_signature(
        cn,
        f"""
        SELECT
            COUNT_BIG(*) AS row_count,
            SUM(CASE WHEN event_type = N'PLAN' THEN 1 ELSE 0 END) AS plan_events,
            SUM(CASE WHEN event_type = N'ASSESSMENT' THEN 1 ELSE 0 END) AS assessment_events,
            SUM(CONVERT(BIGINT, ISNULL(source_daily_task_assignment_id, 0))) AS sum_assignment_id,
            SUM(CONVERT(BIGINT, ISNULL(source_task_assessment_id, 0))) AS sum_assessment_id,
            SUM(CONVERT(BIGINT, CAST(ISNULL(raw_score, 0) * 100 AS DECIMAL(18,0)))) AS sum_raw_score_x100,
            SUM(CONVERT(BIGINT, CAST(ISNULL(normalized_score, 0) * 10000 AS DECIMAL(18,0)))) AS sum_normalized_score_x10000
        FROM {DW_DB}.dw.fact_child_task_event
        WHERE date_key = ?;
        """,
        [date_key(d)],
    )


def snapshot_signature_for_date(cn, d: dt.date) -> tuple[Any, ...]:
    return row_signature(
        cn,
        f"""
        SELECT
            COUNT_BIG(*) AS row_count,
            SUM(CONVERT(BIGINT, ISNULL(planned_task_count, 0))) AS sum_planned,
            SUM(CONVERT(BIGINT, ISNULL(assessment_count, 0))) AS sum_assessment,
            SUM(CONVERT(BIGINT, ISNULL(completed_task_count, 0))) AS sum_completed,
            SUM(CONVERT(BIGINT, ISNULL(scored_task_count, 0))) AS sum_scored,
            SUM(CONVERT(BIGINT, ISNULL(not_scored_task_count, 0))) AS sum_not_scored,
            SUM(CONVERT(BIGINT, CAST(ISNULL(raw_score, 0) * 100 AS DECIMAL(18,0)))) AS sum_raw_score_x100,
            SUM(CONVERT(BIGINT, CAST(ISNULL(normalized_score, 0) * 10000 AS DECIMAL(18,0)))) AS sum_normalized_score_x10000
        FROM {DW_DB}.dw.fact_daily_student_task_progress
        WHERE date_key = ?;
        """,
        [date_key(d)],
    )


def validate_source_to_transaction_fact_by_day(cn, checked_days: list[dt.date]) -> None:
    """
    Validates exact per-day source-to-transaction-fact correctness:
        - assignment count
        - assessment count
        - raw_score sum
        - scored/not_scored counts
        - no missing source rows in DW fact
    """
    for d in checked_days:
        dk = date_key(d)

        assert_equal(
            cn,
            f"{d} source assignments == fact planned rows",
            f"""
            SELECT
                (SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.daily_task_assignments WHERE [date] = ?)
                -
                (SELECT COUNT(*)
                 FROM {DW_DB}.dw.fact_tran_student_task_progress
                 WHERE date_key = ?
                   AND source_daily_task_assignment_id IS NOT NULL
                   AND source_task_assessment_id IS NULL);
            """,
            0,
            [d, dk],
        )

        assert_equal(
            cn,
            f"{d} source assessments == fact assessment rows",
            f"""
            SELECT
                (SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.task_assessments WHERE [date] = ?)
                -
                (SELECT COUNT(*)
                 FROM {DW_DB}.dw.fact_tran_student_task_progress
                 WHERE date_key = ?
                   AND source_task_assessment_id IS NOT NULL);
            """,
            0,
            [d, dk],
        )

        assert_equal(
            cn,
            f"{d} source raw_score sum == fact raw_score sum",
            f"""
            SELECT
                CAST(ISNULL((SELECT SUM(CAST(score AS DECIMAL(18,2)))
                             FROM {SOURCE_DB}.program_ops.task_assessments
                             WHERE [date] = ?), 0) AS DECIMAL(18,2))
                -
                CAST(ISNULL((SELECT SUM(CAST(raw_score AS DECIMAL(18,2)))
                             FROM {DW_DB}.dw.fact_tran_student_task_progress
                             WHERE date_key = ?
                               AND source_task_assessment_id IS NOT NULL), 0) AS DECIMAL(18,2));
            """,
            0,
            [d, dk],
        )

        assert_equal(
            cn,
            f"{d} scored assessment count matches source score-not-null count",
            f"""
            SELECT
                (SELECT COUNT(*)
                 FROM {SOURCE_DB}.program_ops.task_assessments
                 WHERE [date] = ?
                   AND score IS NOT NULL)
                -
                (SELECT COUNT(*)
                 FROM {DW_DB}.dw.fact_tran_student_task_progress
                 WHERE date_key = ?
                   AND source_task_assessment_id IS NOT NULL
                   AND is_scored = 1);
            """,
            0,
            [d, dk],
        )

        assert_equal(
            cn,
            f"{d} not_scored assessment count matches source status",
            f"""
            SELECT
                (SELECT COUNT(*)
                 FROM {SOURCE_DB}.program_ops.task_assessments
                 WHERE [date] = ?
                   AND assessment_status = N'not_scored')
                -
                (SELECT COUNT(*)
                 FROM {DW_DB}.dw.fact_tran_student_task_progress
                 WHERE date_key = ?
                   AND source_task_assessment_id IS NOT NULL
                   AND is_not_scored = 1);
            """,
            0,
            [d, dk],
        )

    assert_zero(
        cn,
        "no source assignment is missing from transaction fact",
        f"""
        SELECT COUNT(*)
        FROM {SOURCE_DB}.program_ops.daily_task_assignments AS s
        LEFT JOIN {DW_DB}.dw.fact_tran_student_task_progress AS f
            ON f.source_daily_task_assignment_id = s.id
           AND f.source_task_assessment_id IS NULL
        WHERE f.student_task_progress_key IS NULL;
        """,
    )

    assert_zero(
        cn,
        "no source assessment is missing from transaction fact",
        f"""
        SELECT COUNT(*)
        FROM {SOURCE_DB}.program_ops.task_assessments AS s
        LEFT JOIN {DW_DB}.dw.fact_tran_student_task_progress AS f
            ON f.source_task_assessment_id = s.id
        WHERE f.student_task_progress_key IS NULL;
        """,
    )


def validate_fact_flag_consistency(cn) -> None:
    """
    Important bug-catching tests for fact_tran flags.
    """
    assert_zero(
        cn,
        "no transaction fact row has both is_scored and is_not_scored",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE is_scored = 1
          AND is_not_scored = 1;
        """,
    )

    assert_zero(
        cn,
        "raw_score is not NULL only when is_scored = 1",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE raw_score IS NOT NULL
          AND ISNULL(is_scored, 0) <> 1;
        """,
    )

    assert_zero(
        cn,
        "is_scored rows have raw_score",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE is_scored = 1
          AND raw_score IS NULL;
        """,
    )

    assert_zero(
        cn,
        "is_not_scored rows do not have raw_score",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE is_not_scored = 1
          AND raw_score IS NOT NULL;
        """,
    )

    assert_zero(
        cn,
        "assessment rows are marked is_assessed = 1",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NOT NULL
          AND ISNULL(is_assessed, 0) <> 1;
        """,
    )

    assert_zero(
        cn,
        "planned-only rows are not marked assessed",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NULL
          AND source_daily_task_assignment_id IS NOT NULL
          AND ISNULL(is_assessed, 0) <> 0;
        """,
    )

    assert_zero(
        cn,
        "assessment rows are classified as scored/not_scored/refused/cancelled/incomplete when assessed",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NOT NULL
          AND ISNULL(is_assessed, 0) = 1
          AND ISNULL(is_scored, 0) = 0
          AND ISNULL(is_not_scored, 0) = 0
          AND ISNULL(is_refused, 0) = 0
          AND ISNULL(is_cancelled, 0) = 0
          AND ISNULL(is_incomplete, 0) = 0;
        """,
    )


def validate_event_fact_correctness(cn, checked_days: list[dt.date]) -> None:
    for d in checked_days:
        dk = date_key(d)

        assert_equal(
            cn,
            f"{d} PLAN event count equals assignment count",
            f"""
            SELECT
                (SELECT COUNT(*)
                 FROM {SOURCE_DB}.program_ops.daily_task_assignments
                 WHERE [date] = ?)
                -
                (SELECT COUNT(*)
                 FROM {DW_DB}.dw.fact_child_task_event
                 WHERE date_key = ?
                   AND event_type = N'PLAN');
            """,
            0,
            [d, dk],
        )

        assert_equal(
            cn,
            f"{d} ASSESSMENT event count equals assessment count",
            f"""
            SELECT
                (SELECT COUNT(*)
                 FROM {SOURCE_DB}.program_ops.task_assessments
                 WHERE [date] = ?)
                -
                (SELECT COUNT(*)
                 FROM {DW_DB}.dw.fact_child_task_event
                 WHERE date_key = ?
                   AND event_type = N'ASSESSMENT');
            """,
            0,
            [d, dk],
        )

        assert_equal(
            cn,
            f"{d} event assessment raw_score sum equals transaction fact raw_score sum",
            f"""
            SELECT
                CAST(ISNULL((SELECT SUM(CAST(raw_score AS DECIMAL(18,2)))
                             FROM {DW_DB}.dw.fact_child_task_event
                             WHERE date_key = ?
                               AND event_type = N'ASSESSMENT'), 0) AS DECIMAL(18,2))
                -
                CAST(ISNULL((SELECT SUM(CAST(raw_score AS DECIMAL(18,2)))
                             FROM {DW_DB}.dw.fact_tran_student_task_progress
                             WHERE date_key = ?
                               AND source_task_assessment_id IS NOT NULL), 0) AS DECIMAL(18,2));
            """,
            0,
            [dk, dk],
        )


def validate_daily_snapshot_against_transaction_fact(cn, checked_days: list[dt.date]) -> None:
    """
    Validates every daily snapshot row against an expected as-of aggregate
    recalculated directly from fact_tran_student_task_progress.

    This checks the sums/counts, not only row existence.
    """
    for d in checked_days:
        dk = date_key(d)

        assert_zero(
            cn,
            f"{d} daily snapshot matches transaction fact as-of aggregate",
            f"""
            WITH expected AS
            (
                SELECT
                    ? AS date_key,
                    COALESCE(ft.child_key, -1) AS child_key,
                    COALESCE(ft.center_key, -1) AS center_key,
                    COALESCE(ft.teacher_key, -1) AS teacher_key,

                    CAST(AVG(CASE
                                 WHEN ft.is_scored = 1 AND ft.raw_score IS NOT NULL
                                 THEN ft.raw_score
                             END) AS DECIMAL(10,2)) AS raw_score,

                    CAST(MIN(CASE
                                 WHEN ft.is_scored = 1
                                 THEN dss.min_score
                             END) AS DECIMAL(10,2)) AS min_score,

                    CAST(MAX(CASE
                                 WHEN ft.is_scored = 1
                                 THEN dss.max_score
                             END) AS DECIMAL(10,2)) AS max_score,

                    CAST(AVG(CASE
                                 WHEN ft.is_scored = 1 AND ft.normalized_score IS NOT NULL
                                 THEN ft.normalized_score
                             END) AS DECIMAL(10,4)) AS normalized_score,

                    COUNT(DISTINCT CASE
                                       WHEN ft.source_daily_task_assignment_id IS NOT NULL
                                       THEN ft.source_daily_task_assignment_id
                                   END) AS planned_task_count,

                    COUNT(DISTINCT CASE
                                       WHEN ft.source_task_assessment_id IS NOT NULL
                                       THEN ft.source_task_assessment_id
                                   END) AS assessment_count,

                    COUNT(DISTINCT CASE
                                       WHEN ft.is_completed = 1
                                            AND ft.source_daily_task_assignment_id IS NOT NULL
                                       THEN ft.source_daily_task_assignment_id
                                   END) AS completed_task_count,

                    COUNT(DISTINCT CASE
                                       WHEN ft.is_scored = 1
                                            AND ft.source_daily_task_assignment_id IS NOT NULL
                                       THEN ft.source_daily_task_assignment_id
                                   END) AS scored_task_count,

                    COUNT(DISTINCT CASE
                                       WHEN ft.is_not_scored = 1
                                            AND ft.source_daily_task_assignment_id IS NOT NULL
                                       THEN ft.source_daily_task_assignment_id
                                   END) AS not_scored_task_count
                FROM {DW_DB}.dw.fact_tran_student_task_progress AS ft
                INNER JOIN {DW_DB}.dw.dim_date AS tx_date
                    ON tx_date.TimeKey = ft.date_key
                LEFT JOIN {DW_DB}.dw.dim_score_scale AS dss
                    ON dss.score_scale_key = ft.score_scale_key
                WHERE tx_date.FullDateAlternateKey <= ?
                GROUP BY
                    COALESCE(ft.child_key, -1),
                    COALESCE(ft.center_key, -1),
                    COALESCE(ft.teacher_key, -1)
            ),
            actual AS
            (
                SELECT
                    date_key,
                    COALESCE(child_key, -1) AS child_key,
                    COALESCE(center_key, -1) AS center_key,
                    COALESCE(teacher_key, -1) AS teacher_key,
                    CAST(raw_score AS DECIMAL(10,2)) AS raw_score,
                    CAST(min_score AS DECIMAL(10,2)) AS min_score,
                    CAST(max_score AS DECIMAL(10,2)) AS max_score,
                    CAST(normalized_score AS DECIMAL(10,4)) AS normalized_score,
                    planned_task_count,
                    assessment_count,
                    completed_task_count,
                    scored_task_count,
                    not_scored_task_count
                FROM {DW_DB}.dw.fact_daily_student_task_progress
                WHERE date_key = ?
            )
            SELECT COUNT(*)
            FROM
            (
                SELECT *
                FROM
                (
                    SELECT * FROM expected
                    EXCEPT
                    SELECT * FROM actual
                ) AS missing_or_changed

                UNION ALL

                SELECT *
                FROM
                (
                    SELECT * FROM actual
                    EXCEPT
                    SELECT * FROM expected
                ) AS extra_or_changed
            ) AS diff;
            """,
            [dk, d, dk],
        )


def validate_lifecycle_against_snapshot_and_transaction_history(cn, latest_day: dt.date) -> None:
    latest_key = date_key(latest_day)

    assert_zero(
        cn,
        f"lifecycle counts match latest daily snapshot {latest_day}",
        f"""
        WITH expected AS
        (
            SELECT
                COALESCE(child_key, -1) AS child_key,
                COALESCE(center_key, -1) AS center_key,
                COALESCE(teacher_key, -1) AS teacher_key,
                planned_task_count,
                assessment_count,
                completed_task_count,
                scored_task_count
            FROM {DW_DB}.dw.fact_daily_student_task_progress
            WHERE date_key = ?
        ),
        actual AS
        (
            SELECT
                COALESCE(child_key, -1) AS child_key,
                COALESCE(center_key, -1) AS center_key,
                COALESCE(teacher_key, -1) AS teacher_key,
                planned_task_count,
                assessment_count,
                completed_task_count,
                scored_task_count
            FROM {DW_DB}.dw.fact_child_snapshot_accumulation
        )
        SELECT COUNT(*)
        FROM
        (
            SELECT *
            FROM
            (
                SELECT * FROM expected
                EXCEPT
                SELECT * FROM actual
            ) AS missing_or_changed

            UNION ALL

            SELECT *
            FROM
            (
                SELECT * FROM actual
                EXCEPT
                SELECT * FROM expected
            ) AS extra_or_changed
        ) AS diff;
        """,
        [latest_key],
    )

    assert_zero(
        cn,
        "lifecycle first/last date keys match transaction history",
        f"""
        WITH expected AS
        (
            SELECT
                COALESCE(child_key, -1) AS child_key,
                COALESCE(center_key, -1) AS center_key,
                COALESCE(teacher_key, -1) AS teacher_key,

                MIN(CASE
                        WHEN source_daily_task_assignment_id IS NOT NULL
                        THEN COALESCE(date_key, -1)
                    END) AS first_plan_date_key,

                MAX(CASE
                        WHEN source_daily_task_assignment_id IS NOT NULL
                        THEN COALESCE(date_key, -1)
                    END) AS last_plan_date_key,

                MIN(CASE
                        WHEN source_task_assessment_id IS NOT NULL
                        THEN COALESCE(date_key, -1)
                    END) AS first_assessment_date_key,

                MAX(CASE
                        WHEN source_task_assessment_id IS NOT NULL
                        THEN COALESCE(date_key, -1)
                    END) AS last_assessment_date_key
            FROM {DW_DB}.dw.fact_tran_student_task_progress
            GROUP BY
                COALESCE(child_key, -1),
                COALESCE(center_key, -1),
                COALESCE(teacher_key, -1)
        ),
        actual AS
        (
            SELECT
                COALESCE(child_key, -1) AS child_key,
                COALESCE(center_key, -1) AS center_key,
                COALESCE(teacher_key, -1) AS teacher_key,
                first_plan_date_key,
                last_plan_date_key,
                first_assessment_date_key,
                last_assessment_date_key
            FROM {DW_DB}.dw.fact_child_snapshot_accumulation
        )
        SELECT COUNT(*)
        FROM
        (
            SELECT *
            FROM
            (
                SELECT * FROM expected
                EXCEPT
                SELECT * FROM actual
            ) AS missing_or_changed

            UNION ALL

            SELECT *
            FROM
            (
                SELECT * FROM actual
                EXCEPT
                SELECT * FROM expected
            ) AS extra_or_changed
        ) AS diff;
        """,
    )


def validate_previous_day_signatures_are_stable(cn, state: TestState, checked_days: list[dt.date]) -> None:
    for d in checked_days:
        dk = date_key(d)

        assert_signature_stable(
            state.tran_signature_by_date_key,
            dk,
            transaction_signature_for_date(cn, d),
            f"transaction fact date_key={dk}",
        )

        assert_signature_stable(
            state.event_signature_by_date_key,
            dk,
            event_signature_for_date(cn, d),
            f"event fact date_key={dk}",
        )

        assert_signature_stable(
            state.snapshot_signature_by_date_key,
            dk,
            snapshot_signature_for_date(cn, d),
            f"daily snapshot fact date_key={dk}",
        )


def validate_dw_state(
    cn,
    state: TestState,
    label: str,
    checked_days: list[dt.date],
    window_start_date: dt.date,
) -> None:
    print(f"\nValidating DW state after {label}...")

    assert_equal(
        cn,
        "source daily_task_assignments cumulative count",
        f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.daily_task_assignments;",
        state.expected_assignments,
    )

    assert_equal(
        cn,
        "source task_assessments cumulative count",
        f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.task_assessments;",
        state.expected_assessments,
    )

    assert_equal(
        cn,
        "staging daily_task_assignments cumulative count",
        f"SELECT COUNT(*) FROM {STG_DB}.stg_program_ops.daily_task_assignments;",
        state.expected_assignments,
    )

    assert_equal(
        cn,
        "staging task_assessments cumulative count",
        f"SELECT COUNT(*) FROM {STG_DB}.stg_program_ops.task_assessments;",
        state.expected_assessments,
    )

    expected_fact_tran = state.expected_assignments + state.expected_assessments

    assert_equal(
        cn,
        "dw fact_tran_student_task_progress append-only row count",
        f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_tran_student_task_progress;",
        expected_fact_tran,
    )

    assert_equal(
        cn,
        "dw fact_child_task_event append-only row count",
        f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_task_event;",
        expected_fact_tran,
    )

    assert_equal(
        cn,
        "dw transaction fact planned-only count",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_daily_task_assignment_id IS NOT NULL
          AND source_task_assessment_id IS NULL;
        """,
        state.expected_assignments,
    )

    assert_equal(
        cn,
        "dw transaction fact assessment count",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NOT NULL;
        """,
        state.expected_assessments,
    )

    # No duplicate transaction natural rows.
    assert_zero(
        cn,
        "no duplicate planned rows in transaction fact",
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT source_daily_task_assignment_id, COUNT(*) AS cnt
            FROM {DW_DB}.dw.fact_tran_student_task_progress
            WHERE source_task_assessment_id IS NULL
              AND source_daily_task_assignment_id IS NOT NULL
            GROUP BY source_daily_task_assignment_id
            HAVING COUNT(*) > 1
        ) AS d;
        """,
    )

    assert_zero(
        cn,
        "no duplicate assessment rows in transaction fact",
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT source_task_assessment_id, COUNT(*) AS cnt
            FROM {DW_DB}.dw.fact_tran_student_task_progress
            WHERE source_task_assessment_id IS NOT NULL
            GROUP BY source_task_assessment_id
            HAVING COUNT(*) > 1
        ) AS d;
        """,
    )

    # No duplicate event natural rows.
    assert_zero(
        cn,
        "no duplicate PLAN events",
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT source_daily_task_assignment_id, COUNT(*) AS cnt
            FROM {DW_DB}.dw.fact_child_task_event
            WHERE event_type = N'PLAN'
              AND source_daily_task_assignment_id IS NOT NULL
              AND source_task_assessment_id IS NULL
            GROUP BY source_daily_task_assignment_id
            HAVING COUNT(*) > 1
        ) AS d;
        """,
    )

    assert_zero(
        cn,
        "no duplicate ASSESSMENT events",
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT source_task_assessment_id, COUNT(*) AS cnt
            FROM {DW_DB}.dw.fact_child_task_event
            WHERE event_type = N'ASSESSMENT'
              AND source_task_assessment_id IS NOT NULL
            GROUP BY source_task_assessment_id
            HAVING COUNT(*) > 1
        ) AS d;
        """,
    )

    # Dimension row sanity. With Type 1 truncate+insert and Type 2 versions,
    # exact row counts can vary, so use minimum checks.
    assert_ge(cn, "dw dim_center has business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_center WHERE center_key <> -1;", 1)
    assert_ge(cn, "dw dim_child has business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_child WHERE child_key <> -1;", 1)
    assert_ge(cn, "dw dim_teacher has business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_teacher WHERE teacher_key <> -1;", 1)
    assert_ge(cn, "dw dim_task has business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_task WHERE task_key <> -1;", 1)

    # Daily snapshot should have rows for every checked day.
    for d in checked_days:
        assert_true(
            cn,
            f"daily snapshot rows exist for {d}",
            f"""
            SELECT COUNT(*)
            FROM {DW_DB}.dw.fact_daily_student_task_progress
            WHERE date_key = ?;
            """,
            [date_key(d)],
        )

    # Snapshot should not duplicate one grain for the same date.
    assert_zero(
        cn,
        "no duplicate daily snapshot grain",
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT date_key, child_key, center_key, teacher_key, COUNT(*) AS cnt
            FROM {DW_DB}.dw.fact_daily_student_task_progress
            GROUP BY date_key, child_key, center_key, teacher_key
            HAVING COUNT(*) > 1
        ) AS d;
        """,
    )

    # Lifecycle should not duplicate one lifecycle grain.
    assert_zero(
        cn,
        "no duplicate lifecycle grain",
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT child_key, center_key, teacher_key, COUNT(*) AS cnt
            FROM {DW_DB}.dw.fact_child_snapshot_accumulation
            GROUP BY child_key, center_key, teacher_key
            HAVING COUNT(*) > 1
        ) AS d;
        """,
    )

    assert_ge(
        cn,
        "lifecycle fact has rows",
        f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_snapshot_accumulation;",
        1,
    )

    # Strong correctness tests.
    validate_source_to_transaction_fact_by_day(cn, checked_days)
    validate_fact_flag_consistency(cn)
    validate_event_fact_correctness(cn, checked_days)
    validate_daily_snapshot_against_transaction_fact(cn, checked_days)
    validate_lifecycle_against_snapshot_and_transaction_history(cn, checked_days[-1])

    # Prove old-day warehouse records do not silently change in later phases.
    validate_previous_day_signatures_are_stable(cn, state, checked_days)

    current_start = day_start(window_start_date)
    validate_no_previous_transaction_updates(cn, current_start, window_start_date)

    print(f"[PASS] DW validation completed after {label}.")



def print_final_counts(cn) -> None:
    print("\nFinal DW table counts:")
    targets = [
        "dim_center",
        "dim_domain",
        "dim_score_scale",
        "dim_no_score_reason",
        "dim_assessment_status",
        "dim_child",
        "dim_teacher",
        "dim_task",
        "fact_tran_student_task_progress",
        "fact_child_task_event",
        "fact_daily_student_task_progress",
        "fact_child_snapshot_accumulation",
    ]

    for table in targets:
        count = scalar(cn, f"SELECT COUNT(*) FROM {DW_DB}.dw.{table};")
        print(f"  {table:42s} {count}")


# -----------------------------------------------------------------------------
# Scenario runner
# -----------------------------------------------------------------------------

def run_day1_first_load(cn, state: TestState) -> None:
    day = BASE_DATE
    print("\n==============================")
    print("PHASE 1: Day 1 FIRST LOAD")
    print("==============================")

    seed_day1_base_data(cn, state)
    add_transactions_for_day(cn, state, day, assignment_count=8, assessment_count=5)

    if state.expected_non_transaction_changes[day] < 10:
        raise AssertionError("Day 1 must affect at least 10 non-transaction source records.")

    start = day_start(day)
    end = day_end(day)

    run_staging_etl(cn, end)
    run_dw_first_load(cn, start, end)
    validate_dw_state(cn, state, "Day 1 first load", [day], day)


def run_single_incremental_day(cn, state: TestState, day: dt.date, assignment_count: int, assessment_count: int) -> None:
    print("\n==============================")
    print(f"PHASE: Day {day.day} INCREMENTAL")
    print("==============================")

    affected = apply_non_transaction_changes_for_day(cn, state, day)
    if affected < 10:
        raise AssertionError(f"{day}: expected at least 10 non-transaction source changes.")

    add_transactions_for_day(cn, state, day, assignment_count=assignment_count, assessment_count=assessment_count)

    start = day_start(day)
    end = day_end(day)

    run_staging_etl(cn, end)
    run_dw_incremental(cn, start, end)

    checked_days = [BASE_DATE + dt.timedelta(days=i) for i in range((day - BASE_DATE).days + 1)]
    validate_dw_state(cn, state, f"Day {day.day} incremental", checked_days, day)


def run_final_two_day_incremental(cn, state: TestState, day6: dt.date, day7: dt.date) -> None:
    print("\n===================================")
    print("FINAL PHASE: 2-DAY INCREMENTAL RUN")
    print("===================================")

    for day, assignment_count, assessment_count in [
        (day6, 7, 4),
        (day7, 6, 3),
    ]:
        affected = apply_non_transaction_changes_for_day(cn, state, day)
        if affected < 10:
            raise AssertionError(f"{day}: expected at least 10 non-transaction source changes.")
        add_transactions_for_day(cn, state, day, assignment_count=assignment_count, assessment_count=assessment_count)

    start = day_start(day6)
    end = day_end(day7)

    run_staging_etl(cn, end)
    run_dw_incremental(cn, start, end)

    checked_days = [BASE_DATE + dt.timedelta(days=i) for i in range((day7 - BASE_DATE).days + 1)]
    validate_dw_state(cn, state, "final two-day incremental", checked_days, day6)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-day ETL flow test for ProgramOps DW.")
    parser.add_argument("--server", default="localhost", help="SQL Server name/host. Default: localhost")
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", help="ODBC driver name.")
    parser.add_argument("--trusted", action="store_true", help="Use Windows trusted authentication.")
    parser.add_argument("--user", default="sa", help="SQL Server username if not using --trusted.")
    parser.add_argument("--password", default="", help="SQL Server password if not using --trusted.")
    parser.add_argument("--timeout", type=int, default=120, help="pyodbc command timeout in seconds.")
    parser.add_argument("--no-reset", action="store_true", help="Do not reset source/staging/DW before running.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.trusted and not args.password:
        print("WARNING: No password provided. Use --password or --trusted.")

    cn = connect(args)
    state = TestState()

    try:
        check_required_objects(cn)

        if not args.no_reset:
            reset_source(cn)
            reset_staging(cn)
            reset_dw_except_dim_date(cn)

        ensure_dim_date_rows(cn, BASE_DATE, FINAL_END_DATE)

        # Day 1: first load only.
        run_day1_first_load(cn, state)

        # Day 2 through Day 5: four single-day incremental runs.
        run_single_incremental_day(cn, state, BASE_DATE + dt.timedelta(days=1), assignment_count=6, assessment_count=4)
        run_single_incremental_day(cn, state, BASE_DATE + dt.timedelta(days=2), assignment_count=6, assessment_count=4)
        run_single_incremental_day(cn, state, BASE_DATE + dt.timedelta(days=3), assignment_count=5, assessment_count=3)
        run_single_incremental_day(cn, state, BASE_DATE + dt.timedelta(days=4), assignment_count=5, assessment_count=3)

        # Final phase: add two days and run one 2-day ETL window.
        run_final_two_day_incremental(
            cn,
            state,
            BASE_DATE + dt.timedelta(days=5),
            BASE_DATE + dt.timedelta(days=6),
        )

        print_final_counts(cn)

        print("\nNon-transaction affected source records by day:")
        for day in sorted(state.expected_non_transaction_changes):
            print(f"  {day}: {state.expected_non_transaction_changes[day]}")

        print("\nExpected final transaction totals:")
        print(f"  daily_task_assignments: {state.expected_assignments}")
        print(f"  task_assessments:       {state.expected_assessments}")
        print(f"  expected fact/event:    {state.expected_assignments + state.expected_assessments}")

        print("\nALL COMPREHENSIVE MULTI-DAY TESTS PASSED ✅")
        return 0

    except Exception as exc:
        try:
            cn.rollback()
        except Exception:
            pass

        print("\nMULTI-DAY TEST FAILED ❌")
        print(str(exc))
        return 1

    finally:
        cn.close()


if __name__ == "__main__":
    raise SystemExit(main())
