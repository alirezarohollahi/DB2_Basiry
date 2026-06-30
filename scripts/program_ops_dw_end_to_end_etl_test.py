#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ProgramOps Data Warehouse End-to-End ETL Smoke Test
==================================================

What this script does:
    1. Assumes tables are empty or resets them itself.
    2. Keeps/creates dim_date rows needed for the test.
    3. Inserts deterministic test data into Source_ProgramOps_DB.program_ops.
    4. Runs Source -> Staging ETL:
           Stg_ProgramOps_DB.etl_admin.usp_run_stg_program_ops_all
    5. Runs DW first-load procedures:
           dimensions -> transaction/event facts -> daily snapshot -> lifecycle
    6. Validates DW output against expected business results.
    7. Inserts/updates additional source records.
    8. Runs Source -> Staging again.
    9. Runs DW incremental procedures.
   10. Validates the incremental DW result.

Important:
    - This script is designed for SQL Server.
    - It uses pyodbc.
    - Install dependency:
          pip install pyodbc
    - Example run:
          python program_ops_dw_end_to_end_etl_test.py --server localhost --trusted
      or:
          python program_ops_dw_end_to_end_etl_test.py --server localhost --user sa --password "YourPassword"

Before running:
    Make sure these scripts/procedures have already been installed:
        01_create_source_program_ops_db.sql
        05_create_stg_program_ops_db.sql
        06_create_stg_program_ops_tables.sql
        09_create_etl_program_ops_to_staging_procedures.sql
        11_create_dw_db.sql
        12_create_dw_mart1_tables.sql
        13_etl_dw_dim_center_procedures.sql
        14_etl_dw_dim_teacher_procedures.sql
        15_etl_dw_dim_child_procedures.sql
        16_etl_dw_dim_domain_procedures.sql
        17_etl_dw_dim_task_procedures.sql
        18_etl_dw_dim_score_scale_procedures.sql
        19_etl_dw_dim_assessment_status_procedures.sql
        20_etl_dw_dim_no_score_reason_procedures.sql
        21_etl_dw_fact_tran_student_task_progress_procedures.sql
        22_etl_dw_fact_daily_student_task_progress_WHILE_corrected.sql
        23_etl_dw_fact_child_snapshot_accumulation_procedures.sql
        24_etl_dw_fact_child_task_event_procedures.sql
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from dataclasses import dataclass
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

FIRST_START = dt.datetime(2028, 1, 1, 0, 0, 0)
FIRST_END = dt.datetime(2028, 1, 9, 0, 0, 0)

INCR_START = dt.datetime(2028, 1, 10, 0, 0, 0)
INCR_END = dt.datetime(2028, 1, 11, 0, 0, 0)

FIRST_CREATED_AT = dt.datetime(2028, 1, 1, 8, 0, 0)
INCR_CHANGED_AT = dt.datetime(2028, 1, 10, 8, 0, 0)


@dataclass
class TestContext:
    center_ids: list[int]
    teacher_ids: list[int]
    user_ids: list[int]
    child_ids: list[int]
    domain_ids: list[int]
    score_scale_ids: list[int]
    task_template_ids: list[int]
    no_score_reason_ids: list[int]
    child_task_plan_ids_by_child: dict[int, list[int]]
    assignment_ids: list[int]
    assessment_ids: list[int]
    carry_forward_child_id: int
    carry_forward_planned_by_user_id: int
    updated_assessment_id: int | None = None
    new_child_id: int | None = None
    new_assignment_ids: list[int] | None = None
    new_assessment_ids: list[int] | None = None


# -----------------------------------------------------------------------------
# Connection and execution helpers
# -----------------------------------------------------------------------------

def build_connection_string(args: argparse.Namespace) -> str:
    driver = args.driver
    parts = [
        f"DRIVER={{{driver}}}",
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
    conn_str = build_connection_string(args)
    cn = pyodbc.connect(conn_str, autocommit=False)
    cn.timeout = args.timeout
    return cn


def drain_cursor(cur) -> None:
    """Consume all possible result sets/messages from a pyodbc cursor."""
    while True:
        try:
            more = cur.nextset()
        except pyodbc.ProgrammingError:
            break
        if not more:
            break


def exec_sql(cn, sql: str, params: Iterable[Any] | None = None) -> None:
    cur = cn.cursor()
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, tuple(params))
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


def assert_equal(cn, label: str, actual_sql: str, expected: Any, params: Iterable[Any] | None = None) -> None:
    actual = scalar(cn, actual_sql, params)
    if actual != expected:
        raise AssertionError(f"[FAIL] {label}: expected {expected!r}, got {actual!r}")
    print(f"[PASS] {label}: {actual!r}")


def assert_ge(cn, label: str, actual_sql: str, minimum: int, params: Iterable[Any] | None = None) -> None:
    actual = scalar(cn, actual_sql, params)
    if actual is None or actual < minimum:
        raise AssertionError(f"[FAIL] {label}: expected >= {minimum!r}, got {actual!r}")
    print(f"[PASS] {label}: {actual!r} >= {minimum!r}")


def assert_true(cn, label: str, actual_sql: str, params: Iterable[Any] | None = None) -> None:
    actual = scalar(cn, actual_sql, params)
    if not actual:
        raise AssertionError(f"[FAIL] {label}: expected true/non-zero, got {actual!r}")
    print(f"[PASS] {label}: {actual!r}")


# -----------------------------------------------------------------------------
# Object checks
# -----------------------------------------------------------------------------

def check_required_objects(cn) -> None:
    required_procs = [
        f"{STG_DB}.etl_admin.usp_run_stg_program_ops_all",

        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_center",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_teacher",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_child",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_domain",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_task",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_score_scale",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_assessment_status",
        f"{DW_DB}.etl_admin.usp_first_load_dw_dim_no_score_reason",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_tran_student_task_progress",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_child_task_event",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_daily_student_task_progress",
        f"{DW_DB}.etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation",

        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_center",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_teacher",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_child",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_domain",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_task",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_score_scale",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_assessment_status",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_dim_no_score_reason",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_child_task_event",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress",
        f"{DW_DB}.etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation",
    ]

    missing = []
    for proc in required_procs:
        exists = scalar(
            cn,
            "SELECT CASE WHEN OBJECT_ID(?, 'P') IS NULL THEN 0 ELSE 1 END;",
            [proc],
        )
        if exists != 1:
            missing.append(proc)

    if missing:
        print("\nMissing required procedures:")
        for item in missing:
            print("  -", item)
        raise RuntimeError("Required ETL procedures are missing. Install all procedure SQL files first.")

    print("[PASS] Required stored procedures exist.")


# -----------------------------------------------------------------------------
# Reset and dim_date
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
    print("\nResetting DW fact/dimension tables except dim_date...")

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

    cn.commit()
    print("[PASS] DW facts/dimensions/logs reset. dim_date was preserved.")


def ensure_dim_date_rows(cn, start_date: dt.date, end_date: dt.date) -> None:
    """
    Ensures dim_date has unknown row and all dates in [start_date, end_date].
    This does not truncate or rebuild dim_date.
    """
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
        time_key = int(current.strftime("%Y%m%d"))
        iso_week = int(current.strftime("%V"))
        quarter = ((current.month - 1) // 3) + 1
        semester = 1 if current.month <= 6 else 2
        day_name = current.strftime("%A")
        month_name = current.strftime("%B")

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
                time_key,
                time_key,
                current,
                current.isoweekday(),
                day_name,
                current.day,
                int(current.strftime("%j")),
                iso_week,
                month_name,
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
# Source test data
# -----------------------------------------------------------------------------

def seed_first_load_source_data(cn) -> TestContext:
    print("\nSeeding first-load source data...")

    center_ids: list[int] = []
    for name, city, address in [
        ("Alpha Autism Center", "Tehran", "Alpha Street 1"),
        ("Beta Rehab Center", "Isfahan", "Beta Street 2"),
        ("Gamma Learning Center", "Shiraz", "Gamma Street 3"),
    ]:
        center_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "centers",
                ["name", "city", "address", "is_active", "created_at", "updated_at"],
                [name, city, address, 1, FIRST_CREATED_AT, None],
            )
        )

    children_seed = [
        (center_ids[0], "Ali", "Ahmadi", "NC1001", dt.date(2016, 3, 5), "male"),
        (center_ids[0], "Sara", "Karimi", "NC1002", dt.date(2017, 6, 12), "female"),
        (center_ids[1], "Reza", "Mohammadi", "NC1003", dt.date(2015, 9, 20), "male"),
        (center_ids[1], "Nika", "Rahimi", "NC1004", dt.date(2018, 2, 2), "female"),
        (center_ids[2], "Mina", "Hosseini", "NC1005", dt.date(2016, 11, 15), "female"),
        (center_ids[2], "Arman", "Jafari", "NC1006", dt.date(2017, 1, 19), "male"),
        (center_ids[0], "Tara", "Moradi", "NC1007", dt.date(2016, 7, 25), "female"),
        (center_ids[1], "Kian", "Abbasi", "NC1008", dt.date(2015, 12, 8), "male"),
    ]

    child_ids: list[int] = []
    for center_id, first_name, last_name, nc, birth, gender in children_seed:
        child_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "children",
                ["center_id", "first_name", "last_name", "national_code", "birth_date",
                 "gender", "enrollment_date", "status", "created_at", "updated_at"],
                [center_id, first_name, last_name, nc, birth, gender,
                 dt.date(2027, 9, 1), "active", FIRST_CREATED_AT, None],
            )
        )

    teachers_seed = [
        (center_ids[0], "Maryam", "Teacher", "09120000001", "maryam.teacher@example.com"),
        (center_ids[0], "Hamed", "Coach", "09120000002", "hamed.coach@example.com"),
        (center_ids[1], "Leila", "Trainer", "09120000003", "leila.trainer@example.com"),
        (center_ids[2], "Omid", "Mentor", "09120000004", "omid.mentor@example.com"),
    ]

    teacher_ids: list[int] = []
    for center_id, first_name, last_name, phone, email in teachers_seed:
        teacher_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "teachers",
                ["center_id", "first_name", "last_name", "phone", "email",
                 "employment_status", "is_active", "created_at", "updated_at"],
                [center_id, first_name, last_name, phone, email,
                 "active", 1, FIRST_CREATED_AT, None],
            )
        )

    user_ids: list[int] = []
    for idx, teacher_id in enumerate(teacher_ids, start=1):
        user_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "users",
                ["username", "password_hash", "role", "teacher_id", "is_active", "created_at", "updated_at"],
                [f"teacher_user_{idx}", "not-a-real-password-hash", "teacher", teacher_id, 1, FIRST_CREATED_AT, None],
            )
        )

    domain_ids: list[int] = []
    for name, desc in [
        ("Communication", "Speech and communication tasks"),
        ("Motor Skills", "Fine and gross motor tasks"),
        ("Cognitive", "Attention and cognitive tasks"),
    ]:
        domain_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "domains",
                ["name", "description", "is_active", "created_at", "updated_at"],
                [name, desc, 1, FIRST_CREATED_AT, None],
            )
        )

    score_scale_ids: list[int] = []
    for name, min_score, max_score, desc in [
        ("Five Point Scale", 0, 5, "0 to 5 scoring"),
        ("Ten Point Scale", 0, 10, "0 to 10 scoring"),
        ("Percent Scale", 0, 100, "0 to 100 scoring"),
    ]:
        score_scale_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "score_scales",
                ["name", "min_score", "max_score", "description", "is_active", "created_at", "updated_at"],
                [name, min_score, max_score, desc, 1, FIRST_CREATED_AT, None],
            )
        )

    no_score_reason_ids: list[int] = []
    for title, desc in [
        ("Child absent", "The child was absent"),
        ("Child refused", "The child refused the activity"),
        ("Center closed", "The center was closed"),
        ("System issue", "System or recording issue"),
    ]:
        no_score_reason_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "no_score_reasons",
                ["title", "description", "is_active", "created_at", "updated_at"],
                [title, desc, 1, FIRST_CREATED_AT, None],
            )
        )

    # These lookup tables are loaded to staging by the orchestration procedure,
    # and they help exercise the full source -> staging flow.
    absence_reason_id = insert_row(
        cn, SOURCE_DB, "program_ops", "absence_reasons",
        ["title", "description", "is_active", "created_at", "updated_at"],
        ["Illness", "Child illness", 1, FIRST_CREATED_AT, None],
    )
    closure_reason_id = insert_row(
        cn, SOURCE_DB, "program_ops", "closure_reasons",
        ["title", "description", "is_active", "created_at", "updated_at"],
        ["Holiday", "Center holiday", 1, FIRST_CREATED_AT, None],
    )

    template_seed = [
        (domain_ids[0], "Eye Contact Practice", "Practice short eye contact", score_scale_ids[0]),
        (domain_ids[0], "Request With Words", "Request an object verbally", score_scale_ids[1]),
        (domain_ids[1], "Fine Motor Blocks", "Stack small blocks", score_scale_ids[0]),
        (domain_ids[1], "Balance Walk", "Walk on a line", score_scale_ids[1]),
        (domain_ids[2], "Color Matching", "Match colors", score_scale_ids[2]),
        (domain_ids[2], "Attention Game", "Sustain attention", score_scale_ids[2]),
    ]

    task_template_ids: list[int] = []
    for domain_id, title, desc, scale_id in template_seed:
        task_template_ids.append(
            insert_row(
                cn, SOURCE_DB, "program_ops", "task_templates",
                ["domain_id", "title", "description", "default_score_scale_id",
                 "is_active", "created_by", "created_at", "updated_at"],
                [domain_id, title, desc, scale_id, 1, user_ids[0], FIRST_CREATED_AT, None],
            )
        )

    # Map each center to a primary teacher/user.
    primary_teacher_by_center = {
        center_ids[0]: teacher_ids[0],
        center_ids[1]: teacher_ids[2],
        center_ids[2]: teacher_ids[3],
    }
    primary_user_by_teacher = {
        teacher_ids[0]: user_ids[0],
        teacher_ids[1]: user_ids[1],
        teacher_ids[2]: user_ids[2],
        teacher_ids[3]: user_ids[3],
    }

    child_task_plan_ids_by_child: dict[int, list[int]] = {}
    for child_idx, child_id in enumerate(child_ids):
        child_task_plan_ids_by_child[child_id] = []
        for offset in [0, 1]:
            template_idx = (child_idx + offset) % len(task_template_ids)
            template_id = task_template_ids[template_idx]
            domain_id, _, _, scale_id = template_seed[template_idx]
            plan_id = insert_row(
                cn, SOURCE_DB, "program_ops", "child_task_plans",
                ["child_id", "task_template_id", "domain_id", "task_title", "score_scale_id",
                 "start_date", "end_date", "is_active", "created_by", "created_at", "updated_at"],
                [child_id, template_id, domain_id, template_seed[template_idx][1], scale_id,
                 dt.date(2028, 1, 2), None, 1, user_ids[0], FIRST_CREATED_AT, None],
            )
            child_task_plan_ids_by_child[child_id].append(plan_id)

    # Daily statuses exercise is_absent and is_center_closed flags in transaction fact.
    for child_id in [child_ids[1], child_ids[7]]:
        insert_row(
            cn, SOURCE_DB, "program_ops", "child_daily_status",
            ["child_id", "date", "status", "absence_reason_id", "note", "created_by", "created_at", "updated_at"],
            [child_id, dt.date(2028, 1, 4), "absent", absence_reason_id, "Absent for test", user_ids[0], FIRST_CREATED_AT, None],
        )

    insert_row(
        cn, SOURCE_DB, "program_ops", "center_daily_status",
        ["center_id", "date", "status", "closure_reason_id", "note", "created_by", "created_at", "updated_at"],
        [center_ids[2], dt.date(2028, 1, 5), "closed", closure_reason_id, "Closed for test", user_ids[0], FIRST_CREATED_AT, None],
    )

    # Create exactly 30 daily assignments.
    # Child 8 gets only a Jan-02 assignment, which lets us test that the daily
    # snapshot WHILE loop carries the same child/center/teacher grain to later days.
    assignment_plan = [
        (dt.date(2028, 1, 2), child_ids[0:8]),  # 8
        (dt.date(2028, 1, 3), child_ids[0:6]),  # 6
        (dt.date(2028, 1, 4), child_ids[0:6]),  # 6
        (dt.date(2028, 1, 5), child_ids[0:5]),  # 5
        (dt.date(2028, 1, 6), child_ids[0:5]),  # 5
    ]

    assignment_ids: list[int] = []
    assignment_meta: dict[int, dict[str, Any]] = {}

    for business_date, children_for_day in assignment_plan:
        for child_id in children_for_day:
            child_center_id = scalar(
                cn,
                f"SELECT center_id FROM {SOURCE_DB}.program_ops.children WHERE id = ?;",
                [child_id],
            )
            teacher_id = primary_teacher_by_center[child_center_id]
            planned_by = primary_user_by_teacher[teacher_id]
            plans = child_task_plan_ids_by_child[child_id]
            plan_id = plans[(len(assignment_ids) + child_id) % len(plans)]

            plan_row = fetch_one(
                cn,
                f"""
                SELECT task_template_id, domain_id, task_title, score_scale_id
                FROM {SOURCE_DB}.program_ops.child_task_plans
                WHERE id = ?;
                """,
                [plan_id],
            )

            assignment_status = "completed" if len(assignment_ids) % 4 == 0 else "planned"

            assignment_id = insert_row(
                cn, SOURCE_DB, "program_ops", "daily_task_assignments",
                ["child_id", "date", "child_task_plan_id", "task_template_id", "domain_id",
                 "task_title", "score_scale_id", "planned_by", "status", "created_at", "updated_at"],
                [child_id, business_date, plan_id, plan_row.task_template_id, plan_row.domain_id,
                 plan_row.task_title, plan_row.score_scale_id, planned_by, assignment_status, FIRST_CREATED_AT, None],
            )

            assignment_ids.append(assignment_id)
            assignment_meta[assignment_id] = {
                "child_id": child_id,
                "center_id": child_center_id,
                "teacher_id": teacher_id,
                "business_date": business_date,
                "score_scale_id": plan_row.score_scale_id,
            }

    assessment_ids: list[int] = []

    # Create exactly 20 assessments for 20 of the 30 assignments.
    assessed_assignment_ids = [assignment_id for idx, assignment_id in enumerate(assignment_ids) if idx % 3 != 0]
    assert len(assessed_assignment_ids) == 20

    for idx, assignment_id in enumerate(assessed_assignment_ids):
        meta = assignment_meta[assignment_id]

        session_id = insert_row(
            cn, SOURCE_DB, "program_ops", "assessment_sessions",
            ["child_id", "teacher_id", "center_id", "date", "started_at", "ended_at",
             "session_status", "general_note", "created_at", "updated_at"],
            [meta["child_id"], meta["teacher_id"], meta["center_id"], meta["business_date"],
             dt.datetime.combine(meta["business_date"], dt.time(9, 0)),
             dt.datetime.combine(meta["business_date"], dt.time(9, 30)),
             "closed", "Assessment session for test", FIRST_CREATED_AT, None],
        )

        if idx % 7 == 0:
            status = "refused"
            score = None
            no_score_reason_id = no_score_reason_ids[1]
        elif idx % 5 == 0:
            status = "not_scored"
            score = None
            no_score_reason_id = no_score_reason_ids[0]
        else:
            status = "scored"
            # Keep scores in a reasonable range. Some scales are 0-100, some 0-5/10.
            scale_max = scalar(
                cn,
                f"SELECT max_score FROM {SOURCE_DB}.program_ops.score_scales WHERE id = ?;",
                [meta["score_scale_id"]],
            )
            score = float(min(scale_max, 1 + (idx % int(max(2, scale_max)))))
            no_score_reason_id = None

        assessment_id = insert_row(
            cn, SOURCE_DB, "program_ops", "task_assessments",
            ["daily_task_assignment_id", "assessment_session_id", "child_id", "teacher_id",
             "date", "score", "normalized_score", "assessment_status", "no_score_reason_id",
             "attempt_no", "note", "created_at", "updated_at"],
            [assignment_id, session_id, meta["child_id"], meta["teacher_id"],
             meta["business_date"], score, None, status, no_score_reason_id,
             1, "Task assessment for test", FIRST_CREATED_AT, None],
        )
        assessment_ids.append(assessment_id)

    cn.commit()

    print("[PASS] First-load source seed complete:")
    print("       centers=3, children=8, teachers=4, users=4, domains=3, score_scales=3")
    print("       task_templates=6, child_task_plans=16, daily_task_assignments=30, task_assessments=20")

    return TestContext(
        center_ids=center_ids,
        teacher_ids=teacher_ids,
        user_ids=user_ids,
        child_ids=child_ids,
        domain_ids=domain_ids,
        score_scale_ids=score_scale_ids,
        task_template_ids=task_template_ids,
        no_score_reason_ids=no_score_reason_ids,
        child_task_plan_ids_by_child=child_task_plan_ids_by_child,
        assignment_ids=assignment_ids,
        assessment_ids=assessment_ids,
        carry_forward_child_id=child_ids[7],
        carry_forward_planned_by_user_id=primary_user_by_teacher[primary_teacher_by_center[center_ids[1]]],
    )


def seed_incremental_source_changes(cn, ctx: TestContext) -> None:
    print("\nSeeding incremental source changes...")

    # SCD2 checks for center and teacher.
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.centers
        SET city = ?, updated_at = ?
        WHERE id = ?;
        """,
        ["Tehran-East-Updated", INCR_CHANGED_AT, ctx.center_ids[0]],
    )

    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.teachers
        SET employment_status = ?, updated_at = ?
        WHERE id = ?;
        """,
        ["on_leave", INCR_CHANGED_AT, ctx.teacher_ids[0]],
    )

    # Type 1 child update.
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.children
        SET status = ?, updated_at = ?
        WHERE id = ?;
        """,
        ["active_follow_up", INCR_CHANGED_AT, ctx.child_ids[0]],
    )

    # Add a new child.
    new_child_id = insert_row(
        cn, SOURCE_DB, "program_ops", "children",
        ["center_id", "first_name", "last_name", "national_code", "birth_date",
         "gender", "enrollment_date", "status", "created_at", "updated_at"],
        [ctx.center_ids[1], "Yas", "NewChild", "NC2001", dt.date(2017, 4, 3),
         "female", dt.date(2028, 1, 7), "active", INCR_CHANGED_AT, None],
    )
    ctx.new_child_id = new_child_id

    # Add two plans for the new child.
    ctx.child_task_plan_ids_by_child[new_child_id] = []
    for template_id in [ctx.task_template_ids[0], ctx.task_template_ids[4]]:
        plan_source = fetch_one(
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
            [new_child_id, template_id, plan_source.domain_id, plan_source.title, plan_source.default_score_scale_id,
             dt.date(2028, 1, 7), None, 1, ctx.user_ids[2], INCR_CHANGED_AT, None],
        )
        ctx.child_task_plan_ids_by_child[new_child_id].append(plan_id)

    # Update one existing assessment score. It must update DW fact, not duplicate it.
    ctx.updated_assessment_id = ctx.assessment_ids[0]
    exec_sql(
        cn,
        f"""
        UPDATE {SOURCE_DB}.program_ops.task_assessments
        SET score = ?, normalized_score = NULL, assessment_status = ?, no_score_reason_id = NULL, updated_at = ?
        WHERE id = ?;
        """,
        [5.00, "scored", INCR_CHANGED_AT, ctx.updated_assessment_id],
    )

    # Add 5 new assignments on Jan-07 and 3 assessments.
    new_assignment_ids: list[int] = []
    new_assessment_ids: list[int] = []

    new_assignment_children = [
        ctx.carry_forward_child_id,  # proves old carried grain also gets new activity
        new_child_id,
        ctx.child_ids[0],
        ctx.child_ids[2],
        ctx.child_ids[4],
    ]

    for idx, child_id in enumerate(new_assignment_children):
        child_center_id = scalar(
            cn,
            f"SELECT center_id FROM {SOURCE_DB}.program_ops.children WHERE id = ?;",
            [child_id],
        )

        teacher_id = ctx.teacher_ids[2] if child_center_id == ctx.center_ids[1] else ctx.teacher_ids[0]
        planned_by_user_id = ctx.user_ids[2] if teacher_id == ctx.teacher_ids[2] else ctx.user_ids[0]

        if child_id == new_child_id:
            plan_id = ctx.child_task_plan_ids_by_child[new_child_id][idx % 2]
        else:
            plan_id = ctx.child_task_plan_ids_by_child[child_id][0]

        plan_row = fetch_one(
            cn,
            f"""
            SELECT task_template_id, domain_id, task_title, score_scale_id
            FROM {SOURCE_DB}.program_ops.child_task_plans
            WHERE id = ?;
            """,
            [plan_id],
        )

        assignment_id = insert_row(
            cn, SOURCE_DB, "program_ops", "daily_task_assignments",
            ["child_id", "date", "child_task_plan_id", "task_template_id", "domain_id",
             "task_title", "score_scale_id", "planned_by", "status", "created_at", "updated_at"],
            [child_id, dt.date(2028, 1, 7), plan_id, plan_row.task_template_id, plan_row.domain_id,
             plan_row.task_title, plan_row.score_scale_id, planned_by_user_id, "planned", INCR_CHANGED_AT, None],
        )
        new_assignment_ids.append(assignment_id)

        # Add assessments for the first 3 new assignments.
        if idx < 3:
            session_id = insert_row(
                cn, SOURCE_DB, "program_ops", "assessment_sessions",
                ["child_id", "teacher_id", "center_id", "date", "started_at", "ended_at",
                 "session_status", "general_note", "created_at", "updated_at"],
                [child_id, teacher_id, child_center_id, dt.date(2028, 1, 7),
                 dt.datetime(2028, 1, 7, 10, 0), dt.datetime(2028, 1, 7, 10, 25),
                 "closed", "Incremental assessment session", INCR_CHANGED_AT, None],
            )

            assessment_id = insert_row(
                cn, SOURCE_DB, "program_ops", "task_assessments",
                ["daily_task_assignment_id", "assessment_session_id", "child_id", "teacher_id",
                 "date", "score", "normalized_score", "assessment_status", "no_score_reason_id",
                 "attempt_no", "note", "created_at", "updated_at"],
                [assignment_id, session_id, child_id, teacher_id,
                 dt.date(2028, 1, 7), 4.00 + idx, None, "scored", None,
                 1, "Incremental assessment", INCR_CHANGED_AT, None],
            )
            new_assessment_ids.append(assessment_id)

    ctx.new_assignment_ids = new_assignment_ids
    ctx.new_assessment_ids = new_assessment_ids

    cn.commit()

    print("[PASS] Incremental source changes complete:")
    print("       updated center=1, updated teacher=1, updated child=1")
    print("       new children=1, new assignments=5, new assessments=3, updated assessment=1")


# -----------------------------------------------------------------------------
# ETL execution
# -----------------------------------------------------------------------------

def run_staging_etl(cn, to_date: dt.datetime) -> None:
    print(f"\nRunning Source -> Staging ETL to_date={to_date.isoformat()}...")
    exec_sql(
        cn,
        f"EXEC {STG_DB}.etl_admin.usp_run_stg_program_ops_all @to_date = ?;",
        [to_date],
    )
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


DW_FIRST_LOAD_PROCS = [
    "etl_admin.usp_first_load_dw_dim_center",
    "etl_admin.usp_first_load_dw_dim_teacher",
    "etl_admin.usp_first_load_dw_dim_child",
    "etl_admin.usp_first_load_dw_dim_domain",
    "etl_admin.usp_first_load_dw_dim_score_scale",
    "etl_admin.usp_first_load_dw_dim_task",
    "etl_admin.usp_first_load_dw_dim_assessment_status",
    "etl_admin.usp_first_load_dw_dim_no_score_reason",

    "etl_admin.usp_first_load_dw_fact_tran_student_task_progress",
    "etl_admin.usp_first_load_dw_fact_child_task_event",
    "etl_admin.usp_first_load_dw_fact_daily_student_task_progress",
    "etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation",
]

DW_INCREMENTAL_PROCS = [
    "etl_admin.usp_incremental_load_dw_dim_center",
    "etl_admin.usp_incremental_load_dw_dim_teacher",
    "etl_admin.usp_incremental_load_dw_dim_child",
    "etl_admin.usp_incremental_load_dw_dim_domain",
    "etl_admin.usp_incremental_load_dw_dim_score_scale",
    "etl_admin.usp_incremental_load_dw_dim_task",
    "etl_admin.usp_incremental_load_dw_dim_assessment_status",
    "etl_admin.usp_incremental_load_dw_dim_no_score_reason",

    "etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress",
    "etl_admin.usp_incremental_load_dw_fact_child_task_event",
    "etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress",
    "etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation",
]


def run_dw_procedure_list(cn, proc_names: list[str], start_time: dt.datetime, end_time: dt.datetime) -> None:
    for proc in proc_names:
        print(f"  EXEC {DW_DB}.{proc}")
        exec_sql(
            cn,
            f"EXEC {DW_DB}.{proc} @start_time = ?, @end_time = ?;",
            [start_time, end_time],
        )
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


def run_dw_first_load(cn) -> None:
    print(f"\nRunning DW first-load ETL {FIRST_START.isoformat()} <= source_updated_at < {FIRST_END.isoformat()}...")
    run_dw_procedure_list(cn, DW_FIRST_LOAD_PROCS, FIRST_START, FIRST_END)
    print("[PASS] DW first-load ETL succeeded.")


def run_dw_incremental(cn) -> None:
    print(f"\nRunning DW incremental ETL {INCR_START.isoformat()} <= source_updated_at < {INCR_END.isoformat()}...")
    run_dw_procedure_list(cn, DW_INCREMENTAL_PROCS, INCR_START, INCR_END)
    print("[PASS] DW incremental ETL succeeded.")


# -----------------------------------------------------------------------------
# Verifications
# -----------------------------------------------------------------------------

def verify_first_load(cn, ctx: TestContext) -> None:
    print("\nVerifying first-load DW result...")

    # Source sanity.
    assert_equal(cn, "source daily_task_assignments", f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.daily_task_assignments;", 30)
    assert_equal(cn, "source task_assessments", f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.task_assessments;", 20)

    # Staging sanity.
    assert_equal(cn, "staging daily_task_assignments", f"SELECT COUNT(*) FROM {STG_DB}.stg_program_ops.daily_task_assignments;", 30)
    assert_equal(cn, "staging task_assessments", f"SELECT COUNT(*) FROM {STG_DB}.stg_program_ops.task_assessments;", 20)

    # Dimension sanity, excluding unknown rows.
    assert_equal(cn, "dw dim_center business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_center WHERE center_key <> -1;", 3)
    assert_equal(cn, "dw dim_teacher business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_teacher WHERE teacher_key <> -1;", 4)
    assert_equal(cn, "dw dim_child business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_child WHERE child_key <> -1;", 8)
    assert_equal(cn, "dw dim_domain business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_domain WHERE domain_key <> -1;", 3)
    assert_equal(cn, "dw dim_score_scale business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_score_scale WHERE score_scale_key <> -1;", 3)
    assert_equal(cn, "dw dim_no_score_reason business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_no_score_reason WHERE no_score_reason_key <> -1;", 4)
    assert_ge(cn, "dw dim_task business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_task WHERE task_key <> -1;", 6)
    assert_ge(cn, "dw dim_assessment_status business rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_assessment_status WHERE assessment_status_key <> -1;", 3)

    # Fact transaction expected rows = planned rows + assessment rows.
    expected_tran_rows = 30 + 20
    assert_equal(cn, "dw fact_tran total rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_tran_student_task_progress;", expected_tran_rows)
    assert_equal(
        cn,
        "dw fact_tran planned-only rows",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_daily_task_assignment_id IS NOT NULL
          AND source_task_assessment_id IS NULL;
        """,
        30,
    )
    assert_equal(
        cn,
        "dw fact_tran assessment rows",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NOT NULL;
        """,
        20,
    )

    # Missing dimension checks. These should be zero for this deterministic valid seed.
    assert_equal(
        cn,
        "dw fact_tran rows with unresolved major dimension keys",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE child_key = -1 OR center_key = -1 OR domain_key = -1 OR task_key = -1 OR score_scale_key = -1;
        """,
        0,
    )

    # Flags from child_daily_status and center_daily_status should be present.
    assert_ge(
        cn,
        "dw fact_tran absent-flag rows",
        f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_tran_student_task_progress WHERE is_absent = 1;",
        1,
    )
    assert_ge(
        cn,
        "dw fact_tran center-closed-flag rows",
        f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_tran_student_task_progress WHERE is_center_closed = 1;",
        1,
    )

    # Event fact should have one event per transaction fact row for this design.
    assert_equal(cn, "dw fact_child_task_event rows", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_task_event;", expected_tran_rows)

    # Lifecycle fact should equal distinct child/center/teacher combinations in transaction fact.
    assert_equal(
        cn,
        "dw lifecycle row count equals distinct child/center/teacher transaction groups",
        f"""
        SELECT
            (SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_snapshot_accumulation)
            -
            (SELECT COUNT(*)
             FROM (
                SELECT child_key, center_key, teacher_key
                FROM {DW_DB}.dw.fact_tran_student_task_progress
                GROUP BY child_key, center_key, teacher_key
             ) AS x);
        """,
        0,
    )

    # Daily snapshot WHILE-loop/as-of behavior:
    # carry_forward_child_id has assignment only on Jan-02 in first load, but the
    # same grain must exist on Jan-03 and Jan-04 too.
    carry = fetch_one(
        cn,
        f"""
        SELECT TOP (1)
            ft.child_key,
            ft.center_key,
            ft.teacher_key
        FROM {DW_DB}.dw.fact_tran_student_task_progress AS ft
        INNER JOIN {DW_DB}.dw.dim_child AS dc
            ON dc.child_key = ft.child_key
        WHERE dc.child_id = ?
          AND ft.source_task_assessment_id IS NULL
        ORDER BY ft.date_key;
        """,
        [ctx.carry_forward_child_id],
    )

    if carry is None:
        raise AssertionError("[FAIL] Could not find carry-forward child grain in fact_tran.")

    for date_key in [20280102, 20280103, 20280104, 20280105, 20280106]:
        assert_equal(
            cn,
            f"daily snapshot carries child grain to date_key {date_key}",
            f"""
            SELECT COUNT(*)
            FROM {DW_DB}.dw.fact_daily_student_task_progress
            WHERE date_key = ?
              AND child_key = ?
              AND center_key = ?
              AND teacher_key = ?;
            """,
            1,
            [date_key, carry.child_key, carry.center_key, carry.teacher_key],
        )

    print("[PASS] First-load DW verification completed.")


def verify_incremental(cn, ctx: TestContext) -> None:
    print("\nVerifying incremental DW result...")

    # Source sanity after incremental changes.
    assert_equal(cn, "source children after incremental", f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.children;", 9)
    assert_equal(cn, "source daily_task_assignments after incremental", f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.daily_task_assignments;", 35)
    assert_equal(cn, "source task_assessments after incremental", f"SELECT COUNT(*) FROM {SOURCE_DB}.program_ops.task_assessments;", 23)

    # Dimensions.
    assert_equal(cn, "dw dim_child business rows after incremental", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_child WHERE child_key <> -1;", 9)

    assert_equal(
        cn,
        "dw dim_child Type 1 status updated",
        f"""
        SELECT status
        FROM {DW_DB}.dw.dim_child
        WHERE child_id = ?;
        """,
        "active_follow_up",
        [ctx.child_ids[0]],
    )

    assert_equal(
        cn,
        "dw dim_center has exactly one current version for changed center",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.dim_center
        WHERE center_id = ?
          AND is_current = 1;
        """,
        1,
        [ctx.center_ids[0]],
    )

    assert_equal(
        cn,
        "dw dim_center current city updated",
        f"""
        SELECT city
        FROM {DW_DB}.dw.dim_center
        WHERE center_id = ?
          AND is_current = 1;
        """,
        "Tehran-East-Updated",
        [ctx.center_ids[0]],
    )

    assert_ge(
        cn,
        "dw dim_center old SCD2 version closed",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.dim_center
        WHERE center_id = ?
          AND is_current = 0
          AND effective_to IS NOT NULL;
        """,
        1,
        [ctx.center_ids[0]],
    )

    assert_equal(
        cn,
        "dw dim_teacher current employment_status updated",
        f"""
        SELECT employment_status
        FROM {DW_DB}.dw.dim_teacher
        WHERE teacher_id = ?
          AND is_current = 1;
        """,
        "on_leave",
        [ctx.teacher_ids[0]],
    )

    # Fact transaction: 35 planned rows + 23 assessment rows = 58.
    expected_tran_rows = 35 + 23
    assert_equal(cn, "dw fact_tran total rows after incremental", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_tran_student_task_progress;", expected_tran_rows)
    assert_equal(
        cn,
        "dw fact_tran planned-only rows after incremental",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_daily_task_assignment_id IS NOT NULL
          AND source_task_assessment_id IS NULL;
        """,
        35,
    )
    assert_equal(
        cn,
        "dw fact_tran assessment rows after incremental",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NOT NULL;
        """,
        23,
    )

    # The updated assessment should be updated, not duplicated.
    assert_equal(
        cn,
        "updated assessment appears once in fact_tran",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id = ?;
        """,
        1,
        [ctx.updated_assessment_id],
    )

    assert_equal(
        cn,
        "updated assessment raw_score propagated",
        f"""
        SELECT CONVERT(DECIMAL(10,2), raw_score)
        FROM {DW_DB}.dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id = ?;
        """,
        5.00,
        [ctx.updated_assessment_id],
    )

    assert_equal(cn, "dw fact_child_task_event rows after incremental", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_task_event;", expected_tran_rows)

    # New child must have Jan-07 daily snapshot row.
    assert_true(
        cn,
        "new child has Jan-07 daily snapshot row",
        f"""
        SELECT COUNT(*)
        FROM {DW_DB}.dw.fact_daily_student_task_progress AS f
        INNER JOIN {DW_DB}.dw.dim_child AS dc
            ON dc.child_key = f.child_key
        WHERE dc.child_id = ?
          AND f.date_key = 20280107;
        """,
        [ctx.new_child_id],
    )

    # Lifecycle remains idempotent: one row per child/center/teacher group.
    assert_equal(
        cn,
        "dw lifecycle row count equals distinct child/center/teacher groups after incremental",
        f"""
        SELECT
            (SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_snapshot_accumulation)
            -
            (SELECT COUNT(*)
             FROM (
                SELECT child_key, center_key, teacher_key
                FROM {DW_DB}.dw.fact_tran_student_task_progress
                GROUP BY child_key, center_key, teacher_key
             ) AS x);
        """,
        0,
    )

    # Check that the carry-forward child accumulated at least two planned tasks after the Jan-07 new assignment.
    assert_ge(
        cn,
        "carry-forward child lifecycle planned_task_count after incremental",
        f"""
        SELECT MAX(f.planned_task_count)
        FROM {DW_DB}.dw.fact_child_snapshot_accumulation AS f
        INNER JOIN {DW_DB}.dw.dim_child AS dc
            ON dc.child_key = f.child_key
        WHERE dc.child_id = ?;
        """,
        2,
        [ctx.carry_forward_child_id],
    )

    print("[PASS] Incremental DW verification completed.")


def print_summary(cn) -> None:
    print("\nFinal DW table counts:")
    queries = [
        ("dim_center", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_center;"),
        ("dim_teacher", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_teacher;"),
        ("dim_child", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_child;"),
        ("dim_domain", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_domain;"),
        ("dim_task", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_task;"),
        ("dim_score_scale", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_score_scale;"),
        ("dim_assessment_status", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_assessment_status;"),
        ("dim_no_score_reason", f"SELECT COUNT(*) FROM {DW_DB}.dw.dim_no_score_reason;"),
        ("fact_tran_student_task_progress", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_tran_student_task_progress;"),
        ("fact_daily_student_task_progress", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_daily_student_task_progress;"),
        ("fact_child_snapshot_accumulation", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_snapshot_accumulation;"),
        ("fact_child_task_event", f"SELECT COUNT(*) FROM {DW_DB}.dw.fact_child_task_event;"),
    ]
    for name, query in queries:
        print(f"  {name:42s} {scalar(cn, query)}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="End-to-end ETL smoke test for ProgramOps -> Staging -> Charity DW MART1."
    )

    parser.add_argument("--server", default="localhost", help="SQL Server name/host. Default: localhost")
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", help="ODBC driver name.")
    parser.add_argument("--trusted", action="store_true", help="Use Windows trusted authentication.")
    parser.add_argument("--user", default="sa", help="SQL Server username if not using --trusted.")
    parser.add_argument("--password", default="", help="SQL Server password if not using --trusted.")
    parser.add_argument("--timeout", type=int, default=120, help="pyodbc command timeout in seconds.")
    parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Do not reset source/staging/DW tables. Not recommended for this smoke test.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.trusted and not args.password:
        print("WARNING: No password provided. Use --password or --trusted.")

    cn = connect(args)

    try:
        check_required_objects(cn)

        if not args.no_reset:
            reset_source(cn)
            reset_staging(cn)
            reset_dw_except_dim_date(cn)

        ensure_dim_date_rows(cn, dt.date(2028, 1, 1), dt.date(2028, 1, 11))

        ctx = seed_first_load_source_data(cn)

        run_staging_etl(cn, FIRST_END)
        run_dw_first_load(cn)
        verify_first_load(cn, ctx)

        seed_incremental_source_changes(cn, ctx)

        run_staging_etl(cn, INCR_END)
        run_dw_incremental(cn)
        verify_incremental(cn, ctx)

        print_summary(cn)

        print("\nALL TESTS PASSED ✅")
        return 0

    except Exception as exc:
        try:
            cn.rollback()
        except Exception:
            pass

        print("\nTEST FAILED ❌")
        print(str(exc))
        return 1

    finally:
        cn.close()


if __name__ == "__main__":
    raise SystemExit(main())
