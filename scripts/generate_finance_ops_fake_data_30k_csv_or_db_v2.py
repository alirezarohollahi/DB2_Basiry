"""
Generate around 30,000 English fake records for Source_FinanceOps_DB.finance_ops.

Two modes are supported:
    OUTPUT_MODE = "db"   -> insert directly into SQL Server Source DB
    OUTPUT_MODE = "csv"  -> write CSV files only, one CSV per source table

Requirements:
    CSV mode: pip install Faker
    DB mode : pip install Faker pyodbc

Before running in DB mode:
    1) Edit SERVER / DRIVER if needed.
    2) Make sure SQL Server is running.
    3) Make sure the Source_FinanceOps_DB database and finance_ops schema exist.

Generated records use deterministic IDs starting from BASE_ID.
In DB mode the script first deletes previous fake rows with id >= BASE_ID,
then inserts new rows.
Currency rates are inserted with a duplicate-safe loader because source DBs often already have
rates for common pairs/dates.
"""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta, time
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

from faker import Faker

# -----------------------------------------------------------------------------
# Output mode - EDIT THIS
# -----------------------------------------------------------------------------
# "db"  = insert generated data directly into SQL Server
# "csv" = write generated data as CSV files only
OUTPUT_MODE = "db"
CSV_OUTPUT_DIR = Path("finance_ops_fake_csv_30k")

# -----------------------------------------------------------------------------
# Connection settings - EDIT THESE
# -----------------------------------------------------------------------------
DRIVER = "ODBC Driver 18 for SQL Server"
SERVER = r"localhost"          # example: r"GOD-PC\SQLEXPRESS" or "localhost"
DATABASE = "Source_FinanceOps_DB"
TRUSTED_CONNECTION = True       # Windows Authentication
USERNAME = ""                   # use only if TRUSTED_CONNECTION = False
PASSWORD = ""                   # use only if TRUSTED_CONNECTION = False

# -----------------------------------------------------------------------------
# Volume settings - total is around 30k records
# -----------------------------------------------------------------------------
BASE_ID = 1_000_000
SEED = 20260627

N_DONORS = 3_000
N_CAMPAIGNS = 120
N_CATEGORIES = 80
N_DONATIONS = 12_000
N_EXPENSES = 6_000
N_PAYMENTS = 4_000
N_ALLOCATIONS = 4_000
N_FINANCIAL_TRANSACTIONS = 800
N_CURRENCY_RATES = 365

START_DATE = date(2025, 1, 1)
END_DATE = date(2026, 12, 31)

CENTER_IDS = list(range(1, 11))       # external/shared source ids; no FK in finance source
CHILD_IDS = list(range(1, 501))       # external/shared source ids; no FK in finance source
TEACHER_IDS = list(range(1, 101))     # external/shared source ids; no FK in finance source

CURRENCIES = ["IRR", "USD", "EUR"]
DONATION_TYPES = ["cash", "bank_transfer", "online", "in_kind"]
DONATION_STATUSES = ["pending", "confirmed", "rejected", "refunded"]
EXPENSE_STATUSES = ["pending", "approved", "rejected"]
PAYMENT_TYPES = ["salary", "bonus", "vendor", "refund"]
PAYMENT_STATUSES = ["pending", "approved", "paid", "cancelled", "rejected"]
CAMPAIGN_STATUSES = ["planned", "active", "closed", "cancelled"]

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)


def get_connection():
    """Create SQL Server connection. pyodbc is imported only in DB mode."""
    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError(
            "pyodbc is required only for OUTPUT_MODE = 'db'. Install it with: pip install pyodbc"
        ) from exc

    if TRUSTED_CONNECTION:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};PWD={PASSWORD};"
            "TrustServerCertificate=yes;"
        )
    return pyodbc.connect(conn_str)


def chunks(items: Sequence[tuple], size: int = 1000) -> Iterable[Sequence[tuple]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def random_date(start: date = START_DATE, end: date = END_DATE) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def as_datetime(d: date, hour_start: int = 8, hour_end: int = 18) -> datetime:
    return datetime.combine(
        d,
        time(
            hour=random.randint(hour_start, hour_end),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        ),
    )


def maybe_updated(created_at: datetime, probability: float = 0.25) -> datetime | None:
    if random.random() > probability:
        return None
    add_days = random.randint(1, 90)
    updated = created_at + timedelta(days=add_days, hours=random.randint(0, 8))
    return updated


def weighted_choice(items: list[str], weights: list[int]) -> str:
    return random.choices(items, weights=weights, k=1)[0]


def money(min_value: int, max_value: int) -> Decimal:
    value = random.randint(min_value, max_value)
    return Decimal(value).quantize(Decimal("0.01"))


def execute_many_identity(
    cursor,
    table_name: str,
    columns: list[str],
    rows: list[tuple],
    batch_size: int = 1000,
) -> None:
    if not rows:
        return
    col_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f"INSERT INTO {table_name} ({col_sql}) VALUES ({placeholders})"

    cursor.execute(f"SET IDENTITY_INSERT {table_name} ON;")
    cursor.fast_executemany = True
    for batch in chunks(rows, batch_size):
        cursor.executemany(insert_sql, batch)
    cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF;")


def delete_previous_fake_rows(cursor) -> None:
    # Reverse dependency order
    cursor.execute("DELETE FROM finance_ops.currency_rates WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.financial_transactions WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.budget_allocations WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.payments WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.expenses WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.donations WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.expense_categories WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.campaigns WHERE id >= ?", BASE_ID)
    cursor.execute("DELETE FROM finance_ops.donors WHERE id >= ?", BASE_ID)


def build_donors() -> list[tuple]:
    rows = []
    for i in range(N_DONORS):
        donor_id = BASE_ID + i
        donor_type = weighted_choice(["individual", "organization"], [75, 25])
        if donor_type == "individual":
            full_name = fake.name()
        else:
            full_name = fake.company()
        created_at = as_datetime(random_date())
        rows.append((
            donor_id,
            full_name[:200],
            f"FAKE-NID-{donor_id}",
            f"09{random.randint(10_000_000, 99_999_999)}",
            f"fake_donor_{donor_id}@example.com",
            donor_type,
            1 if random.random() > 0.03 else 0,
            created_at,
            maybe_updated(created_at),
        ))
    return rows


def build_campaigns() -> list[tuple]:
    rows = []
    campaign_words = ["Education", "Health", "Food", "Housing", "Books", "Winter", "Spring", "Emergency"]
    for i in range(N_CAMPAIGNS):
        campaign_id = BASE_ID + i
        start_d = random_date()
        end_d = start_d + timedelta(days=random.randint(30, 240))
        created_at = as_datetime(start_d - timedelta(days=random.randint(1, 20)))
        title = f"Fake {random.choice(campaign_words)} Campaign {campaign_id}"
        rows.append((
            campaign_id,
            title[:300],
            fake.paragraph(nb_sentences=3)[:2000],
            money(50_000_000, 5_000_000_000),
            start_d,
            end_d,
            weighted_choice(CAMPAIGN_STATUSES, [10, 55, 30, 5]),
            created_at,
            maybe_updated(created_at),
        ))
    return rows


def build_categories() -> list[tuple]:
    rows = []
    root_names = [
        "Education", "Health", "Food", "Housing", "Transport", "Clothing",
        "Maintenance", "Salary", "Equipment", "Administration"
    ]
    for i in range(N_CATEGORIES):
        category_id = BASE_ID + i
        if i < len(root_names):
            name = f"Fake {root_names[i]}"
            parent_id = None
        else:
            name = f"Fake {fake.word().title()} Category {category_id}"
            parent_id = BASE_ID + random.randint(0, min(i - 1, len(root_names) - 1))
        created_at = as_datetime(random_date())
        rows.append((
            category_id,
            name[:200],
            parent_id,
            1 if random.random() > 0.02 else 0,
            created_at,
            maybe_updated(created_at),
        ))
    return rows


def build_donations(donor_ids: list[int], campaign_ids: list[int]) -> list[tuple]:
    rows = []
    for i in range(N_DONATIONS):
        donation_id = BASE_ID + i
        donation_d = random_date()
        created_at = as_datetime(donation_d)
        status = weighted_choice(DONATION_STATUSES, [12, 78, 6, 4])
        rows.append((
            donation_id,
            random.choice(donor_ids),
            random.choice(campaign_ids) if random.random() > 0.08 else None,
            money(100_000, 100_000_000),
            weighted_choice(CURRENCIES, [96, 2, 2]),
            weighted_choice(DONATION_TYPES, [25, 35, 35, 5]),
            donation_d,
            status,
            f"FAKE-DON-{donation_id}",
            created_at,
            maybe_updated(created_at),
        ))
    return rows


def build_expenses(category_ids: list[int]) -> list[tuple]:
    rows = []
    for i in range(N_EXPENSES):
        expense_id = BASE_ID + i
        expense_d = random_date()
        created_at = as_datetime(expense_d)
        rows.append((
            expense_id,
            random.choice(CENTER_IDS),
            random.choice(CHILD_IDS) if random.random() > 0.35 else None,
            random.choice(category_ids),
            money(50_000, 50_000_000),
            weighted_choice(CURRENCIES, [97, 2, 1]),
            expense_d,
            fake.sentence(nb_words=10)[:2000],
            random.randint(1, 50) if random.random() > 0.2 else None,
            weighted_choice(EXPENSE_STATUSES, [15, 80, 5]),
            created_at,
            maybe_updated(created_at),
        ))
    return rows


def build_payments() -> list[tuple]:
    rows = []
    for i in range(N_PAYMENTS):
        payment_id = BASE_ID + i
        payment_d = random_date()
        created_at = as_datetime(payment_d)
        payment_type = weighted_choice(PAYMENT_TYPES, [45, 15, 35, 5])
        teacher_id = random.choice(TEACHER_IDS) if payment_type in ("salary", "bonus") else None
        rows.append((
            payment_id,
            payment_type,
            teacher_id,
            random.choice(CENTER_IDS),
            money(100_000, 80_000_000),
            weighted_choice(CURRENCIES, [98, 1, 1]),
            payment_d,
            weighted_choice(PAYMENT_STATUSES, [10, 15, 70, 2, 3]),
            created_at,
            maybe_updated(created_at),
        ))
    return rows


def build_allocations(donation_rows: list[tuple], category_ids: list[int]) -> list[tuple]:
    rows = []
    confirmed_donations = [r for r in donation_rows if r[7] == "confirmed"]
    all_donations = donation_rows

    for i in range(N_ALLOCATIONS):
        allocation_id = BASE_ID + i
        use_donation = random.random() < 0.82
        if use_donation and all_donations:
            donation = random.choice(confirmed_donations or all_donations)
            source_type = "donation"
            source_id = donation[0]
            base_date = donation[6]
            allocation_d = base_date + timedelta(days=random.randint(0, 20))
            amount = min(donation[3], money(50_000, 50_000_000))
        else:
            source_type = "internal_budget"
            source_id = None
            allocation_d = random_date()
            amount = money(100_000, 100_000_000)

        rows.append((
            allocation_id,
            source_type,
            source_id,
            random.choice(CENTER_IDS),
            random.choice(CHILD_IDS) if random.random() > 0.55 else None,
            random.choice(category_ids) if random.random() > 0.08 else None,
            amount,
            allocation_d,
            f"Fake allocation event {allocation_id}: {fake.sentence(nb_words=8)}"[:2000],
            as_datetime(allocation_d),
        ))
    return rows


def build_financial_transactions(donation_ids: list[int], expense_ids: list[int], payment_ids: list[int]) -> list[tuple]:
    rows = []
    entity_options = ["donation", "expense", "payment"]
    for i in range(N_FINANCIAL_TRANSACTIONS):
        tx_id = BASE_ID + i
        entity_type = weighted_choice(entity_options, [50, 30, 20])
        if entity_type == "donation":
            entity_id = random.choice(donation_ids)
            tx_type = "credit"
        elif entity_type == "expense":
            entity_id = random.choice(expense_ids)
            tx_type = "debit"
        else:
            entity_id = random.choice(payment_ids)
            tx_type = "debit"
        tx_date = random_date()
        rows.append((
            tx_id,
            entity_type,
            entity_id,
            tx_type,
            money(50_000, 100_000_000),
            tx_date,
            as_datetime(tx_date),
        ))
    return rows


def build_currency_rates() -> list[tuple]:
    rows = []
    pairs = [("USD", "IRR"), ("EUR", "IRR"), ("IRR", "USD"), ("IRR", "EUR")]
    d = START_DATE
    for i in range(N_CURRENCY_RATES):
        rate_id = BASE_ID + i
        from_cur, to_cur = pairs[i % len(pairs)]
        if from_cur == "USD" and to_cur == "IRR":
            rate = Decimal(random.randint(550_000, 750_000)).quantize(Decimal("0.00000001"))
        elif from_cur == "EUR" and to_cur == "IRR":
            rate = Decimal(random.randint(600_000, 850_000)).quantize(Decimal("0.00000001"))
        elif from_cur == "IRR" and to_cur == "USD":
            rate = Decimal("0.00000150")
        else:
            rate = Decimal("0.00000130")
        rows.append((rate_id, from_cur, to_cur, rate, d))
        d += timedelta(days=1)
        if d > END_DATE:
            d = START_DATE
    return rows



# -----------------------------------------------------------------------------
# Table column definitions
# -----------------------------------------------------------------------------
TABLES = {
    "donors": {
        "table_name": "finance_ops.donors",
        "columns": [
            "id", "full_name", "national_id", "phone", "email", "donor_type",
            "is_active", "created_at", "updated_at"
        ],
    },
    "campaigns": {
        "table_name": "finance_ops.campaigns",
        "columns": [
            "id", "title", "description", "target_amount", "start_date", "end_date",
            "status", "created_at", "updated_at"
        ],
    },
    "expense_categories": {
        "table_name": "finance_ops.expense_categories",
        "columns": [
            "id", "name", "parent_id", "is_active", "created_at", "updated_at"
        ],
    },
    "donations": {
        "table_name": "finance_ops.donations",
        "columns": [
            "id", "donor_id", "campaign_id", "amount", "currency", "donation_type",
            "donation_date", "status", "reference_code", "created_at", "updated_at"
        ],
    },
    "expenses": {
        "table_name": "finance_ops.expenses",
        "columns": [
            "id", "center_id", "child_id", "category_id", "amount", "currency",
            "expense_date", "description", "approved_by_user_id", "status", "created_at", "updated_at"
        ],
    },
    "payments": {
        "table_name": "finance_ops.payments",
        "columns": [
            "id", "payment_type", "teacher_id", "center_id", "amount", "currency",
            "payment_date", "status", "created_at", "updated_at"
        ],
    },
    "budget_allocations": {
        "table_name": "finance_ops.budget_allocations",
        "columns": [
            "id", "source_type", "source_id", "center_id", "child_id", "category_id",
            "allocated_amount", "allocation_date", "reason", "created_at"
        ],
    },
    "financial_transactions": {
        "table_name": "finance_ops.financial_transactions",
        "columns": [
            "id", "entity_type", "entity_id", "transaction_type", "amount",
            "transaction_date", "created_at"
        ],
    },
    "currency_rates": {
        "table_name": "finance_ops.currency_rates",
        "columns": [
            "id", "from_currency", "to_currency", "rate", "rate_date"
        ],
    },
}


def csv_value(value):
    """Convert Python values to clean CSV strings for SQL Server import."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def write_csv_file(output_dir: Path, name: str, columns: list[str], rows: list[tuple]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([csv_value(value) for value in row])
    return path


def write_all_csv(datasets: dict[str, list[tuple]]) -> None:
    print(f"Writing CSV files to: {CSV_OUTPUT_DIR.resolve()}")
    for name, rows in datasets.items():
        path = write_csv_file(CSV_OUTPUT_DIR, name, TABLES[name]["columns"], rows)
        print(f"  {path.name:<28} {len(rows):>8,} rows")

    manifest_path = CSV_OUTPUT_DIR / "_manifest.csv"
    with manifest_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["table", "file", "rows", "columns"])
        for name, rows in datasets.items():
            writer.writerow([
                TABLES[name]["table_name"],
                f"{name}.csv",
                len(rows),
                ",".join(TABLES[name]["columns"]),
            ])
    print(f"  {manifest_path.name:<28} manifest")


def insert_currency_rates_duplicate_safe(cursor, rows: list[tuple], batch_size: int = 500) -> int:
    """Insert currency rates while skipping existing from/to/date combinations.

    Source databases commonly already contain real rates for common pairs such as
    USD/IRR on some dates. The source table has a unique constraint on
    (from_currency, to_currency, rate_date), so a normal INSERT can fail.
    This loader keeps identity values deterministic for new generated rows and
    skips duplicates safely.
    """
    if not rows:
        return 0

    insert_sql = """
    INSERT INTO finance_ops.currency_rates
        (id, from_currency, to_currency, rate, rate_date)
    SELECT ?, ?, ?, ?, ?
    WHERE NOT EXISTS (
        SELECT 1
        FROM finance_ops.currency_rates AS cr
        WHERE cr.from_currency = ?
          AND cr.to_currency = ?
          AND cr.rate_date = ?
    );
    """

    params = [
        (rate_id, from_cur, to_cur, rate, rate_date, from_cur, to_cur, rate_date)
        for rate_id, from_cur, to_cur, rate, rate_date in rows
    ]

    cursor.execute("SET IDENTITY_INSERT finance_ops.currency_rates ON;")
    cursor.fast_executemany = True
    inserted_before = cursor.connection.cursor()
    inserted_before.execute("SELECT COUNT(*) FROM finance_ops.currency_rates WHERE id >= ?", BASE_ID)
    before_count = inserted_before.fetchone()[0]
    inserted_before.close()

    for batch in chunks(params, batch_size):
        cursor.executemany(insert_sql, batch)

    inserted_after = cursor.connection.cursor()
    inserted_after.execute("SELECT COUNT(*) FROM finance_ops.currency_rates WHERE id >= ?", BASE_ID)
    after_count = inserted_after.fetchone()[0]
    inserted_after.close()
    cursor.execute("SET IDENTITY_INSERT finance_ops.currency_rates OFF;")

    return after_count - before_count


def insert_all_to_db(datasets: dict[str, list[tuple]]) -> None:
    with get_connection() as conn:
        conn.autocommit = False
        cursor = conn.cursor()
        try:
            print("Deleting previous generated rows...")
            delete_previous_fake_rows(cursor)

            for name, rows in datasets.items():
                print(f"Inserting {name}...")
                if name == "currency_rates":
                    inserted = insert_currency_rates_duplicate_safe(cursor, rows)
                    skipped = len(rows) - inserted
                    print(f"  currency_rates inserted: {inserted:,}; skipped existing duplicates: {skipped:,}")
                else:
                    execute_many_identity(
                        cursor,
                        TABLES[name]["table_name"],
                        TABLES[name]["columns"],
                        rows,
                    )

            conn.commit()
            print("Done. Inserted fake data successfully.")
        except Exception:
            try:
                cursor.execute("SET IDENTITY_INSERT finance_ops.currency_rates OFF;")
            except Exception:
                pass
            conn.rollback()
            raise

def main() -> None:
    mode = OUTPUT_MODE.strip().lower()
    if mode not in {"db", "csv"}:
        raise ValueError("OUTPUT_MODE must be either 'db' or 'csv'.")

    donors = build_donors()
    campaigns = build_campaigns()
    categories = build_categories()

    donor_ids = [r[0] for r in donors]
    campaign_ids = [r[0] for r in campaigns]
    category_ids = [r[0] for r in categories]

    donations = build_donations(donor_ids, campaign_ids)
    expenses = build_expenses(category_ids)
    payments = build_payments()
    allocations = build_allocations(donations, category_ids)
    financial_transactions = build_financial_transactions(
        [r[0] for r in donations],
        [r[0] for r in expenses],
        [r[0] for r in payments],
    )
    currency_rates = build_currency_rates()

    datasets = {
        "donors": donors,
        "campaigns": campaigns,
        "expense_categories": categories,
        "donations": donations,
        "expenses": expenses,
        "payments": payments,
        "budget_allocations": allocations,
        "financial_transactions": financial_transactions,
        "currency_rates": currency_rates,
    }

    total = sum(len(rows) for rows in datasets.values())

    print(f"Prepared {total:,} fake source records.")
    print(f"OUTPUT_MODE = {mode!r}")

    if mode == "csv":
        write_all_csv(datasets)
        print("Done. CSV files generated successfully.")
    else:
        insert_all_to_db(datasets)

    print("Counts:")
    for name, rows in datasets.items():
        print(f"  {name:<23}: {len(rows):,}")
    print(f"  {'total':<23}: {total:,}")


if __name__ == "__main__":
    main()
