# Staging Design

## Current staging database

Database:
- `Stg_ProgramOps_DB`

Schemas:
- `stg_program_ops`
- `etl_admin`

## Purpose

`stg_program_ops` will store extracted copies of source tables from:

- `Source_ProgramOps_DB.program_ops`

`etl_admin` stores ETL operational metadata such as:

- batch status
- load logs
- row counts
- error messages

## Why separate staging from source?

The staging layer lets us:

- extract source data safely
- keep raw source snapshots
- validate data before warehouse loading
- add ETL metadata
- avoid querying operational source tables directly from the warehouse

## Future staging table pattern

Example future table:

```sql
CREATE TABLE stg_program_ops.children (
    id INT,
    center_id INT,
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    national_code NVARCHAR(20),
    birth_date DATE,
    gender NVARCHAR(20),
    enrollment_date DATE,
    status NVARCHAR(50),
    created_at DATETIME2(0),
    updated_at DATETIME2(0),

    etl_batch_id INT,
    source_system NVARCHAR(100),
    extracted_at DATETIME2(0),
    source_database NVARCHAR(128),
    source_schema NVARCHAR(128),
    source_table NVARCHAR(128),
    row_hash VARBINARY(32),
    is_valid BIT,
    validation_message NVARCHAR(MAX)
);
```
