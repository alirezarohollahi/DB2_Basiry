# Operational Source Database - Charity Financial System

## Overview
This database is the operational source system for financial data in the charity organization. It captures donations, expenses, payments, and budget allocation events. It is designed for ETL extraction into a dimensional warehouse.

---

# CORE TABLES

## 1. donors
- id (PK)
- full_name
- national_id
- phone
- email
- donor_type (individual / organization)
- is_active
- created_at
- updated_at

---

## 2. campaigns
- id (PK)
- title
- description
- target_amount
- start_date
- end_date
- status
- created_at
- updated_at

---

## 3. donations
- id (PK)
- donor_id (FK)
- campaign_id (FK)
- amount
- currency
- donation_type (cash / bank_transfer / online / in_kind)
- donation_date
- status (pending / confirmed / rejected / refunded)
- reference_code
- created_at
- updated_at

---

## 4. expense_categories
- id (PK)
- name
- parent_id
- is_active
- created_at
- updated_at

---

## 5. expenses
- id (PK)
- center_id (FK)
- child_id (nullable)
- category_id (FK)
- amount
- currency
- expense_date
- description
- approved_by_user_id
- status (pending / approved / rejected)
- created_at
- updated_at

---

## 6. payments
- id (PK)
- payment_type (salary / bonus / vendor / refund)
- teacher_id (nullable)
- center_id (FK)
- amount
- currency
- payment_date
- status
- created_at
- updated_at

---

## 7. budget_allocations
- id (PK)
- source_type (donation / internal_budget)
- source_id
- center_id
- child_id
- category_id
- allocated_amount
- allocation_date
- reason
- created_at

---

## 8. financial_transactions (optional audit layer)
- id (PK)
- entity_type (donation / expense / payment)
- entity_id
- transaction_type (credit / debit)
- amount
- transaction_date
- created_at

---

## 9. currency_rates (optional)
- id (PK)
- from_currency
- to_currency
- rate
- rate_date

---

# DESIGN NOTES

- Fully normalized operational schema
- Designed for ETL extraction
- No analytical calculations stored in source
- Clear separation between donations, expenses, and payments
- Supports auditability and traceability