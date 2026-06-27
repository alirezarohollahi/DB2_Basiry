/*
===============================================================================
 Debug seed data - Finance Ops
 Purpose: only 2 fixed rows per table for tracing Source -> Staging -> DW
 Notes:
   - Fixed IDs are used with IDENTITY_INSERT.
   - Fixed datetime values are used.
   - Shared reference IDs match Program Ops debug IDs:
       center_id  100001, 100002
       child_id   100001, 100002
       teacher_id 100001, 100002
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Source_FinanceOps_DB;
GO

/* Re-run cleanup: delete children tables first */
DELETE FROM finance_ops.currency_rates WHERE id IN (100001, 100002);
DELETE FROM finance_ops.financial_transactions WHERE id IN (100001, 100002);
DELETE FROM finance_ops.budget_allocations WHERE id IN (100001, 100002);
DELETE FROM finance_ops.payments WHERE id IN (100001, 100002);
DELETE FROM finance_ops.expenses WHERE id IN (100001, 100002);
DELETE FROM finance_ops.donations WHERE id IN (100001, 100002);
DELETE FROM finance_ops.expense_categories WHERE id IN (100002, 100001);
DELETE FROM finance_ops.campaigns WHERE id IN (100001, 100002);
DELETE FROM finance_ops.donors WHERE id IN (100001, 100002);
GO

SET IDENTITY_INSERT finance_ops.donors ON;
INSERT INTO finance_ops.donors (id, full_name, national_id, phone, email, donor_type, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Donor A', N'DBG-DONOR-100001', N'09130000001', N'debug.donor.a@example.com', N'individual',   1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Donor B', N'DBG-DONOR-100002', N'09130000002', N'debug.donor.b@example.com', N'organization', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT finance_ops.donors OFF;
GO

SET IDENTITY_INSERT finance_ops.campaigns ON;
INSERT INTO finance_ops.campaigns (id, title, description, target_amount, start_date, end_date, status, created_at, updated_at)
VALUES
(100001, N'DEBUG Campaign A', N'Debug campaign for donation trace A', 50000000.00, '2026-06-01', '2026-06-30', N'active',  '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Campaign B', N'Debug campaign for donation trace B', 75000000.00, '2026-06-01', '2026-07-15', N'active',  '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT finance_ops.campaigns OFF;
GO

SET IDENTITY_INSERT finance_ops.donations ON;
INSERT INTO finance_ops.donations (id, donor_id, campaign_id, amount, currency, donation_type, donation_date, status, reference_code, created_at, updated_at)
VALUES
(100001, 100001, 100001, 1000000.00, 'IRR', N'online',        '2026-06-03', N'confirmed', N'DBG-DON-REF-100001', '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, 100002, 2500000.00, 'IRR', N'bank_transfer', '2026-06-04', N'confirmed', N'DBG-DON-REF-100002', '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT finance_ops.donations OFF;
GO

SET IDENTITY_INSERT finance_ops.expense_categories ON;
INSERT INTO finance_ops.expense_categories (id, name, parent_id, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Education Costs', NULL,   1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Therapy Costs',   100001, 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT finance_ops.expense_categories OFF;
GO

SET IDENTITY_INSERT finance_ops.expenses ON;
INSERT INTO finance_ops.expenses (id, center_id, child_id, category_id, amount, currency, expense_date, description, approved_by_user_id, status, created_at, updated_at)
VALUES
(100001, 100001, 100001, 100001, 300000.00, 'IRR', '2026-06-03', N'Debug expense A for child 100001', 100001, N'approved', '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, 100002, 100002, 450000.00, 'IRR', '2026-06-04', N'Debug expense B for child 100002', 100002, N'approved', '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT finance_ops.expenses OFF;
GO

SET IDENTITY_INSERT finance_ops.payments ON;
INSERT INTO finance_ops.payments (id, payment_type, teacher_id, center_id, amount, currency, payment_date, status, created_at, updated_at)
VALUES
(100001, N'salary', 100001, 100001, 12000000.00, 'IRR', '2026-06-05', N'paid',     '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'bonus',  100002, 100002,  2000000.00, 'IRR', '2026-06-06', N'approved', '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT finance_ops.payments OFF;
GO

SET IDENTITY_INSERT finance_ops.budget_allocations ON;
INSERT INTO finance_ops.budget_allocations (id, source_type, source_id, center_id, child_id, category_id, allocated_amount, allocation_date, reason, created_at)
VALUES
(100001, N'donation', 100001, 100001, 100001, 100001, 700000.00,  '2026-06-03', N'Debug allocation from donation 100001', '2026-06-01 08:00:00'),
(100002, N'donation', 100002, 100002, 100002, 100002, 1200000.00, '2026-06-04', N'Debug allocation from donation 100002', '2026-06-01 08:00:00');
SET IDENTITY_INSERT finance_ops.budget_allocations OFF;
GO

SET IDENTITY_INSERT finance_ops.financial_transactions ON;
INSERT INTO finance_ops.financial_transactions (id, entity_type, entity_id, transaction_type, amount, transaction_date, created_at)
VALUES
(100001, N'donation', 100001, N'credit', 1000000.00, '2026-06-03', '2026-06-01 08:00:00'),
(100002, N'expense',  100001, N'debit',   300000.00, '2026-06-03', '2026-06-01 08:00:00');
SET IDENTITY_INSERT finance_ops.financial_transactions OFF;
GO

SET IDENTITY_INSERT finance_ops.currency_rates ON;
INSERT INTO finance_ops.currency_rates (id, from_currency, to_currency, rate, rate_date)
VALUES
(100001, 'IRR', 'IRR', 1.00000000, '2026-06-03'),
(100002, 'USD', 'IRR', 420000.00000000, '2026-06-03');
SET IDENTITY_INSERT finance_ops.currency_rates OFF;
GO

PRINT 'Debug Finance Ops seed data inserted: 2 fixed rows per table.';
GO
