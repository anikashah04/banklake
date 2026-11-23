# Gold lineage

Silver to Gold, table by table and column by column. Table-level dependencies come from the pipeline metadata; column-level lineage is checked against the real tables by `tests/test_lineage.py`.

Full path of every Gold column: raw file -> Bronze -> Silver Merge (CDC) -> Silver Transform (clean, validate; still masked) -> Silver -> Gold (customer PII unmasked).

## gold.customer_360
Grain: one row per customer. Built from Silver: account, customer, deposit, loan, transaction.

| Gold column | How it is computed | Silver sources |
|---|---|---|
| customer_id | copied | customer.customer_id |
| customer_name | unmasked from the vault at Gold (masked in Bronze, Silver Merge and Silver Transform) | customer.customer_name |
| email | unmasked from the vault at Gold | customer.email |
| phone | unmasked from the vault at Gold | customer.phone |
| customer_name_token | copied; key into the PII vault | customer.customer_name_token |
| email_token | copied; key into the PII vault | customer.email_token |
| phone_token | copied; key into the PII vault | customer.phone_token |
| city | copied | customer.city |
| segment | copied | customer.segment |
| n_accounts | COUNT of the customer's accounts (0 if none) | account.account_id, account.customer_id |
| total_balance | SUM(balance) over the customer's accounts (0 if none) | account.balance, account.customer_id |
| total_deposits | SUM(deposit_amount) over the customer's deposits (0 if none) | deposit.deposit_amount, deposit.customer_id |
| total_loans | SUM(loan_amount) over the customer's loans (0 if none) | loan.loan_amount, loan.customer_id |
| n_transactions | COUNT of transactions on the customer's accounts (transaction JOIN account) | transaction.transaction_id, transaction.account_id, account.account_id, account.customer_id |
| total_txn_amount | SUM(amount) of those transactions (0 if none) | transaction.amount, transaction.account_id, account.customer_id |
| customer_token | salted SHA-256 pseudonym of customer_id | customer.customer_id |

## gold.branch_performance
Grain: one row per branch. Built from Silver: account, branch, transaction.

| Gold column | How it is computed | Silver sources |
|---|---|---|
| branch_id | copied | branch.branch_id |
| branch_name | copied | branch.branch_name |
| region | copied | branch.region |
| n_accounts | COUNT of accounts at the branch (0 if none) | account.account_id, account.branch_id |
| n_active_customers | COUNT DISTINCT customer_id over the branch's accounts | account.customer_id, account.branch_id |
| total_balance | SUM(balance) at the branch (0 if none) | account.balance, account.branch_id |
| avg_balance | AVG(balance) as DECIMAL(18,2); NULL if no accounts | account.balance, account.branch_id |
| n_transactions | COUNT of transactions on the branch's accounts | transaction.transaction_id, transaction.account_id, account.branch_id |
| txn_amount | SUM(amount) of those transactions (0 if none) | transaction.amount, transaction.account_id, account.branch_id |

## gold.deposit_summary
Grain: one row per customer segment and maturity month. Built from Silver: customer, deposit.

| Gold column | How it is computed | Silver sources |
|---|---|---|
| segment | customer's segment (deposit JOIN customer) | customer.segment, customer.customer_id, deposit.customer_id |
| maturity_year | YEAR(maturity_date) | deposit.maturity_date |
| maturity_month | maturity_date formatted yyyy-MM | deposit.maturity_date |
| n_deposits | COUNT of deposits in the group | deposit.deposit_id |
| total_deposits | SUM(deposit_amount) | deposit.deposit_amount |
| avg_deposit | AVG(deposit_amount) as DECIMAL(18,2) | deposit.deposit_amount |

## gold.loan_summary
Grain: one row per customer segment. Built from Silver: customer, loan.

| Gold column | How it is computed | Silver sources |
|---|---|---|
| segment | customer's segment (loan JOIN customer) | customer.segment, customer.customer_id, loan.customer_id |
| n_loans | COUNT of loans in the segment | loan.loan_id |
| total_loan_amount | SUM(loan_amount) | loan.loan_amount |
| avg_interest_rate | AVG(interest_rate) as DECIMAL(5,2) | loan.interest_rate |

## gold.transaction_summary
Grain: one row per month and transaction type. Built from Silver: transaction.

| Gold column | How it is computed | Silver sources |
|---|---|---|
| txn_month | transaction_date formatted yyyy-MM | transaction.transaction_date |
| txn_type | copied (grouping key) | transaction.txn_type |
| n_transactions | COUNT of transactions in the group | transaction.transaction_id |
| total_amount | SUM(amount) | transaction.amount |
