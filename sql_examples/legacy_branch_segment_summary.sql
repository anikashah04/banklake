/* Fictional Teradata-style report written from scratch for BankLake. */
SEL  b.region,
     c.segment,
     COUNT(*)                              AS n_accounts,
     ZEROIFNULL(SUM(a.balance))            AS total_balance,
     CAST(AVG(a.balance) AS DECIMAL(18,2)) AS avg_balance
FROM legacy_db.account a
JOIN legacy_db.customer c ON a.customer_id = c.customer_id
JOIN legacy_db.branch   b ON a.branch_id   = b.branch_id
WHERE a.opened_date <= DATE '2026-06-30'
  AND a.account_type = 'SAVINGS'
GROUP BY b.region, c.segment
ORDER BY b.region, c.segment;
