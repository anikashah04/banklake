/* Uses ADD_MONTHS and INDEX - both need dialect translation. */
SEL  a.account_type,
     COUNT(*)                         AS n_accounts,
     ZEROIFNULL(SUM(a.balance))       AS total_balance,
     SUM(CASE WHEN INDEX(c.city, 'a') > 0 THEN 1 ELSE 0 END) AS cities_with_a
FROM legacy_db.account a
JOIN legacy_db.customer c ON a.customer_id = c.customer_id
WHERE a.opened_date >= ADD_MONTHS(DATE '2026-06-30', -24)
GROUP BY a.account_type
ORDER BY a.account_type;
