/* SEL TOP n becomes LIMIT n. */
SEL TOP 5 c.customer_id, ZEROIFNULL(SUM(a.balance)) AS total_balance
FROM legacy_db.account a
JOIN legacy_db.customer c ON a.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY total_balance DESC, c.customer_id;
