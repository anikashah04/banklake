/* Contains constructs the converter deliberately refuses to guess at. */
SEL a.customer_id, a.balance
FROM legacy_db.account a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.customer_id ORDER BY a.balance DESC) = 1;
