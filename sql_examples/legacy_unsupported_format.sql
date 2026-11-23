/* FORMAT phrase and INITCAP have no safe automatic translation. */
SEL INITCAP(c.customer_name) AS name, c.created_at (FORMAT 'YYYY-MM-DD') AS created
FROM legacy_db.customer c;
