/* CSUM is a Teradata cumulative-sum function; Athena needs a window function. */
SEL  t.m,
     t.monthly_amount,
     CSUM(t.monthly_amount, t.m) AS running_amount
FROM (SEL EXTRACT(YEAR FROM transaction_date) * 100 + EXTRACT(MONTH FROM transaction_date) AS m,
             SUM(amount) AS monthly_amount
      FROM legacy_db.transaction
      WHERE transaction_date IS NOT NULL
      GROUP BY 1) t
ORDER BY t.m;
