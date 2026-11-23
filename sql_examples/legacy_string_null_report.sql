/* NVL, CHARACTERS and a FLOAT cast: function and data-type conversion. */
SEL  c.segment,
     COUNT(*)                                       AS n_customers,
     SUM(CHARACTERS(NVL(c.city, '')))               AS total_city_chars,
     CAST(AVG(CHARACTERS(c.customer_name)) AS FLOAT) AS avg_name_len
FROM BANKING.CUSTOMER_MASTER c
GROUP BY c.segment
ORDER BY c.segment;
