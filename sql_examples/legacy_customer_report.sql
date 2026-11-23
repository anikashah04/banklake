/* Simple Teradata-style listing: upper-case names and a business schema. */
SELECT CUSTOMER_ID, CURRENT_DATE AS REPORT_DATE
FROM BANKING.CUSTOMER_MASTER
ORDER BY CUSTOMER_ID;
