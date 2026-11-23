"""Silver Transform: reads the merged table, drops soft-deleted rows, converts types, standardises text,
validates business rules and foreign keys, masks PII, and splits valid rows from rejected rows.

Output: valid rows -> lake/silver/<entity> (this is the Silver Transform output), rejected rows with a
reason -> lake/quarantine/silver_<entity>. Nothing clear-text leaves this stage.
"""
