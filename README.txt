ETL Telecom Project
-------------------

1. Run SQL scripts in order:
   - 01. Create database.sql
   - 02. Create fact transaction table.sql
   - 03. Create error destination output.sql
   - 04. Create dim imsi.sql
   - 05. Create error source output.sql
   - 06_create_staging_and_sp.sql

2. Update process_etl.py with correct DB connection and folder paths.

3. Place CSV files into INCOMING_DIR and run:
   python process_etl.py

4. The stored procedure validates rows and inserts into fact_transaction or error tables.

Rules implemented:
- IMSI, CELL, LAC must not be NULL.
- EVENT_TS validated (dd/MM/yyyy or yyyy-MM-dd HH:mm:ss).
- Subscriber ID from dim_imsi_reference else -99999.
- TAC = first 8 chars of IMEI (if >=14 chars) else -99999.
- IMEI left 14 if valid else -99999.
- SNR = right 6 of IMEI if valid else -99999.

5. Processed files go to PROCESSED_DIR. Failed files go to FAILED_DIR.
