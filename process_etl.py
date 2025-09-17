#!/usr/bin/env python3
"""ETL script for Telecom CSV files."""

import os, csv, pyodbc, shutil
from datetime import datetime

INCOMING_DIR = r"C:\ETL\incoming"
PROCESSED_DIR = r"C:\ETL\processed"
FAILED_DIR = r"C:\ETL\failed"
BATCH_SIZE = 1000

DATABASE_CONNECTION = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=YOUR_SQL_SERVER_INSTANCE;"
    "DATABASE=SSIS_Telecom_DB;"
    "UID=YOUR_USERNAME;"
    "PWD=YOUR_PASSWORD;"
)

def ensure_dirs():
    for d in (INCOMING_DIR, PROCESSED_DIR, FAILED_DIR):
        os.makedirs(d, exist_ok=True)

def connect():
    return pyodbc.connect(DATABASE_CONNECTION, autocommit=True)

def bulk_insert_rows(cursor, rows, source_file_name):
    sql = """INSERT INTO stg_telecom_raw (source_file_name, ID, IMSI, IMEI, CELL, LAC, EVENT_TYPE, EVENT_TS)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
    params = []
    for r in rows:
        params.append((
            source_file_name,
            int(r['ID']) if r['ID'] else None,
            r['IMSI'] or None,
            r['IMEI'] or None,
            int(r['CELL']) if r['CELL'] else None,
            int(r['LAC']) if r['LAC'] else None,
            r['EVENT_TYPE'] or None,
            r['EVENT_TS'] or None
        ))
    cursor.fast_executemany=True
    cursor.executemany(sql, params)

def load_csv_to_staging(conn, file_path, source_file_name):
    cursor = conn.cursor()
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        batch=[]
        for row in reader:
            batch.append(row)
            if len(batch)>=BATCH_SIZE:
                bulk_insert_rows(cursor,batch,source_file_name)
                batch=[]
        if batch:
            bulk_insert_rows(cursor,batch,source_file_name)

def process_file(conn, file_path):
    src = os.path.basename(file_path)
    try:
        load_csv_to_staging(conn, file_path, src)
        conn.cursor().execute("{CALL usp_ProcessStagingTelecom}")
        shutil.move(file_path, os.path.join(PROCESSED_DIR, src+datetime.now().strftime(".%Y%m%d%H%M%S")))
    except Exception as e:
        print("Error:",e)
        shutil.move(file_path, os.path.join(FAILED_DIR, src+datetime.now().strftime(".%Y%m%d%H%M%S")))

def main():
    ensure_dirs()
    conn=connect()
    try:
        for fname in os.listdir(INCOMING_DIR):
            if fname.lower().endswith('.csv'):
                process_file(conn, os.path.join(INCOMING_DIR,fname))
    finally:
        conn.close()

if __name__=="__main__":
    main()
