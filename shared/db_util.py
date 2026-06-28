#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

import mysql.connector


def db_open():
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        option_groups=["client"],
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
    return conn, cursor


def db_close(conn, cursor, success=True):
    try:
        if conn and conn.in_transaction:
            if success:
                conn.commit()
            else:
                print("Rolling back transaction...", file=sys.stderr)
                conn.rollback()
    except mysql.connector.Error as e:
        print(f"MySQL Error during commit/rollback: {e}", file=sys.stderr)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def truncate_table(cursor, table_name: str) -> None:
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        print(f"Table {table_name} truncated.", file=sys.stderr)

    except mysql.connector.Error as e:
        print(f"MySQL Error during truncation of {table_name}: {e}", file=sys.stderr)
        raise
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")


if __name__ == "__main__":
    conn = None
    cursor = None
    success = False
    try:
        conn, cursor = db_open()
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        print(f"DB Connection Test Result: {result['test']}")
        success = True
    except Exception as e:
        print(f"Error during DB session test: {e}", file=sys.stderr)
        raise
    finally:
        if conn or cursor:
            db_close(conn, cursor, success=success)

# __END__
