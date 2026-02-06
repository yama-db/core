#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

# コマンドライン引数の解析
parser = ArgumentParser(description="CSVファイルをDBに登録")
parser.add_argument("csv_file", help="CSVファイル・パス")
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()
csv_file = args.csv_file
table_name = args.table_name
truncate = args.truncate

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

# テーブルを空にする
if truncate:
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print(f"Table {table_name} truncated.")
    except mysql.connector.Error as err:
        print(f"MySQL Error during truncation: {err}")
        sys.exit(1)

# CSVファイルの読み込み
with open(args.csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    values = [tuple(row.values()) for row in reader]

try:
    columns = ",".join([f"`{name}`" for name in fieldnames])
    placeholders = ",".join(["%s"] * len(fieldnames))
    cursor.executemany(
        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values
    )
    conn.commit()
    print(f"{cursor.rowcount} rows inserted into {table_name}.")
except mysql.connector.Error as err:
    print(f"MySQL Error during insert: {err}")
    conn.rollback()

cursor.close()
conn.close()

# __END__
