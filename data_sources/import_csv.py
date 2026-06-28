#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from argparse import ArgumentParser

from shared import db_util

# コマンドライン引数の解析
parser = ArgumentParser(description="CSVファイルをDBに登録")
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument("csv_file", help="CSVファイル・パス")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()
csv_file = args.csv_file
table_name = args.table_name
truncate = args.truncate

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    # テーブルを空にする
    if truncate:
        db_util.truncate_table(cursor, table_name)

    # CSVファイルの読み込み
    with open(args.csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        values = [tuple(row.values()) for row in reader]

    columns = ",".join([f"`{name}`" for name in fieldnames])
    placeholders = ",".join(["%s"] * len(fieldnames))
    cursor.executemany(
        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values
    )
    print(f"{cursor.rowcount} rows inserted into {table_name}.")
    success = True

except Exception as err:
    print(f"Error during DB session: {err}")
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
