#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 書籍のPOIファイルを stg_book_pois テーブルに登録

import csv
import json
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

from shared import extract_aliases, generate_source_uuid

# コマンドライン引数の解析
parser = ArgumentParser(description="書籍のCSVファイルをDBに登録")
parser.add_argument("table_name", help="登録先テーブル名")
parser.add_argument("source_id", help="情報源ID")
parser.add_argument("csv_file", help="書籍のCSVファイル・パス")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()
table_name = args.table_name
source_id = int(args.source_id)
csv_file = args.csv_file
truncate = args.truncate

if os.path.isfile(csv_file) and os.path.getsize(csv_file) == 0:
    print(f"空のファイルのため処理をスキップします: {csv_file}")
    sys.exit(0)

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        option_groups=["client"],
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
    sys.exit(1)

# 情報源の正式名称とNDL書誌IDを取得
cursor.execute(
    "SELECT info_type, display_name, ndl_id FROM information_sources WHERE id = %s",
    (source_id,),
)
result = cursor.fetchone()
if not result:
    print(f"No book found with source_id {source_id}")
    sys.exit(1)
if result["info_type"] != "BOOK":
    print(f"Source ID {source_id} is not a BOOK type")
    sys.exit(1)
display_name = result["display_name"]
ndl_id = result["ndl_id"]
print(f"Processing book: {display_name} (NDL{ndl_id})")

# 指定された情報源IDのデータを削除
if truncate:
    try:
        cursor.execute(f"DELETE FROM {table_name} WHERE source_id = %s", (source_id,))
        conn.commit()
        print(f"Table {table_name} truncated for source_id {source_id}.")
    except mysql.connector.Error as e:
        print(f"MySQL Error during truncation: {e}")
        sys.exit(1)

# CSVファイルの読み込み
with open(csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    values = []
    for row in reader:
        raw_id = row["raw_id"]
        raw_type = "山"  # FIXME:書籍のPOIはすべて山として扱う
        uuid = generate_source_uuid(f"NDL{ndl_id}_poi", raw_id)
        aliases = extract_aliases(row["name"], row["kana"])
        data = [{"name": name, "kana": kana} for name, kana in aliases]
        names_json = json.dumps(data, ensure_ascii=False)
        values.append(
            (
                uuid.bytes,
                source_id,
                raw_id,
                raw_type,
                row["mountain_id"] or None,
                names_json,
                row["elevation"] or None,
            )
        )

# データの挿入
try:
    cursor.executemany(
        f"""
        INSERT INTO {table_name} (
            source_uuid, source_id, raw_id, raw_type, mountain_id, names_json, elevation
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        """,
        values,
    )
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into {table_name}")
except mysql.connector.Error as e:
    print(f"MySQL Error during insertion: {e}")
    conn.rollback()

# MySQL接続のクローズ
cursor.close()
conn.close()

# __END__
