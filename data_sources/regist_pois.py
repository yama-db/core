#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# POI CSVファイルをDBに登録

import csv
import os
import sys
from argparse import ArgumentParser
from uuid import UUID

import mysql.connector
from dotenv import load_dotenv

# DB接続パラメータの読み込み
load_dotenv()
db_params = {
    "host": os.environ.get("DB_HOST"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASS"),
    "database": os.environ.get("DB_NAME"),
}

# コマンドライン引数の解析
parser = ArgumentParser(description="POIのCSVファイルをDBに登録")
parser.add_argument("csv_file", help="POIのCSVファイル・パス")
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument(
    "-c",
    "--count",
    type=int,
    default=100000,
    help="一括登録する行数の上限 (デフォルト: 100000)",
)
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()

# MySQL接続の確立
try:
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

# テーブルを空にする
if args.truncate:
    try:
        cursor.execute(f"TRUNCATE TABLE {args.table_name}")
        conn.commit()
        print(f"Table {args.table_name} truncated.")
    except mysql.connector.Error as err:
        print(f"MySQL Error during truncation: {err}")
        sys.exit(1)

# データ挿入用SQL文
sql = f"""
INSERT INTO {args.table_name} (
    source_uuid,
    raw_remote_id,
    names_json,
    geom,
    elevation_m,
    poi_type_raw,
    last_updated_at
) VALUES (
    %s, %s, %s, ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s, %s, %s
)
"""

count = 0
data = []


def insert_poi_data():
    try:
        cursor.executemany(sql, data)
        conn.commit()
        print(f"Inserted {count} rows into {args.table_name}")
    except mysql.connector.Error as err:
        print(f"MySQL Error during insertion: {err}")
        conn.rollback()
        sys.exit(1)


# CSVファイルの読み込み
with open(args.csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        source_uuid = row["source_uuid"]
        if not source_uuid:
            continue
        uuid = UUID(source_uuid)
        lon = float(row["lon"])
        lat = float(row["lat"])
        assert abs(lon) <= 180.0, "Error: Out of range longitude value"
        assert abs(lat) <= 90.0, "Error: Out of range latitude value"
        name = row["name"]
        kana = row["kana"]
        data.append(
            (
                uuid.bytes,
                row["raw_remote_id"],
                f'[{{"name": "{name}", "kana": "{kana}"}}]',
                f"POINT({lon} {lat})",
                row["elevation_m"] or None,
                row["poi_type_raw"],
                row["last_updated_at"] or None,
            )
        )
        count += 1
        if count % args.count == 0:
            insert_poi_data()
            data = []

if data:
    insert_poi_data()
cursor.close()
conn.close()

# __END__
