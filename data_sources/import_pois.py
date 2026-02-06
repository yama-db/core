#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# POI CSVファイルをDBに登録

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path
from uuid import UUID

import mysql.connector

# コマンドライン引数の解析
parser = ArgumentParser(description="POIのCSVファイルをDBに登録")
parser.add_argument("csv_file", help="POIのCSVファイル・パス")
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument(
    "-m",
    "--max-count",
    type=int,
    default=100000,
    help="一括登録する行数の上限 (デフォルト: 100000)",
)
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()
csv_file = args.csv_file
table_name = args.table_name
max_count = args.max_count
truncate = args.truncate

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
    sys.exit(1)

# テーブルを空にする
if truncate:
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        print(f"Table {table_name} truncated.")
    except mysql.connector.Error as e:
        print(f"MySQL Error during truncation: {e}")
        sys.exit(1)


def insert_poi_data(values):
    try:
        cursor.executemany(
            f"""
            INSERT INTO {table_name} (
                source_uuid, raw_remote_id, names_json,
                geom, elevation_m, poi_type_raw, last_updated_at
            ) VALUES (
                %s, %s, %s,
                ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                names_json = JSON_MERGE_PRESERVE(names_json, VALUES(names_json))
            """,
            values,
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during insertion: {e}")
        conn.rollback()
        sys.exit(1)


# CSVファイルの読み込み
try:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        suffix = Path(csv_file).suffix.lower()
        delimiter = "\t" if suffix == ".tsv" else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        count = 0
        values = []
        for row in reader:
            source_uuid = row["source_uuid"]
            if not source_uuid:  # 別名はスキップ
                continue
            uuid = UUID(source_uuid)
            name = row["name"]
            kana = row["kana"]
            names_json = json.dumps([{"name": name, "kana": kana}], ensure_ascii=False)
            lon = row["lon"]
            lat = row["lat"]
            coord = f"POINT({lon} {lat})" if lon and lat else None
            values.append(
                (
                    uuid.bytes,
                    row["raw_remote_id"],
                    names_json,
                    coord,
                    row["elevation_m"] or None,
                    row["poi_type_raw"],
                    row["last_updated_at"] or None,
                )
            )
            count += 1
            if count % max_count == 0:
                insert_poi_data(values)
                print(f"Inserted {count} rows into {table_name}")
                values = []

    if values:
        insert_poi_data(values)
        print(f"Inserted {count} rows into {table_name}")

except FileNotFoundError:
    print(f"File not found: {csv_file}")
except csv.Error as e:
    print(f"CSV Error: {e}")

cursor.close()
conn.close()

# __END__
