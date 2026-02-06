#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

# コマンドライン引数の解析
parser = ArgumentParser(description="行政区域境界のGeoJSONファイルをDBに登録")
parser.add_argument("geojson_file", help="行政区域境界のGeoJSONファイル・パス")
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument(
    "-m",
    "--max-count",
    type=int,
    default=5000,
    help="一括登録する行数の上限 (デフォルト: 5000)",
)
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()
geojson_file = args.geojson_file
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
    print(f"MySQL Connection Error: {e}")
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


def insert_geom_data(values):
    try:
        cursor.executemany(
            f"""
            INSERT IGNORE INTO {table_name} (jis_code, geom) VALUES
            (%s, ST_GeomFromGeoJSON(%s))
            """,
            values,
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during insertion: {e}")
        conn.rollback()


try:
    with open(args.geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    values = []
    count = 0
    for feature in data["features"]:
        properties = feature["properties"]
        jis_code = properties["N03_007"]
        geometry_json = json.dumps(feature["geometry"])
        values.append((jis_code, geometry_json))
        count += 1
        if count % max_count == 0:
            insert_geom_data(values)
            print(f"Inserted {count} rows into {table_name}")
            values = []

    if values:
        insert_geom_data(values)
        print(f"Inserted {count} rows into {table_name}")

except FileNotFoundError:
    print(f"File not found: {geojson_file}")
except json.JSONDecodeError as e:
    print(f"Error parsing JSON file: {e}")
except KeyError as e:
    print(f"Missing expected key in GeoJSON data: {e}")

cursor.close()
conn.close()

# __END__
