#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

from shared import tile_utils

parser = ArgumentParser(description="統一POIテーブルを初期化")
parser.add_argument("table_name", choices=["mountain_pois"], help="統一POIテーブル名")
parser.add_argument("csv_file", help="ジオメトリ情報を含むCSVファイル")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="テーブルを空にしてから登録"
)
args = parser.parse_args()
csv_file = args.csv_file
table_name = args.table_name
truncate = args.truncate

category_id = "mountain"
display_name = "山"

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

try:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        values = []
        for i, row in enumerate(reader):
            assert int(row["id"]) == i + 1, f"IDが連番でない: {row['id']}"
            is_used = row["is_used"]
            name = row["name"]
            kana = row["kana"]
            lat = row["lat"]
            lon = row["lon"]
            if lon and lat:
                x_z18, y_z18 = tile_utils.lnglat_to_tile(float(lon), float(lat), 18)
            else:
                x_z18, y_z18 = (None, None)
            alt = row["alt"]
            values.append(
                (is_used, name, kana, f"POINT({lon} {lat})", alt, x_z18, y_z18)
            )

    cursor.executemany(
        f"""
        INSERT INTO {table_name} (
            is_used, main_name, main_kana, geom, elevation, x_z18, y_z18
        ) VALUES (
            %s, %s, %s,
            ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s, %s, %s
        )
        """,
        values,
    )
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into {table_name}")

except FileNotFoundError:
    print(f"CSVファイルが見つかりません: {csv_file}")
except mysql.connector.Error as e:
    print(f"MySQL Error during insertion: {e}")
    conn.rollback()

cursor.close()
conn.close()

# __END__
