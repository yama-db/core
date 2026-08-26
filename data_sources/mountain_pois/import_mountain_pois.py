#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser

from shared import tile_utils, db_util

parser = ArgumentParser(description="統一POIテーブルを初期化")
parser.add_argument("table_name", choices=["mountain_pois"], help="統一POIテーブル名")
parser.add_argument("csv_file", help="ジオメトリ情報を含むCSVファイル")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="テーブルを空にしてから登録"
)
args = parser.parse_args()
table_name = args.table_name
csv_file = args.csv_file
truncate = args.truncate

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    if truncate:
        db_util.truncate_table(cursor, table_name)

    values = []
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if int(row["id"]) != i + 1:
                raise ValueError(f"IDが連番でない: {row['id']}")
            is_used = row["is_used"]
            name = row["name"]
            kana = row["kana"]
            lat = row["lat"]
            lon = row["lon"]
            alt = row["alt"]
            values.append(
                (is_used, name, kana, f"POINT({lon} {lat})", alt)
            )

    cursor.executemany(
        f"""
        INSERT INTO {table_name} (
            is_used, main_name, main_kana, geom, elevation
        ) VALUES (
            %s, %s, %s,
            ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s
        )
        """,
        values,
    )
    print(f"Inserted {cursor.rowcount} rows into {table_name}")
    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
