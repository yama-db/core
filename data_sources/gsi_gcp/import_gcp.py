#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# POI CSVファイルをDBに登録

import csv
import sys
from argparse import ArgumentParser
from pathlib import Path

from shared import db_util, generate_source_uuid, tile_utils

# コマンドライン引数の解析
parser = ArgumentParser(description="POIのTSVファイルをDBに登録")
parser.add_argument(
    "table_name", choices=["stg_gsi_gcp_pois"], help="登録先のテーブル名"
)
parser.add_argument("tsv_file", help="POIのTSVファイル・パス")
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
tsv_file = args.tsv_file
table_name = args.table_name
max_count = args.max_count
truncate = args.truncate

query_insert = f"""
    INSERT INTO `{table_name}` (
        source_uuid, raw_id, raw_type, names_json,
        geom, elevation, last_updated_at, grade
    ) VALUES (
        %s, %s, %s, %s,
        ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        names_json = JSON_MERGE_PRESERVE(names_json, VALUES(names_json))
"""

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    # テーブルを空にする
    if truncate:
        db_util.truncate_table(cursor, table_name)

    # TSVファイルの読み込み
    with open(tsv_file, "r", encoding="utf-8-sig") as f:
        suffix = Path(tsv_file).suffix.lower()
        reader = csv.DictReader(f, delimiter="\t")
        count = 0
        values = []
        for row in reader:
            raw_id = row["raw_id"]
            if not raw_id:  # 別名はスキップ
                continue
            uuid = generate_source_uuid(table_name, raw_id)
            lon = row["lon"]
            lat = row["lat"]
            if lon and lat:
                coord = f"POINT({lon} {lat})"
            else:
                coord = None
            value = (
                uuid.bytes,
                raw_id,
                row["raw_type"],
                row["names_json"],
                coord,
                row["elevation"] or None,
                row["last_updated_at"] or None,
                row["grade"] or None,
            )
            values.append(value)
            count += 1
            if count % max_count == 0:
                cursor.executemany(query_insert, values)
                print(f"Inserted {count} rows into {table_name}", file=sys.stderr)
                values = []

    if values:
        cursor.executemany(query_insert, values)
        print(f"Inserted {count} rows into {table_name}", file=sys.stderr)
    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
