#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from argparse import ArgumentParser

from shared import db_util

# コマンドライン引数の解析
parser = ArgumentParser(description="行政区域境界のGeoJSONファイルをDBに登録")
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument("geojson_file", help="行政区域境界のGeoJSONファイル・パス")
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
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    # テーブルを空にする
    if truncate:
        db_util.truncate_table(cursor, table_name)

    with open(args.geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    values = []
    count = 0
    for feature in data["features"]:
        properties = feature["properties"]
        jis_code = properties["N03_007"]
        geometry = feature["geometry"]
        geometry_type = geometry["type"]
        if geometry_type == "MultiPolygon":
            for coordinates in geometry["coordinates"]:
                geometry_json = json.dumps({
                    "type": "Polygon",
                    "coordinates": coordinates,
                })
                values.append((jis_code, geometry_json))
                count += 1
        elif geometry_type == "Polygon":
            geometry_json = json.dumps(geometry)
            values.append((jis_code, geometry_json))
            count += 1
        else:
            continue

        if len(values) >= max_count:
            cursor.executemany(
                f"""
                INSERT IGNORE INTO `{table_name}` (jis_code, geom) VALUES
                (%s, ST_GeomFromGeoJSON(%s))
                """,
                values,
            )
            print(f"Inserted {count} rows into {table_name}")
            values = []

    if values:
        cursor.executemany(
            f"""
            INSERT IGNORE INTO `{table_name}` (jis_code, geom) VALUES
            (%s, ST_GeomFromGeoJSON(%s))
            """,
            values,
        )
        print(f"Inserted {count} rows into {table_name}")

    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
