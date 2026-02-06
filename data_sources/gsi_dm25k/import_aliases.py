#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 別名をDBに登録

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

# コマンドライン引数の解析
parser = ArgumentParser(description="POIのCSVファイルをDBに登録")
parser.add_argument("csv_file", help="POIのCSVファイル・パス")
parser.add_argument(
    "table_name", choices=["stg_gsi_dm25k_pois"], help="登録先のテーブル名"
)
parser.add_argument(
    "-r",
    "--radius",
    type=int,
    default=50,
    help="検索半径（メートル単位、デフォルト: 50m）",
)
args = parser.parse_args()
table_name = args.table_name
radius = args.radius

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        autocommit=True,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

with open(args.csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        source_uuid = row["source_uuid"]
        if source_uuid:
            continue
        alias_name = row["name"]
        alias_kana = row["kana"]
        alias = json.dumps({"name": alias_name, "kana": alias_kana}, ensure_ascii=False)

        # 位置情報から検索
        lon = row["lon"]
        lat = row["lat"]
        if not (lon and lat):
            print(f"Skipping {alias_name} due to missing coordinates")
            continue
        assert abs(float(lon)) <= 180.0, f"Error: Out of range longitude value: {lon}"
        assert abs(float(lat)) <= 90.0, f"Error: Out of range latitude value: {lat}"
        coord = f"POINT({lon} {lat})"
        cursor.execute(
            f"""
            SELECT
                source_uuid,
                names_json->>'$[0].name' AS name,
                ST_Distance_Sphere(
                    geom,
                    ST_GeomFromText(%s, 4326, "axis-order=long-lat")
                ) AS distance_m
            FROM {table_name}
            WHERE
                ST_Within(
                    geom,
                    ST_Buffer(
                        ST_GeomFromText(%s, 4326, "axis-order=long-lat"),
                        %s
                    )
                )
            ORDER BY distance_m ASC
            LIMIT 1
            """,
            (coord, coord, radius),
        )
        result = cursor.fetchone()
        if not result:
            print(
                f"Skipping {alias_name} ({lat}, {lon}) due to no matching result found "
            )
            continue

        result_uuid = result["source_uuid"]
        result_name = result["name"]
        cursor.execute(
            f"""
            UPDATE {table_name}
            SET names_json = JSON_ARRAY_APPEND(names_json, '$', CAST(%s AS JSON))
            WHERE source_uuid = %s
                AND NOT JSON_CONTAINS(names_json, CAST(%s AS JSON), '$')
            """,
            (alias, result_uuid, alias),
        )
        print(f"Registered alias for: {alias_name} -> {result_name}")

cursor.close()
conn.close()

# __END__
