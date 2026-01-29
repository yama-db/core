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
parser.add_argument("table_name", help="登録先のテーブル名")
parser.add_argument(
    "-r",
    "--radius",
    type=int,
    default=50,
    help="検索半径（メートル単位、デフォルト: 50m）",
)
args = parser.parse_args()
table_name = args.table_name

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
        raw_remote_id = row["raw_remote_id"]
        cursor.execute(
            f"""
            SELECT source_uuid, names_json
            FROM {table_name}
            WHERE raw_remote_id = %s
            LIMIT 1
            """,
            (raw_remote_id,),
        )
        result = cursor.fetchone()
        if not result:
            # source_uuidがない場合、位置情報から検索
            lon = row["lon"]
            lat = row["lat"]
            if not (lon and lat):
                print(f"result not found for {name} (missing coordinates)")
                continue
            assert (
                abs(float(lon)) <= 180.0
            ), f"Error: Out of range longitude value: {lon}"
            assert abs(float(lat)) <= 90.0, f"Error: Out of range latitude value: {lat}"
            coord = f"POINT({lon} {lat})"

            cursor.execute(
                f"""
                SELECT
                    source_uuid,
                    names_json,
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
                (coord, coord, args.radius),
            )
            result = cursor.fetchone()
            if not result:
                print(f"result not found for {name} ({lat}, {lon})")
                continue

        result_uuid = result["source_uuid"]
        names_json = json.loads(result["names_json"])
        name = row["name"]
        kana = row["kana"]
        alias = {"name": name, "kana": kana}
        if alias in names_json:
            print(f"Alias already exists for: {name} -> {names_json}")
            continue
        names_json.append(alias)
        updated_names_json = json.dumps(names_json, ensure_ascii=False)
        cursor.execute(
            f"UPDATE {table_name} SET names_json = %s WHERE source_uuid = %s",
            (updated_names_json, result_uuid),
        )
        print(f"Registered alias for: {name} -> {names_json}")

cursor.close()
conn.close()

# __END__
