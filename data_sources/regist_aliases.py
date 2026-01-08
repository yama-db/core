#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 別名をDBに登録

import csv
import json
import os
import sys
from argparse import ArgumentParser

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
    "-r",
    "--radius",
    type=int,
    default=50,
    help="検索半径（メートル単位、デフォルト: 50m）",
)
args = parser.parse_args()

# MySQL接続の確立
try:
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

query = f"""
SELECT
    source_uuid,
    names_json,
    ST_Distance_Sphere(geom, ST_GeomFromText(%s, 4326, "axis-order=long-lat")) AS distance_m
FROM {args.table_name}
WHERE
    ST_Within(
        geom,
        ST_Buffer(ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s)
    )
ORDER BY distance_m ASC
LIMIT 1
"""
update_sql = f"""
UPDATE {args.table_name}
SET names_json = %s
WHERE source_uuid = %s
"""

with open(args.csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        source_uuid = row["source_uuid"]
        if source_uuid:
            continue
        lon = float(row["lon"])
        lat = float(row["lat"])
        assert abs(lon) <= 180.0, "Error: Out of range longitude value"
        assert abs(lat) <= 90.0, "Error: Out of range latitude value"
        name = row["name"]
        kana = row["kana"]
        pt = f"POINT({lon} {lat})"
        cursor.execute(query, (pt, pt, args.radius))
        result = cursor.fetchone()
        if not result:
            print(f"result not found for: {name} ({pt})")
            continue
        result_uuid = result["source_uuid"]
        names_json = result["names_json"]
        if isinstance(names_json, str):
            names_json = json.loads(names_json)
        alias = {"name": name, "kana": kana}
        if alias in names_json:
            print(f"Alias already exists for: {name} -> {names_json}")
            continue
        names_json.append(alias)
        updated_names_json = json.dumps(names_json, ensure_ascii=False)
        cursor.execute(update_sql, (updated_names_json, result_uuid))
        conn.commit()
        print(f"Registered alias for: {name} -> {names_json}")

cursor.close()
conn.close()

# __END__
