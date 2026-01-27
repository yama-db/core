#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

parser = ArgumentParser(description="統一POIテーブルを初期化")
parser.add_argument("-c", "--csv_file", help="ジオメトリ情報を含むCSVファイル・パス")
args = parser.parse_args()
csv_file = args.csv_file
table_name = "unified_pois"

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        database="anineco_tozan",
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

writer = csv.DictWriter(sys.stdout, fieldnames=["id", "name", "lat", "lon", "alt"])
writer.writeheader()

next_id = 1
cursor.execute(
    """
    SELECT id, s.name, lat, lon, alt
    FROM geom
    JOIN sanmei AS s USING (id)
    WHERE type = 1
    ORDER BY id ASC
    """
)
for row in cursor.fetchall():
    id = row["id"]
    name = row["name"]
    lat = row["lat"]
    lon = row["lon"]
    alt = row["alt"]
    for i in range(next_id, id):
        # ギャップを埋めるためのダミー行を出力
        writer.writerow({"id": i, "name": "", "lat": 0.0, "lon": 0.0, "alt": 0.0})
    writer.writerow(row)
    next_id = id + 1

cursor.close()
conn.close()

# __END__
