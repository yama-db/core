#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

EPS = 40  # 位置の許容誤差[m]

parser = ArgumentParser(description="Unified POI住所情報設定スクリプト")
parser.add_argument("table_name", choices=["poi_address_map"], help="対象テーブル名")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="対象テーブルを初期化"
)
args = parser.parse_args()
table_name = args.table_name
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

if truncate:
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        print(f"Table {table_name} truncated.")
    except mysql.connector.Error as e:
        print(f"MySQL Error during truncate: {e}")
        sys.exit(1)

cursor.execute(
    "SELECT id FROM unified_pois WHERE display_lat != 0 AND display_lon != 0"
)
address_text_data = []
jis_code_data = []
count = 0
for row in cursor.fetchall():
    id = row["id"]
    cursor.execute(
        """
        SELECT 
            GROUP_CONCAT(DISTINCT t.pref_name ORDER BY t.jis_code SEPARATOR '・') AS prefs,
            JSON_ARRAYAGG(t.jis_code) AS jis_codes_json
        FROM (
            SELECT DISTINCT r.pref_name, r.jis_code
            FROM unified_pois AS u
            JOIN administrative_boundaries AS b ON ST_Intersects(b.geom, ST_Buffer(u.representative_geom, %s))
            JOIN administrative_regions AS r USING (jis_code)
            WHERE u.id = %s
            ORDER BY r.jis_code
        ) AS t
        """,
        (EPS, id),
    )
    result = cursor.fetchone()
    if result:
        address_text_data.append((result["prefs"], id))
        for jis_code in json.loads(result["jis_codes_json"]):
            jis_code_data.append((id, jis_code))
    else:
        print(f"ID {id} の住所情報が見つかりません。")
    count += 1
    if count % 1000 == 0:
        print(f"{count} 件処理中...")

if address_text_data:
    cursor.executemany(
        "UPDATE unified_pois SET address_text = %s WHERE id = %s", address_text_data
    )
    conn.commit()
if jis_code_data:
    cursor.executemany(
        f"INSERT INTO {table_name} (unified_poi_id, jis_code) VALUES (%s, %s)",
        jis_code_data,
    )
    conn.commit()

print(f"{count} 件の住所情報を設定しました。")

cursor.close()
conn.close()

# __END__
