#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 書籍のPOIファイルを stg_book_pois のCSV形式で出力

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

from shared import generate_source_uuid

# コマンドライン引数の解析
parser = ArgumentParser(description="書籍のCSVファイルをDBに登録")
parser.add_argument("source_id", help="情報源ID")
parser.add_argument("csv_file", help="書籍のCSVファイル・パス")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
args = parser.parse_args()
source_id = args.source_id
csv_file = args.csv_file
truncate = args.truncate

table_name = "stg_book_pois"

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

# 情報源の正式名称とNDL書誌IDを取得
cursor.execute(
    "SELECT formal_title, ndl_id FROM book_details WHERE source_id = %s",
    (source_id,),
)
result = cursor.fetchone()
if not result:
    print(f"No book found with source_id {source_id}")
    sys.exit(1)
formal_title = result["formal_title"]
ndl_id = result["ndl_id"]
print(f"Processing book: {formal_title} (NDL{ndl_id})")

# 指定された情報源IDのデータを削除
if truncate:
    try:
        cursor.execute(
            f"DELETE FROM {table_name} WHERE source_id = %s", (source_id,)
        )
        conn.commit()
        print(f"Table {table_name} truncated for source_id {source_id}.")
    except mysql.connector.Error as e:
        print(f"MySQL Error during truncation: {e}")
        sys.exit(1)

# CSVファイルの読み込み
with open(csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    values = []
    for row in reader:
        raw_remote_id = row["raw_remote_id"]
        uuid = generate_source_uuid(f"NDL{ndl_id}", raw_remote_id)
        lon = 0.0
        lat = 0.0
        unified_poi_id = row["unified_poi_id"]
        if unified_poi_id:
            cursor.execute(
                """
                SELECT display_lat AS lat, display_lon AS lon 
                FROM unified_pois
                WHERE id = %s
                """,
                (unified_poi_id,),
            )
            poi = cursor.fetchone()
            if poi:
                lon = float(poi["lon"])
                lat = float(poi["lat"])
            else:
                print(
                    f"Warning: No unified_pois entry found for id {unified_poi_id}",
                    file=sys.stderr,
                )
        try:
            name = row["name"]
            kana = row["kana"]
            names_json = json.dumps([{"name": name, "kana": kana}], ensure_ascii=False)
        except Exception as e:
            print(
                f"Error encoding JSON for name={name}, kana={kana}: {e}",
                file=sys.stderr,
            )
            sys.exit(1)

        values.append(
            (
                uuid.bytes,
                raw_remote_id,
                names_json,
                f"POINT({lon} {lat})",
                row["elevation_m"] or None,
                source_id,
            )
        )

# データの挿入
try:
    cursor.executemany(
        f"""
        INSERT INTO {table_name} (
            source_uuid, raw_remote_id, names_json,
            geom, elevation_m, source_id
        ) VALUES (
            %s, %s, %s,
            ST_GeomFromText(%s, 4326, "axis-order=long-lat"), %s, %s
        )
        """,
        values
    )
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into {table_name}")
except mysql.connector.Error as e:
    print(f"MySQL Error during insertion: {e}")
    conn.rollback()

# MySQL接続のクローズ
cursor.close()
conn.close()

# __END__
