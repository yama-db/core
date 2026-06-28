#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from argparse import ArgumentParser

from shared import config, db_util

# コマンドライン引数の解析
parser = ArgumentParser(description="Unified POI住所情報設定スクリプト")
parser.add_argument("table_name", choices=["poi_address_map"], help="対象テーブル名")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="対象テーブルを初期化"
)
args = parser.parse_args()
table_name = args.table_name
truncate = args.truncate

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    if truncate:
        db_util.truncate_table(cursor, table_name)

    jis_code_data = []
    count = 0

    cursor.execute(
        """
        SELECT m.id
        FROM mountain_pois AS m
        LEFT JOIN poi_address_map AS p ON m.id = p.mountain_id
        WHERE m.is_used AND p.mountain_id IS NULL
        """,
    )
    for row in cursor.fetchall():
        id = row["id"]
        cursor.execute(
            """
            SELECT 
                JSON_ARRAYAGG(t.jis_code) AS jis_codes_json
            FROM (
                SELECT DISTINCT r.jis_code
                FROM mountain_pois AS m
                JOIN administrative_boundaries AS b ON ST_Intersects(b.geom, ST_Buffer(m.geom, %s))
                JOIN administrative_regions AS r USING (jis_code)
                WHERE m.id = %s
                ORDER BY r.jis_code
            ) AS t
            """,
            (config.EPS, id),
        )
        result = cursor.fetchone()
        if result:
            for jis_code in json.loads(result["jis_codes_json"]):
                jis_code_data.append((id, jis_code))
        else:
            print(f"ID {id} の住所情報が見つかりません。")
        count += 1
        if count % 1000 == 0:
            print(f"{count} 件処理中...")

    if jis_code_data:
        cursor.executemany(
            f"INSERT INTO {table_name} (mountain_id, jis_code) VALUES (%s, %s)",
            jis_code_data,
        )

    print(f"{count} 件の住所情報を設定しました。")
    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
