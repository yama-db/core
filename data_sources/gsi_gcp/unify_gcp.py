#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from argparse import ArgumentParser

from shared import config, db_util

# コマンドライン引数の解析
parser = ArgumentParser(description="GSI GCPを統合POIにリンク")
parser.add_argument(
    "table_name", choices=["stg_gsi_gcp_pois"], help="POIデータソースのテーブル名"
)
parser.add_argument(
    "-r",
    "--radius",
    type=int,
    default=config.EPS,
    help=f"バッファの半径[m] (デフォルト: {config.EPS})",
)
parser.add_argument(
    "-t",
    "--truncate",
    action="store_true",
    help="既存のリンクを削除してから処理を実行 (デフォルト: False)",
)
args = parser.parse_args()
table_name = args.table_name
radius = args.radius
truncate = args.truncate

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    cursor.execute(
        "SELECT DISTINCT id FROM information_sources WHERE source_table = %s",
        (table_name,),
    )
    if not (rows := cursor.fetchall()):
        raise ValueError(
            f"Error: source_table '{table_name}' not found in information_sources."
        )
    source_id = rows[0]["id"]

    # 既存のリンクを削除
    if truncate:
        cursor.execute("DELETE FROM poi_links WHERE source_id = %s", (source_id,))
        print(f"Existing links for {table_name} have been deleted.")

    # 現在、登録されているリンクの最大値
    cursor.execute(
        "SELECT COALESCE(MAX(mountain_id), 0) AS max_id FROM poi_links WHERE source_id = %s",
        (source_id,),
    )
    max_id = cursor.fetchone()["max_id"]
    print(f"Current max ID linked to {table_name}: {max_id}")

    cursor.execute(
        "SELECT id FROM mountain_pois WHERE is_used AND id > %s",
        (max_id,),
    )
    for row in cursor.fetchall():
        id = row["id"]
        # バッファの作成
        cursor.execute(
            """
            SELECT ST_Buffer(geom, %s) INTO @buffer
            FROM mountain_pois WHERE id = %s
            """,
            (radius, id),
        )

        # バッファ内で最も標高の高いPOIを取得
        cursor.execute(
            f"""
            SELECT source_uuid
            FROM `{table_name}`
            WHERE ST_Within(geom, @buffer)
            ORDER BY elevation DESC
            LIMIT 1
            """,
        )
        result = cursor.fetchone()
        if not result:
            print(
                f"No GCP POI found within {radius}m for mountain_id {id}",
                file=sys.stderr,
            )
            z_min = 13  # デフォルトの最小表示Zレベルを設定
            cursor.execute(
                "UPDATE mountain_pois SET z_min = %s WHERE id = %s",
                (z_min, id),
            )
            continue

        source_uuid = result["source_uuid"]

        # バッファ内で最小のズームレベルを取得
        cursor.execute(
            f"""
            SELECT z_min
            FROM `{table_name}`
            WHERE ST_Within(geom, @buffer)
            ORDER BY z_min ASC
            LIMIT 1
            """,
        )
        result = cursor.fetchone()  # 少なくとも１つはある。
        z_min = result["z_min"]

        # 山岳統合POIの地理座標、標高、最小表示Zレベルを更新
        cursor.execute(
            f"""
            UPDATE mountain_pois AS target
            JOIN `{table_name}` AS source ON source.source_uuid = %s
            SET 
                target.geom = source.geom,
                target.elevation = source.elevation,
                target.z_min = %s
            WHERE target.id = %s
            """,
            (source_uuid, z_min, id),
        )

        # 統合POIと情報源を関連付ける
        cursor.execute(
            """
            INSERT INTO poi_links (mountain_id, source_id, source_uuid, linked_distance)
            VALUES (%s, %s, %s, 0)
            """,
            (id, source_id, source_uuid),
        )

    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
