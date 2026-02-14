#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

EPS = 40  # 位置の許容誤差[m]

# コマンドライン引数の解析
parser = ArgumentParser(description="GSI GCPを統合POIにリンク")
parser.add_argument(
    "table_name", choices=["stg_gsi_gcp_pois"], help="POIデータソースのテーブル名"
)
parser.add_argument(
    "-r",
    "--radius",
    type=int,
    default=EPS,
    help=f"バッファの半径[m] (デフォルト: {EPS})",
)
args = parser.parse_args()
table_name = args.table_name
source_type = re.sub(r"^stg_|_pois$", "", table_name).upper()
radius = args.radius

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

cursor.execute("SELECT id FROM information_sources WHERE display_name = %s", (source_type,))
source_id = cursor.fetchone()["id"]
print(f"Using source_type: {source_type}, source_id: {source_id}")

# 一時テーブルの作成（POI種別ごとのズームレベル）
cursor.execute(
    """
    CREATE TEMPORARY TABLE poi_weights (
        poi_type_raw VARCHAR(50) PRIMARY KEY,
        min_zoom_level TINYINT NOT NULL
    )
    """,
)
cursor.executemany(
    "INSERT INTO poi_weights (poi_type_raw, min_zoom_level) VALUES (%s, %s)",
    [
        ("経緯度原点", 8),
        ("電子基準点", 8),
        ("一等三角点", 8),
        ("二等三角点", 9),
        ("三等三角点", 10),
        ("四等三角点", 11),
        ("GPS固定点", 11),
        ("標高点", 12),
    ],
)
conn.commit()

# 既存のリンクを削除
cursor.execute("DELETE FROM poi_links WHERE source_type = %s", (source_type,))
conn.commit()

cursor.execute("SELECT id FROM unified_pois ORDER BY id ASC")
for row in cursor.fetchall():
    id = row["id"]
    # バッファの作成
    cursor.execute(
        """
        SELECT ST_Buffer(representative_geom, %s) INTO @buffer
        FROM unified_pois
        WHERE id = %s
        """,
        (radius, id),
    )
    # バッファ内で最も標高の高いPOIを取得
    cursor.execute(
        f"""
        SELECT source_uuid
        FROM {table_name}
        WHERE ST_Within(geom, @buffer)
        ORDER BY elevation_m DESC
        LIMIT 1
        """,
    )
    result = cursor.fetchone()
    if not result:
        print(f"No GCP POI found within {radius}m for unified_poi_id {id}")
        continue
    source_uuid = result["source_uuid"]
    # バッファ内で最小のズームレベルを取得
    cursor.execute(
        f"""
        SELECT min_zoom_level
        FROM {table_name}
        JOIN poi_weights USING (poi_type_raw)
        WHERE ST_Within(geom, @buffer)
        ORDER BY min_zoom_level ASC
        LIMIT 1
        """,
    )
    result = cursor.fetchone()
    assert result, "Expected at least one POI type weight"
    min_zoom_level = result["min_zoom_level"]
    try:
        cursor.execute(
            f"""
            UPDATE unified_pois AS target
            JOIN {table_name} AS source ON source.source_uuid = %s
            SET 
                target.representative_geom = source.geom,
                target.elevation_m = source.elevation_m,
                target.min_zoom_level = %s
            WHERE target.id = %s
            """,
            (source_uuid, min_zoom_level, id),
        )
        cursor.execute(
            """
            INSERT INTO poi_links (unified_poi_id, source_type, source_id, source_uuid)
            VALUES (%s, %s, %s, %s)
            """,
            (id, source_type, source_id, source_uuid),
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during update for unified_poi_id {id}: {e}")
        conn.rollback()
        sys.exit(1)

conn.close()
cursor.close()

# __END__
