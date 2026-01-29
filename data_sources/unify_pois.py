#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import Levenshtein
import mysql.connector

parser = ArgumentParser(description="POIデータソースと統合POIのリンクを作成")
parser.add_argument(
    "table_name",
    choices=[
        "stg_gsi_dm25k_pois",
        "stg_gsi_vtexp_pois",
        "stg_yamap_pois",
        "stg_yamareco_pois",
        "stg_wikidata_pois",
        "stg_book_pois",
    ],
    help="POIデータソースのテーブル名",
)
parser.add_argument(
    "-r",
    "--radius",
    type=int,
    default=10000,
    help="バッファの半径[m] (デフォルト: 10000)",
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


# リンク挿入関数
def link_pois(id, source_type, source_uuid):
    try:
        cursor.execute(
            """
            INSERT INTO poi_links (unified_poi_id, source_type, source_uuid)
            VALUES (%s, %s, %s)
            """,
            (id, source_type, source_uuid),
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during insertion: {e}")
        conn.rollback()
        sys.exit(1)


# 既存のリンクを削除
cursor.execute("DELETE FROM poi_links WHERE source_type = %s", (source_type,))
conn.commit()

# POIごとに処理
column = "unified_poi_id" if table_name == "stg_book_pois" else "NULL"
cursor.execute(
    f"SELECT source_uuid, names_json, elevation_m, {column} AS id FROM {table_name}"
)
count = 0  # 一致したPOIのカウント
for row in cursor.fetchall():
    source_uuid = row["source_uuid"]
    names = [item.get("name", "") for item in json.loads(row["names_json"])]
    if table_name == "stg_book_pois":
        cursor.execute(
            """
            SELECT
                id, representative_name,
                0 AS distance_m
            FROM unified_pois
            WHERE id = %s AND ABS(elevation_m - %s) <= 20
            """,
            (row["id"], row["elevation_m"]),
        )
    else:        
        cursor.execute(
            f"""
            SELECT
                id, representative_name,
                ST_Distance_Sphere(representative_geom, geom) AS distance_m
            FROM unified_pois
            CROSS JOIN {table_name}
            WHERE source_uuid = %s AND ST_Within(
                representative_geom,
                ST_Buffer(geom, %s)
            )
            ORDER BY distance_m ASC
            """,
            (source_uuid, radius),
        )
    for result in cursor.fetchall():
        id = result["id"]
        representative_name = result["representative_name"]
        distance_m = result["distance_m"]
        if table_name != "stg_book_pois" and distance_m < 100:  # 100m未満は無条件でリンク
            print(
                f"Linking {names[0]} to ID: {id} {representative_name} "
                f"(distance: {distance_m:.1f} m)"
            )
            link_pois(id, source_type, source_uuid)
            count += 1
            break

        max_similarity = max(
            Levenshtein.jaro_winkler(name, representative_name) for name in names
        )

        # 類似度が0.8以上であればリンクを作成
        if max_similarity >= 0.8:
            print(
                f"Linking {names[0]} to ID: {id} {representative_name} "
                f"(distance: {distance_m:.1f} m, similarity: {max_similarity:.3f})"
            )
            link_pois(id, source_type, source_uuid)
            count += 1
            break

print(f"Total linked POIs: {count}")

# 接続終了
cursor.close()
conn.close()

# __END__
