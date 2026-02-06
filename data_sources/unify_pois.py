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
        "stg_legacy_pois",
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


# POI階層情報を取得
cursor.execute("""
    SELECT DISTINCT
        parent_id, p.representative_name AS parent_name
    FROM unified_pois AS p
    JOIN poi_hierarchies ON p.id=parent_id
    JOIN unified_pois AS c ON child_id=c.id
    WHERE parent_id IS NOT NULL AND child_id IS NOT NULL
    """)
mt_ranges = {row["parent_name"]: row["parent_id"] for row in cursor.fetchall()}

# 既存のリンクを削除
cursor.execute("DELETE FROM poi_links WHERE source_type = %s", (source_type,))
conn.commit()

# POIごとに処理
column = "unified_poi_id" if table_name == "stg_book_pois" else "NULL"
cursor.execute(
    f"SELECT source_uuid, names_json, elevation_m, {column} AS id FROM {table_name}"
)
total = 0  # 一致したPOIのカウント
for row in cursor.fetchall():
    source_uuid = row["source_uuid"]
    names_json = json.loads(row["names_json"])
    names = [name for item in names_json if (name := item.get("name"))]
    if table_name in ["stg_gsi_dm25k_pois", "stg_gsi_vtexp_pois"]:
        # まず山域名での一致を試みる
        ids = [id for name in names if (id := mt_ranges.get(name))]
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            cursor.execute(
                f"""
                SELECT
                    id, representative_name,
                    ST_Distance_Sphere(representative_geom, geom) AS distance_m
                FROM unified_pois
                CROSS JOIN {table_name}
                WHERE source_uuid = %s AND id IN ({placeholders})
                ORDER BY distance_m ASC
                LIMIT 1
                """,
                (source_uuid, *ids),
            )
            result = cursor.fetchone()
            if (distance_m := result["distance_m"]) < 5000:
                id = result["id"]
                representative_name = result["representative_name"]
                distance_m = result["distance_m"]
                print(
                    f"{names[0]} is a mountain range matched to ID: {id} (distance: {distance_m:.1f} m)"
                )
                print("skipping further matching.")
                continue

    if table_name == "stg_book_pois":
        id = row["id"]
        if elevation_m := row["elevation_m"]:
            cursor.execute(
                """
                SELECT id, representative_name, 0 AS distance_m
                FROM unified_pois
                WHERE ABS(elevation_m - %s) <= 20 AND id IN (
                    SELECT %s
                    UNION
                    SELECT parent_id FROM poi_hierarchies WHERE child_id = %s
                    UNION
                    SELECT child_id FROM poi_hierarchies WHERE parent_id = %s
                )
                """,
                (elevation_m, id, id, id),
            )
        else:
            cursor.execute(
                """
                SELECT id, representative_name, 0 AS distance_m
                FROM unified_pois
                WHERE id IN (
                    SELECT %s
                    UNION
                    SELECT parent_id FROM poi_hierarchies WHERE child_id = %s
                    UNION
                    SELECT child_id FROM poi_hierarchies WHERE parent_id = %s
                )
                """,
                (id, id, id),
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
    results = cursor.fetchall()
    if not results:
        continue
    for result in results:
        result["similarity"] = max(
            Levenshtein.jaro_winkler(name, result["representative_name"])
            for name in names
        )
    sorted_results = sorted(results, key=lambda x: (x["distance_m"], -x["similarity"]))
    nearest = sorted_results[0]
    id = nearest["id"]
    distance_m = nearest["distance_m"]
    similarity = nearest["similarity"]
    representative_name = nearest["representative_name"]
    if table_name == "stg_book_pois" or distance_m < 100 or similarity >= 0.8:
        if distance_m >= 100 or similarity < 0.8:
            # print(
            #    f"Linking {names[0]} to ID: {id} {representative_name} "
            #    f"(distance: {distance_m:.1f} m, similarity: {similarity:.3f})"
            # )
            pass
        link_pois(id, source_type, source_uuid)
        total += 1

print(f"Total linked POIs: {total}")

# 接続終了
cursor.close()
conn.close()

# __END__
