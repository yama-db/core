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
def link_pois(id, source_type, source_id, source_uuid, distance_m):
    cursor.execute(
        f"""
        SELECT
            raw_remote_id,
            names_json->>'$[0].name' AS name
        FROM unified_pois
        JOIN poi_links AS p ON id = unified_poi_id
        JOIN {table_name} USING (source_uuid)
        WHERE id = %s AND p.source_id = %s
            AND ST_Distance_Sphere(geom, representative_geom) <= %s
        LIMIT 1
        """,
        (id, source_id, distance_m),
    )
    if existing_link := cursor.fetchone():
        return existing_link
    try:
        cursor.execute(
            """
            INSERT INTO poi_links (unified_poi_id, source_type, source_id, source_uuid)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE source_uuid = VALUES(source_uuid)
            """,
            (id, source_type, source_id, source_uuid),
        )
        conn.commit()
        return None
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
if table_name == "stg_book_pois":
    cursor.execute(
        f"""
        SELECT source_uuid, source_id, names_json, elevation_m, unified_poi_id AS id
        FROM {table_name}
        """,
    )
else:
    cursor.execute(
        "SELECT id FROM information_sources WHERE display_name = %s", (source_type,)
    )
    source_id = cursor.fetchone()["id"]
    cursor.execute(
        f"""
        SELECT source_uuid, names_json, elevation_m, NULL AS id
        FROM {table_name}
        """,
    )
total = 0  # 一致したPOIのカウント
for row in cursor.fetchall():
    source_uuid = row["source_uuid"]
    if table_name == "stg_book_pois":
        source_id = row["source_id"]
    names_json = json.loads(row["names_json"])
    names = [item["name"] for item in names_json]
    if table_name == "stg_book_pois":
        if not (id := row["id"]):
            print(f"{names[0]} has no pre-assigned unified_poi_id, skipping.")
            continue
        if elevation_m := row["elevation_m"]:
            cursor.execute(
                """
                SELECT
                    id, representative_name,
                    0 AS distance_m
                FROM unified_pois
                WHERE id IN (
                    SELECT %s
                    UNION
                    SELECT child_id FROM poi_hierarchies WHERE parent_id = %s
                    UNION
                    SELECT parent_id FROM poi_hierarchies WHERE child_id = %s
                ) AND ABS(elevation_m - %s) <= 20
                """,
                (id, id, id, elevation_m),
            )
        else:
            cursor.execute(
                """
                SELECT
                    id, representative_name,
                    0 AS distance_m
                FROM unified_pois
                WHERE id IN (
                    SELECT %s
                    UNION
                    SELECT child_id FROM poi_hierarchies WHERE parent_id = %s
                    UNION
                    SELECT parent_id FROM poi_hierarchies WHERE child_id = %s
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
            WHERE source_uuid = %s AND geom IS NOT NULL AND ST_Within(
                representative_geom,
                ST_Buffer(geom, %s)
            )
            ORDER BY distance_m ASC
            """,
            (source_uuid, radius),
        )
    results = cursor.fetchall()
    if not results:
        print(f"No candidates found for {names[0]}, skipping.")
        continue
    for result in results:
        result["similarity"] = max(
            Levenshtein.jaro_winkler(name, result["representative_name"])
            for name in names
        )
    for candidate in sorted(results, key=lambda x: (x["distance_m"], -x["similarity"])):
        id = candidate["id"]
        distance_m = candidate["distance_m"]
        similarity = candidate["similarity"]
        representative_name = candidate["representative_name"]
        if (
            table_name == "stg_book_pois"
            or distance_m < 100
            or (distance_m < 500 and similarity >= 0.8)
            or similarity >= 0.9
        ):
            if existing_link := link_pois(id, source_type, source_id, source_uuid, distance_m):
                raw_remote_id = existing_link["raw_remote_id"]
                name = existing_link["name"]
                print(f"link insertion '{names[0]}' to ID:{id} '{representative_name}'")
                print(f"already linked to {raw_remote_id} '{name}', skipping.")
            else:
                total += 1
                break

print(f"Total linked POIs: {total}")

# 接続終了
cursor.close()
conn.close()

# __END__
