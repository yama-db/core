#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector
from shared import config

EPS = config.EPS
RADIUS = int(2.5 * EPS)

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
    "-t",
    "--truncate",
    action="store_true",
    help="既存のリンクを削除してから処理を実行 (デフォルト: False)",
)
args = parser.parse_args()
table_name = args.table_name
source_type = re.sub(r"^stg_|_pois$", "", table_name).upper()
truncate = args.truncate

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        option_groups=["client", "mysql"],
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
    sys.exit(1)

# 既存のリンクを削除
if truncate:
    cursor.execute("DELETE FROM poi_links WHERE source_type = %s", (source_type,))
    conn.commit()
    print(f"Existing links for {source_type} have been deleted.")

# 統合POIとデータソースのリンク作成
if table_name == "stg_book_pois":
    cursor.execute(
        f"""
        SELECT
            raw_remote_id,
            source_uuid,
            names_json->>'$[0].name' AS name,
            elevation_m,
            source_id,
            unified_poi_id AS id
        FROM {table_name}
        """,
    )
else:
    cursor.execute(
        "SELECT id FROM information_sources WHERE display_name = %s",
        (source_type,),
    )
    source_id = cursor.fetchone()["id"]
    cursor.execute(
        f"""
        SELECT
            raw_remote_id,
            source_uuid,
            names_json->>'$[0].name' AS name,
            elevation_m
        FROM {table_name}
        """,
    )
for row in cursor.fetchall():
    raw_remote_id = row["raw_remote_id"]
    source_uuid = row["source_uuid"]
    name = row["name"]
    elevation_m = row["elevation_m"]
    if table_name == "stg_book_pois":
        # idを使って直接リンクを作成する
        source_id = row["source_id"]
        if (id := row["id"]) is None:
            continue
        # 親POIがあり、名称が一致する場合はリンク対象外
        cursor.execute(
            f"""
            SELECT
                u.id AS unified_poi_id,
                u.elevation_m,
                0 AS distance_m
            FROM unified_pois AS u
            LEFT JOIN poi_hierarchies AS h ON u.id = h.child_id
            WHERE u.display_lat != 0 AND u.display_lon != 0
                AND u.id = %s
                AND NOT (h.child_id IS NOT NULL AND h.parent_name = %s)
            """,
            (id, name),
        )
        if not (pois := cursor.fetchall()):
            print(f"Skipping {raw_remote_id} '{name}': unified POI {id} not found.")
            continue
        poi = pois[0]
        ele = poi["elevation_m"]
        if elevation_m is not None and abs(elevation_m - ele) > 20:
            print(f"Skipping {raw_remote_id} '{name}': unified POI {id} not matched by elevation.")
            print(f"  Source: {elevation_m:.0f}m, Unified POI: {ele:.0f}m")
            continue
    else:
        # stg_book_pois以外は距離で絞り込み、標高が最高となる統合POIを検索
        cursor.execute(
            f"SELECT geom INTO @center FROM {table_name} WHERE source_uuid = %s",
            (source_uuid,),
        )
        # 親POIの場合、あるいは、子POIでその親POIと名称が一致する場合はリンクしない
        cursor.execute(
            f"""
            SELECT
                u.id AS unified_poi_id,
                u.elevation_m,
                ST_Distance_Sphere(u.representative_geom, @center) AS distance_m
            FROM unified_pois AS u
            LEFT JOIN poi_hierarchies AS h ON u.id = h.child_id
            WHERE u.display_lat != 0 AND u.display_lon != 0
                AND ST_Within(u.representative_geom, ST_Buffer(@center, %s))
                AND NOT (h.child_id IS NOT NULL AND h.parent_name = %s)
                AND NOT EXISTS (SELECT 1 FROM poi_hierarchies WHERE parent_id = u.id)
            ORDER BY distance_m
            """,
            (RADIUS, name),
        )
        if not (pois := cursor.fetchall()):
            print(f"Skipping {raw_remote_id} '{name}': unified POI not found within {RADIUS}m.")
            continue
        if len(pois) == 1 or pois[0]["distance_m"] < EPS:
            poi = pois[0]
        else:
            poi = max(pois, key=lambda x: x["elevation_m"])

    unified_poi_id = poi["unified_poi_id"]
    distance_m = poi["distance_m"]

    # 既に同じ情報源からリンクされている場合は、距離が近い方を優先
    cursor.execute(
        f"""
        SELECT
            s.raw_remote_id,
            s.names_json->>'$[0].name' AS name,
            p.distance_m
        FROM {table_name} AS s
        JOIN poi_links AS p ON s.source_uuid = p.source_uuid
        WHERE p.unified_poi_id = %s AND p.source_id = %s AND p.distance_m < %s
        ORDER BY p.distance_m
        LIMIT 1
        """,
        (unified_poi_id, source_id, distance_m),
    )
    if (exists_closer := cursor.fetchone()) is not None:
        print(f"Skipping {raw_remote_id} '{name}': closer link already exists.")
        print(
            f"  Existing: {exists_closer['raw_remote_id']} '{exists_closer['name']}' at {exists_closer['distance_m']:.1f}m"
        )
        continue
    try:
        cursor.execute(
            """
            INSERT INTO poi_links
                (unified_poi_id, source_type, source_id, source_uuid, distance_m)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                source_uuid = VALUES(source_uuid),
                distance_m = VALUES(distance_m)
            """,
            (unified_poi_id, source_type, source_id, source_uuid, distance_m),
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during insertion: {e}")
        conn.rollback()
        sys.exit(1)

cursor.execute(
    "SELECT COUNT(*) AS total FROM poi_links WHERE source_type = %s",
    (source_type,),
)
total = cursor.fetchone()["total"]
print(f"Total linked POIs: {total}")

# 接続終了
cursor.close()
conn.close()

# __END__
