#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from argparse import ArgumentParser

from shared import config, db_util

EPS = config.EPS
RADIUS = int(5 * EPS)

parser = ArgumentParser(description="POIデータソースと統合POIのリンクを作成")
parser.add_argument(
    "table_name",
    choices=[
        "stg_gsi_1003_pois",
        "stg_gsi_dm25k_pois",
        "stg_gsi_vtexp_pois",
        "stg_yamap_pois",
        "stg_yamareco_pois",
        "stg_wikidata_pois",
        "stg_legacy_pois",
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
truncate = args.truncate

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    cursor.execute(
        f"""
        SELECT DISTINCT id FROM information_sources WHERE source_table = %s
        """,
        (table_name,),
    )
    if not (rows := cursor.fetchall()):
        raise ValueError(
            f"Error: source_table '{table_name}' not found in information_sources."
        )
    source_id = rows[0]["id"]

    # 既存のリンクを削除
    if truncate:
        cursor.execute(
            """
            DELETE p
            FROM poi_links AS p
            JOIN information_sources AS s ON p.source_id = s.id
            WHERE s.source_table = %s
            """,
            (table_name,),
        )
        print(f"Existing links for {table_name} have been deleted.", file=sys.stderr)

    # 統合POIとデータソースのリンク作成
    cursor.execute(
        f"""
        SELECT
            source_uuid,
            raw_id,
            names_json->>'$[0].name' AS name,
            lat,
            lon
        FROM `{table_name}`
        """,
    )
    rows = cursor.fetchall()
    total_count = len(rows)
    for row in rows:
        source_uuid = row["source_uuid"]
        raw_id = row["raw_id"]
        name = row["name"]

        # 半径RADIUS内の統合POIを検索
        cursor.execute(
            f"SELECT geom INTO @center FROM `{table_name}` WHERE source_uuid = %s",
            (source_uuid,),
        )
        cursor.execute(
            f"""
            SELECT
                m.id AS mountain_id,
                ST_Distance_Sphere(m.geom, @center) AS distance
            FROM mountain_pois AS m
            WHERE m.is_used
                AND ST_Within(m.geom, ST_Buffer(@center, %s))
                AND NOT EXISTS (
                    SELECT 1
                    FROM poi_hierarchies AS p
                    WHERE p.parent_id = m.id
                )
            ORDER BY distance
            LIMIT 1
            """,
            (RADIUS,),
        )
        if not (pois := cursor.fetchall()):
            lat = row["lat"]
            lon = row["lon"]
            print(
                f"Skipping {raw_id} '{name}' ({lat:.6f}, {lon:.6f}): unified POI not found within {RADIUS}m.",
                file=sys.stderr,
            )
            continue
        poi = pois[0]
        mountain_id = poi["mountain_id"]
        distance = poi["distance"]

        cursor.execute(
            """
            INSERT INTO poi_links
                (mountain_id, source_id, source_uuid, linked_distance)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                source_uuid = VALUES(source_uuid),
                linked_distance = VALUES(linked_distance)
            """,
            (mountain_id, source_id, source_uuid, distance),
        )

    # 重複リンクの削除（距離が大きいもの、または距離が同じ場合はsource_uuidが大きいものを削除）
    cursor.execute(
        """
        DELETE p1
        FROM poi_links AS p1
        JOIN poi_links AS p2
            ON p1.mountain_id = p2.mountain_id 
        AND p1.source_id = %s
        AND p2.source_id = %s
        AND (
            p1.linked_distance > p2.linked_distance
            OR (
                p1.linked_distance = p2.linked_distance 
                AND p1.source_uuid > p2.source_uuid
            )
        )
        """,
        (source_id, source_id),
    )

    # 情報源テーブルにリンクした mountain_id を格納
    cursor.execute(
        f"""
        UPDATE `{table_name}` AS s
        JOIN poi_links AS p 
            ON s.source_uuid = p.source_uuid AND p.source_id = %s
        SET s.mountain_id = p.mountain_id
        """,
        (source_id,),
    )

    cursor.execute(
        "SELECT COUNT(*) AS linked_count FROM poi_links WHERE source_id = %s",
        (source_id,),
    )
    linked_count = cursor.fetchone()["linked_count"]
    print(
        f"Total linked POIs: {linked_count}/{total_count} for source '{table_name}'",
        file=sys.stderr,
    )
    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
