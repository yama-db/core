#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from argparse import ArgumentParser

from shared import config, db_util

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
        "stg_book_web_pois",
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
        for row in rows:
            cursor.execute("DELETE FROM poi_links WHERE source_id = %s", (row["id"],))
        print(f"Existing links for {table_name} have been deleted.", file=sys.stderr)

    # 統合POIとデータソースのリンク作成
    if table_name == "stg_book_web_pois":
        cursor.execute(
            f"""
            SELECT
                source_uuid,
                source_id,
                raw_id,
                mountain_id,
                names_json->>'$[0].name' AS name,
                elevation
            FROM `{table_name}`
            """,
        )
    else:
        cursor.execute(
            f"""
            SELECT
                source_uuid,
                raw_id,
                names_json->>'$[0].name' AS name,
                elevation
            FROM `{table_name}`
            """,
        )
    for row in cursor.fetchall():
        source_uuid = row["source_uuid"]
        raw_id = row["raw_id"]
        name = row["name"]
        elevation = row["elevation"]
        if table_name == "stg_book_web_pois":
            # idを使って直接リンクを作成する
            source_id = row["source_id"]
            if (mountain_id := row["mountain_id"]) is None:
                continue
            cursor.execute(
                f"""
                SELECT 
                    child.id AS mountain_id,
                    child.elevation,
                    0 AS distance
                FROM mountain_pois AS child
                WHERE
                    child.is_used
                    AND child.id = %s
                    -- ▼ 親POIが存在し、その名称が指定のものと一致する場合は除外する条件
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM poi_hierarchies AS h
                        INNER JOIN mountain_pois AS parent 
                            ON h.parent_id = parent.id
                        WHERE 
                            h.child_id = child.id
                            AND parent.main_name = %s
                    )
                """,
                (mountain_id, name),
            )
            if not (pois := cursor.fetchall()):
                print(
                    f"Skipping {raw_id} '{name}': mountain POI {mountain_id} not found.",
                    file=sys.stderr,
                )
                continue
            # 標高が大きく異なる場合は除外
            poi = pois[0]
            ele = poi["elevation"]
            if elevation is not None and abs(elevation - ele) > 20:
                print(
                    f"Skipping {raw_id} '{name}': mountain POI {mountain_id} not matched by elevation.",
                    file=sys.stderr,
                )
                print(
                    f"  Source: {elevation:.0f}m, Mountain POI: {ele:.0f}m",
                    file=sys.stderr,
                )
                continue
        else:
            # stg_book_pois以外は距離で絞り込み、標高が最高となる統合POIを検索
            cursor.execute(
                f"SELECT geom INTO @center FROM `{table_name}` WHERE source_uuid = %s",
                (source_uuid,),
            )
            # 親POIの場合、あるいは、子POIでその親POIと名称が一致する場合はリンクしない
            cursor.execute(
                f"""
                SELECT
                    child.id AS mountain_id,
                    child.elevation,
                    ST_Distance_Sphere(child.geom, @center) AS distance
                FROM mountain_pois AS child
                WHERE child.is_used
                    AND ST_Within(child.geom, ST_Buffer(@center, %s))
                    -- ▼ ① 親POIである場合を除外（他の子POIを従えている概念レコードなどを排除）
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM poi_hierarchies AS p
                        WHERE p.parent_id = child.id
                    )
                    -- ▼ ② 子POIであり、かつその親POIと名称が完全一致する場合を除外
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM poi_hierarchies AS h
                        INNER JOIN mountain_pois AS parent 
                            ON h.parent_id = parent.id
                        WHERE h.child_id = child.id
                        AND parent.main_name = %s
                    )
                ORDER BY distance
                """,
                (RADIUS, name),
            )
            if not (pois := cursor.fetchall()):
                print(
                    f"Skipping {raw_id} '{name}': unified POI not found within {RADIUS}m.",
                    file=sys.stderr,
                )
                continue
            if len(pois) == 1 or pois[0]["distance"] < EPS:
                poi = pois[0]
            else:
                poi = max(pois, key=lambda x: x["elevation"])

        mountain_id = poi["mountain_id"]
        distance = poi["distance"]

        # 既に同じ情報源からリンクされている場合は、距離が近い方を優先
        cursor.execute(
            f"""
            SELECT
                s.raw_id,
                s.names_json->>'$[0].name' AS name,
                p.linked_distance AS distance
            FROM `{table_name}` AS s
            JOIN poi_links AS p ON s.source_uuid = p.source_uuid
            WHERE p.mountain_id = %s AND p.source_id = %s AND p.linked_distance < %s
            ORDER BY p.linked_distance
            LIMIT 1
            """,
            (mountain_id, source_id, distance),
        )
        if (exists_closer := cursor.fetchone()) is not None:
            print(
                f"Skipping {raw_id} '{name}': closer link already exists.",
                file=sys.stderr,
            )
            ec_raw_id = exists_closer['raw_id']
            ec_name = exists_closer['name']
            ec_distance = exists_closer['distance']
            print(
                f"  Existing: {ec_raw_id} '{ec_name}' at {ec_distance:.1f}m",
                file=sys.stderr,
            )
            continue

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

    cursor.execute(
        "SELECT COUNT(*) AS total FROM poi_links WHERE source_id = %s",
        (source_id,),
    )
    total = cursor.fetchone()["total"]
    print(f"Total linked POIs: {total}")
    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
