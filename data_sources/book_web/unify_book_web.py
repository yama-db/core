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
    choices=["stg_book_web_pois"],
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

    # 既存のリンクを削除
    if truncate:
        cursor.execute(
            """
            DELETE p
            FROM poi_links AS p
            JOIN information_sources AS s ON p.source_id = s.id AND s.source_table = %s
            """,
            (table_name,),
        )
        print(f"Existing links for {table_name} have been deleted.", file=sys.stderr)

    # 統合POIとデータソースのリンク作成
    # FIXME: names_json->>'$[0].type' is assumed to be 'MAIN'.
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
    for row in cursor.fetchall():
        if (mountain_id := row["mountain_id"]) is None:
            continue
        source_uuid = row["source_uuid"]
        source_id = row["source_id"]
        raw_id = row["raw_id"]
        name = row["name"]
        elevation = row["elevation"]

        cursor.execute(
            f"""
            SELECT id, elevation
            FROM mountain_pois
            WHERE is_used AND id = %s
            """,
            (mountain_id,),
        )
        if not (poi := cursor.fetchone()):
            print(
                f"Skipping source_id={source_id} raw_id={raw_id} '{name}': mountain POI {mountain_id} not found.",
                file=sys.stderr,
            )
            continue

        mountain_id = poi["id"]
        elev = poi["elevation"]
        # 標高が大きく異なる場合は除外
        if elevation is not None and abs(elevation - elev) > 20:
            print(
                f"Skipping source_id={source_id} raw_id={raw_id} '{name}': mountain POI {mountain_id} not matched by elevation.",
                file=sys.stderr,
            )
            print(
                f"  Source: {elevation:.0f}m, Mountain POI: {elev:.0f}m",
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
            (mountain_id, source_id, source_uuid, 0),
        )

    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM poi_links AS p
        JOIN information_sources AS s ON p.source_id = s.id AND s.source_table = %s
        """,
        (table_name,),
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
