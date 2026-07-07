#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser

from shared import db_util

parser = ArgumentParser(description="統一POIデータを生成")
parser.add_argument("mountain_pois_csv", help="統一POIデータの出力先CSVファイル")
parser.add_argument("poi_hierarchies_csv", help="POI階層データの出力先CSVファイル")
args = parser.parse_args()
mountain_pois_csv = args.mountain_pois_csv
poi_hierarchies_csv = args.poi_hierarchies_csv

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open(config_file="legacy.my.cnf")

    cursor.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM geom")
    max_id = cursor.fetchone()["max_id"]
    if max_id == 0:
        raise ValueError("No data found in geom table")

    # Create a temporary table to hold sequential IDs
    cursor.execute("CREATE TEMPORARY TABLE IF NOT EXISTS seq_ids (id INT PRIMARY KEY)")
    cursor.execute("TRUNCATE TABLE seq_ids")
    cursor.executemany(
        "INSERT INTO seq_ids (id) VALUES (%s)", [(i,) for i in range(1, max_id + 1)]
    )

    pois = [
        {"id": i, "is_used": 0, "name": "", "kana": "", "lat": 0.0, "lon": 0.0, "alt": 0.0}
        for i in range(1, max_id + 1)
    ]

    cursor.execute(
        """
        SELECT id, 1 AS is_used, s.name, s.kana, lat, lon, alt
        FROM geom
        JOIN sanmei AS s USING (id)
        WHERE type = 1
        ORDER BY id ASC
        """,
    )
    for row in cursor.fetchall():
        id = row["id"]
        pois[id - 1] = row.copy()

    # 山域名と山頂名を親と子の関係で取得
    cursor.execute(
        """
        SELECT
            g.id AS child_id,
            p.name AS parent_name,
            p.kana AS parent_kana,
            c.name AS child_name,
            c.kana AS child_kana,
            g.lat,
            g.lon,
            g.alt
        FROM geom AS g
        JOIN sanmei AS p ON g.id = p.id
        JOIN sanmei AS c ON g.id = c.id
        WHERE p.type = 0 AND c.type = 1
        """,
    )
    relations = cursor.fetchall()

    # 山頂名を格納するための空きIDを準備
    cursor.execute(
        """
        SELECT s.id AS parent_id
        FROM seq_ids AS s
        LEFT JOIN geom AS g USING (id)
        WHERE g.id IS NULL
        ORDER BY s.id
        LIMIT %s
        """,
        (len(relations),),
    )
    unuseds = cursor.fetchall()

    with open(poi_hierarchies_csv, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "parent_id",
                "parent_name",
                "child_id",
                "child_name",
                "relation_type",
            ],
        )
        writer.writeheader()
        for relation, unused in zip(relations, unuseds, strict=True):
            child_id = relation["child_id"]
            pois[child_id - 1].update(
                {
                    "is_used": 1,
                    "name": relation["child_name"],
                    "kana": relation["child_kana"],
                }
            )
            parent_id = unused["parent_id"]
            pois[parent_id - 1].update(
                {
                    "id": parent_id,
                    "is_used": 1,
                    "name": relation["parent_name"],
                    "kana": relation["parent_kana"],
                    "lat": relation["lat"],
                    "lon": relation["lon"],
                    "alt": relation["alt"],
                }
            )
            writer.writerow(
                {
                    "parent_id": parent_id,
                    "parent_name": relation["parent_name"],
                    "child_id": child_id,
                    "child_name": relation["child_name"],
                    "relation_type": "AREA_TO_PEAK",
                }
            )

    with open(mountain_pois_csv, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["id", "is_used", "name", "kana", "lat", "lon", "alt"]
        )
        writer.writeheader()
        for poi in pois:
            writer.writerow(poi)

    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
