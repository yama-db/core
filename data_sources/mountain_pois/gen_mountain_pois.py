#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser

from shared import db_util

parser = ArgumentParser(description="統一POIデータを生成")
parser.add_argument("parent_pois_csv", help="親POIのCSVファイル")
parser.add_argument("mountain_pois_csv", help="統一POIデータの出力先CSVファイル")
parser.add_argument("poi_hierarchies_csv", help="POI階層データの出力先CSVファイル")
args = parser.parse_args()
parent_pois_csv = args.parent_pois_csv
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
        {
            "id": i,
            "is_used": 0,
            "name": "",
            "kana": "",
            "lat": 0.0,
            "lon": 0.0,
            "alt": 0.0,
        }
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
    with open(parent_pois_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # group_id,parent_name,parent_kana,child_id,child_name,relation_type,is_representative
        relations = list(reader)

    # 異なる group_id の数を数える
    unique_count = len({row["group_id"] for row in relations})

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
        (unique_count,),
    )
    unuseds = cursor.fetchall()

    fieldnames = [
        "parent_id",
        "parent_name",
        "child_id",
        "child_name",
        "relation_type",
        "is_representative",
    ]

    with open(poi_hierarchies_csv, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        previous_group_id = 0
        for relation in relations:
            group_id = int(relation["group_id"])
            unused = unuseds[group_id - 1]
            parent_id = int(unused["parent_id"])
            child_id = int(relation["child_id"])
            child_poi = pois[child_id - 1]
            if group_id == previous_group_id + 1:
                pois[parent_id - 1].update(
                    {
                        "id": parent_id,
                        "is_used": 1,
                        "name": relation["parent_name"],
                        "kana": relation["parent_kana"],
                        "lat": child_poi["lat"],
                        "lon": child_poi["lon"],
                        "alt": child_poi["alt"],
                    }
                )
                previous_group_id = group_id
            elif group_id != previous_group_id:
                raise ValueError(
                    f"Unexpected group_id sequence: {group_id} after {previous_group_id}"
                )
            writer.writerow(
                {
                    "parent_id": parent_id,
                    "parent_name": relation["parent_name"],
                    "child_id": child_id,
                    "child_name": relation["child_name"],
                    "relation_type": relation["relation_type"],
                    "is_representative": relation["is_representative"],
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
