#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser
from pathlib import Path

import mysql.connector

parser = ArgumentParser(description="統一POIデータを生成")
parser.add_argument("unified_pois_csv", help="統一POIデータの出力先CSVファイル")
parser.add_argument("poi_hierarchies_csv", help="POI階層データの出力先CSVファイル")
args = parser.parse_args()
unified_pois_csv = args.unified_pois_csv
poi_hierarchies_csv = args.poi_hierarchies_csv

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        database="anineco_tozan",
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

cursor.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM geom")
max_id = cursor.fetchone()["max_id"]
assert max_id > 0

cursor.execute("CREATE TEMPORARY TABLE IF NOT EXISTS seq_ids (id INT PRIMARY KEY)")
cursor.execute("TRUNCATE TABLE seq_ids")
cursor.executemany(
    "INSERT INTO seq_ids (id) VALUES (%s)", [(i,) for i in range(1, max_id + 1)]
)
conn.commit()

pois = [{"id": i, "name": "", "lat": 0.0, "lon": 0.0, "alt": 0.0} for i in range(1, max_id + 1)]

cursor.execute(
    """
    SELECT id, s.name, lat, lon, alt
    FROM geom
    JOIN sanmei AS s USING (id)
    WHERE type = 1
    ORDER BY id ASC
    """
)
for row in cursor.fetchall():
    id = row["id"]
    pois[id - 1] = row.copy()

# 山域名と山名を親と子の関係で取得
cursor.execute(
    """
    SELECT
        p.id AS parent_id,
        p.name AS parent_name,
        c.name AS child_name,
        g.lat,
        g.lon,
        g.alt
    FROM geom AS g
    JOIN sanmei AS p USING (id)
    JOIN sanmei AS c USING (id)
    WHERE p.type = 0 AND c.type = 1
    """
)
relations = cursor.fetchall()

# 山頂名を格納するための空きIDを準備
cursor.execute(
    """
    SELECT s.id
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
    writer = csv.DictWriter(f, fieldnames=["parent_id", "parent_name", "child_id", "child_name", "relation_type"])
    writer.writeheader()
    for relation, unused in zip(relations, unuseds, strict=True):
        parent_id = relation["parent_id"]
        pois[parent_id - 1].update(
            {
                "name": relation["parent_name"],
            }
        )
        id = unused["id"]
        pois[id - 1].update(
            {
                "id": id,
                "name": relation["child_name"],
                "lat": relation["lat"],
                "lon": relation["lon"],
                "alt": relation["alt"],
            }
        )
        writer.writerow(
            {
                "parent_id": parent_id,
                "parent_name": relation["parent_name"],
                "child_id": id,
                "child_name": relation["child_name"],
                "relation_type": "MEMBER",
            }
        )

with open(unified_pois_csv, "w", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "lat", "lon", "alt"])
    writer.writeheader()
    for poi in pois:
        writer.writerow(poi)

cursor.close()
conn.close()

# __END__
