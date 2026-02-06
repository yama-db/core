#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from pathlib import Path

import mysql.connector
from shared import generate_source_uuid

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / "legacy.my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
    sys.exit(1)

fieldnames = [
    "source_uuid",
    "raw_remote_id",
    "name",
    "kana",
    "lat",
    "lon",
    "elevation_m",
    "poi_type_raw",
    "last_updated_at",
]
writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
writer.writeheader()

cursor.execute(
    """
    SELECT g.id, s.name, s.kana, lat, lon, alt
    FROM geom AS g
    RIGHT JOIN sanmei AS s USING (id)
    WHERE g.id IS NOT NULL AND type >= 1
    ORDER BY g.id, type
    """,
)
for row in cursor.fetchall():
    raw_remote_id = row["id"]
    uuid = generate_source_uuid("legacy_poi", raw_remote_id)
    writer.writerow(
        {
            "source_uuid": uuid,
            "raw_remote_id": raw_remote_id,
            "name": row["name"],
            "kana": row["kana"],
            "lat": row["lat"],
            "lon": row["lon"],
            "elevation_m": row["alt"],
            "poi_type_raw": "",
            "last_updated_at": "",
        }
    )
