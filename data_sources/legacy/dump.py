#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys

from shared import db_util

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open(config_file="legacy.my.cnf")

    fieldnames = [
        "raw_id",
        "raw_type",
        "name",
        "kana",
        "lat",
        "lon",
        "elevation",
        "z_min",
        "last_updated_at",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    raw_type_list = [
        "等高線",
        "その他",
        "標高点",
        "四等三角点",
        "三等三角点",
        "二等三角点",
        "一等三角点",
        "電子基準点",
    ]

    z_min_list = [13, 13, 12, 11, 10, 9, 8, 8]

    cursor.execute(
        """
        SELECT g.id AS raw_id, s.name, s.kana, lat, lon, alt, level
        FROM geom AS g
        RIGHT JOIN sanmei AS s USING (id)
        WHERE g.id IS NOT NULL AND type >= 1
        ORDER BY g.id, type
        """,
    )
    for row in cursor.fetchall():
        level = row["level"]
        writer.writerow(
            {
                "raw_id": row["raw_id"],
                "raw_type": raw_type_list[level & 7],
                "name": row["name"],
                "kana": row["kana"],
                "lat": row["lat"],
                "lon": row["lon"],
                "elevation": row["alt"],
                "z_min": z_min_list[(level >> 3) & 7],
                "last_updated_at": None,
            }
        )

    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
