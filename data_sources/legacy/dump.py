#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
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
        "names_json",
        "lat",
        "lon",
        "elevation",
        "z_min",
        "last_updated_at",
    ]
    print("\t".join(fieldnames))

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

    TYPE_MAP = {
        0: 'AREA',
        1: 'MAIN',
        2: 'ALIAS'
    }

    cursor.execute(
        """
        SELECT
            g.id AS raw_id,
            (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        "name", s.name,
                        "kana", s.kana,
                        "type", s.type
                    )
                )
                FROM sanmei AS s
                WHERE s.id = g.id
            ) AS names_json,
            g.lat,
            g.lon,
            g.alt,
            g.level
        FROM geom AS g
        ORDER BY g.id
        """,
    )
    for row in cursor.fetchall():
        level = row["level"]
        names_json = json.loads(row["names_json"])
        for item in names_json:
            current_type = item.get("type")
            item["type"] = TYPE_MAP.get(current_type, 'UNKNOWN')
        names_json.sort(key=lambda item: item["type"] != 'MAIN')

        row = [
            str(row["raw_id"]),
            raw_type_list[level & 7],
            json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
            str(row["lat"]),
            str(row["lon"]),
            str(row["alt"]),
            str(z_min_list[(level >> 3) & 7]),
            "",
        ]
        print("\t".join(row))

    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
