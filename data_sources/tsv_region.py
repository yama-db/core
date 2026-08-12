#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# YAMAP/Yamareco 国内のPOIのみを出力

import csv
import html
import sys
from argparse import ArgumentParser

from shared import db_util

parser = ArgumentParser(
    description="YAMAP/YamarecoのTSVファイルから国内のPOIのみを抽出"
)
parser.add_argument("tsv_file", help="TSVファイルのパス")
args = parser.parse_args()
tsv_file = args.tsv_file

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

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

    with open(tsv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        print("\t".join(reader.fieldnames))
        for row in reader:
            name = html.unescape(row["name"])
            lon = float(row["lon"])
            lat = float(row["lat"])
            if lon and lat:
                if not (abs(lon) <= 180.0 and abs(lat) <= 90.0):
                    print(
                        f"Warning: {name} ({lat:.6f}, {lon:.6f}) is outside valid range.",
                        file=sys.stderr,
                    )
                    continue
                coord = f"POINT({lon:.6f} {lat:.6f})"
                cursor.execute(
                    f"""
                    SELECT EXISTS (
                        SELECT 1
                        FROM administrative_boundaries 
                        WHERE ST_Contains(
                            geom,
                            ST_GeomFromText(%s, 4326, "axis-order=long-lat")
                        )
                    ) AS is_japan;
                    """,
                    (coord,),
                )
                result = cursor.fetchone()
                if not result["is_japan"]:
                    continue
            print("\t".join([row[field] for field in reader.fieldnames]))

    success = True

except Exception as e:
    print(f"Error processing file: {e}", file=sys.stderr)
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
