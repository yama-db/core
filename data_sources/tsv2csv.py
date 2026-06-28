#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# YAMAP/Yamareco TSVファイルをCSV形式に変換して出力するスクリプト

import csv
import html
import json
import sys
from argparse import ArgumentParser

import jaconv
import regex
from shared import db_util, extract_aliases

parser = ArgumentParser(description="YAMAP/YamarecoのTSVファイルを変換してCSV出力")
parser.add_argument(
    "source",
    choices=["yamap", "yamareco"],
    help="データソース（yamap または yamareco）を指定",
)
parser.add_argument("tsv_file", help="TSVファイルのパス")
args = parser.parse_args()
source = args.source
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
        "name",
        "kana",
        "lat",
        "lon",
        "elevation",
        "z_min",
        "last_updated_at",
    ]

    with open(tsv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            name = html.unescape(row["name"].strip())
            if name.startswith("（") and name.endswith("）"):
                name = name[1:-1]
            name = regex.sub(r"(\p{Han})ケ(\p{Han})", r"\1ヶ\2", name)
            data = json.loads(row["kana"])
            hira = data.get("hira", "")
            kana = jaconv.kata2hira(hira) if hira else ""

            lon = float(row["lon"])
            lat = float(row["lat"])
            if (elevation := row["elevation"]) == "NULL":
                elevation = None
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
                    print(
                        f"Warning: {name} ({lat:.6f}, {lon:.6f}) is outside Japan.",
                        file=sys.stderr,
                    )
                    continue

            for n, k in extract_aliases(name, kana):
                writer.writerow(
                    {
                        "raw_id": row["raw_id"],
                        "raw_type": row["raw_type"],
                        "name": n,
                        "kana": k,
                        "lat": row["lat"],
                        "lon": row["lon"],
                        "elevation": elevation,
                        "z_min": None,
                        "last_updated_at": row["last_updated_at"],
                    }
                )

    success = True

except Exception as e:
    print(f"Error processing file: {e}", file=sys.stderr)
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
