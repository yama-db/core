#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# YAMAP/Yamareco TSVファイルをCSV形式に変換して出力するスクリプト

import csv
import html
import json
import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import jaconv
import mysql.connector
from shared import generate_source_uuid

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

try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
    sys.exit(1)

try:
    with open(tsv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = ["source_uuid"] + reader.fieldnames
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            name = html.unescape(row["name"].strip())
            if name.startswith("（") and name.endswith("）"):
                name = name[1:-1]
            data = json.loads(row["kana"])
            hira = data.get("hira", "")
            kana = jaconv.kata2hira(hira) if hira else ""

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
                    print(
                        f"Warning: {name} ({lat:.6f}, {lon:.6f}) is outside Japan.",
                        file=sys.stderr,
                    )
                    continue

            if row["elevation_m"] == "NULL":
                row["elevation_m"] = ""

            raw_remote_id = row["raw_remote_id"]
            row["source_uuid"] = generate_source_uuid(f"{source}_poi", raw_remote_id)

            pattern = r"(.+?)[（\(](.+?)[）\)]"
            if m := re.fullmatch(pattern, name):
                name = m.group(1).strip()
            if m := re.fullmatch(pattern, kana):
                kana = m.group(1).strip()
            row["name"] = name
            row["kana"] = kana
            writer.writerow(row)

except FileNotFoundError:
    print(f"Error: '{tsv_file}' not found.", file=sys.stderr)
except csv.Error as e:
    print(f"CSV Error: {e}", file=sys.stderr)
except json.JSONDecodeError as e:
    print(f"JSON Decode Error: {e}", file=sys.stderr)
except Exception as e:
    print(f"Error processing file: {e}", file=sys.stderr)
finally:
    cursor.close()
    conn.close()

# __END__
