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
args = parser.parse_args()
source = args.source

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

reader = csv.DictReader(sys.stdin, delimiter="\t")
fieldnames = ["source_uuid"] + reader.fieldnames
writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
writer.writeheader()
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
            print(
                f"Warning: {name} ({lat:.6f}, {lon:.6f}) is outside Japan.",
                file=sys.stderr,
            )
            continue

    if row["elevation_m"] == "NULL":
        row["elevation_m"] = ""

    raw_remote_id = row["raw_remote_id"]
    row["source_uuid"] = generate_source_uuid(f"{source}_poi", raw_remote_id)

    try:
        data = json.loads(row["kana"])
    except json.JSONDecodeError:
        print(f"JSON Decode Error: {row['kana']}", file=sys.stderr)
        data = {}
    hira = data.get("hira") or ""
    kana = jaconv.kata2hira(hira) if hira else ""
    m_name = re.fullmatch(r"(.+?)[（\(](.+?)[）\)]", name)
    m_kana = re.fullmatch(r"(.+?)[（\(](.+?)[）\)]", kana)
    if m_name and m_kana:
        row["name"] = m_name.group(1).strip()
        row["kana"] = m_kana.group(1).strip()
        writer.writerow(row)
        row["name"] = m_name.group(2).strip()
        row["kana"] = m_kana.group(2).strip()
        writer.writerow(row)
    elif m_name:
        row["name"] = m_name.group(1).strip()
        row["kana"] = kana
        writer.writerow(row)
        row["name"] = m_name.group(2).strip()
        row["kana"] = ""
        writer.writerow(row)
    elif m_kana:
        row["name"] = name
        row["kana"] = m_kana.group(1).strip()
        writer.writerow(row)
        row["kana"] = m_kana.group(2).strip()
        writer.writerow(row)
    else:
        row["name"] = name
        for k in kana.split("・"):
            row["kana"] = k
            writer.writerow(row)

cursor.close()
conn.close()

# __END__
