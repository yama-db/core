#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from argparse import ArgumentParser
from pathlib import Path

# コマンドライン引数の解析
parser = ArgumentParser(description="行政区域境界のGeoJSONファイルをDBに登録")
parser.add_argument("geojson_file", help="行政区域境界のGeoJSONファイル・パス")
parser.add_argument("sql_file", help="出力先のSQLファイル・パス")
args = parser.parse_args()
geojson_file = args.geojson_file
sql_file = args.sql_file

table_name = "administrative_boundaries"

try:
    with open(geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"File not found: {geojson_file}", file=sys.stderr)
except json.JSONDecodeError as e:
    print(f"Error parsing JSON file: {e}", file=sys.stderr)
    sys.exit(1)

try:
    with open(sql_file, "w", encoding="utf-8") as f:
        for feature in data["features"]:
            properties = feature["properties"]
            jis_code = properties["N03_007"]
            geometry_json = json.dumps(feature["geometry"])
            print(
                f"INSERT IGNORE INTO {table_name} (jis_code, geom) VALUES ('{jis_code}', ST_GeomFromGeoJSON('{geometry_json}'));",
                file=f,
            )
except KeyError as e:
    print(f"Missing expected key in GeoJSON data: {e}", file=sys.stderr)
    sys.exit(1)

# __END__
