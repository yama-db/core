#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path

parser = ArgumentParser(description="POIのTSVファイルを修正")
parser.add_argument("csv_file", help="修正データのCSVファイル・パス")
args = parser.parse_args()
csv_file = args.csv_file

# 誤記訂正データの読み込み
try:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        corrections = {
            row["raw_id"]: {
                "name": row["name"],
                "kana": row["kana"],
                "name_fixed": row["name_fixed"],
                "kana_fixed": row["kana_fixed"],
            }
            for row in reader
        }
except FileNotFoundError:
    print(f"❌ ファイルが見つかりません: {csv_file}", file=sys.stderr)
    sys.exit(1)

reader = csv.DictReader(sys.stdin, delimiter="\t")
print("\t".join(reader.fieldnames))
while row := next(reader, None):
    raw_id = row["raw_id"]
    if raw_id in corrections:
        corrected = corrections[raw_id]
        names_json = json.loads(row["names_json"])
        for item in names_json:
            if item["name"] == corrected["name"]:
                item["name"] = corrected["name_fixed"]
            if item.get("kana") == corrected["kana"]:
                item["kana"] = corrected["kana_fixed"]
        row["names_json"] = json.dumps(
            names_json, separators=(",", ":"), ensure_ascii=False
        )
    print("\t".join(row.values()))


# __END__
