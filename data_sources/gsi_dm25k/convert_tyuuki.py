#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import sys
from argparse import ArgumentParser

import regex

from convert_pua import convert_pua

# コマンドライン引数の解析
parser = ArgumentParser()
parser.add_argument("csv_file", help="注記ファイル（CSV形式）")
parser.add_argument("--aliases", action="store_true")
args = parser.parse_args()
csv_file = args.csv_file
aliases_only = args.aliases


# 度分秒形式の文字列を10進度に変換
def dms2deg(dms_str: str) -> float:
    r = re.match(r"^(\d+)(\d\d)(\d\d(\.\d+)?)$", dms_str)
    d = float(r.group(1))
    m = float(r.group(2))
    s = float(r.group(3))
    return d + (m / 60) + (s / 3600)


exclude_raw_ids = [
    "493050-419",  # 杵島山
    "543850-105",  # 𣘹原山（たらわらやま）
    "543775-165",  # 鹿島槍ヶ岳
    "553814-57",  # 裏岩管山
    "524051-24",  # 清澄山
    "533872-189",  # 八ヶ岳
]

header = [
    "raw_id",
    "raw_type",
    "names_json",
    "lat",
    "lon",
    "elevation",
    "z_min",
    "last_updated_at",
]
print("\t".join(header))

with open(csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cat1 = int(row["大分類コード"])
        cat2 = int(row["中分類コード"])
        cat3 = int(row["小分類コード"])
        raw_type = f"{cat1}-{cat2}-{cat3}"
        if raw_type != "3-1-2":
            continue  # 山のみ処理
        code = int(row["1/25_000地形図コード"])
        seq = int(row["注記番号"])
        raw_id = f"{code}-{seq}"
        if raw_id in exclude_raw_ids:
            continue
        lat = dms2deg(row["注記代表点緯度"])
        lon = dms2deg(row["注記代表点経度"])
        name = convert_pua(row["注記文字"])
        alias_flag = False
        if name.startswith("（") and name.endswith("）"):
            name = name[1:-1]
            alias_flag = True
        if name.endswith(("尾根", "山脈", "山地")):
            continue
        name = regex.sub(r"(\p{Han})ケ(\p{Han})", r"\1ヶ\2", name)
        kana = row["注記文字の読み"]
        if kana.startswith("（") and kana.endswith("）"):
            kana = kana[1:-1]
            assert alias_flag, "Alias flag mismatch"
        if alias_flag ^ aliases_only:
            continue
        names_json = [
            {"name": name, "kana": kana, "type": "ALIAS" if alias_flag else "MAIN"},
        ]
        output_row = [
            raw_id,
            raw_type,
            json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
            str(lat),
            str(lon),
            "",
            "",
            "",
        ]
        print("\t".join(output_row))


# __END__
