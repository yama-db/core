#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import sys
from argparse import ArgumentParser

import regex
from convert_pua import convert_pua

from shared import generate_source_uuid


# 度分秒形式の文字列を10進度に変換
def dms2deg(dms_str: str) -> float:
    r = re.match(r"^(\d+)(\d\d)(\d\d(\.\d+)?)$", dms_str)
    d = float(r.group(1))
    m = float(r.group(2))
    s = float(r.group(3))
    return d + (m / 60) + (s / 3600)


def main():
    parser = ArgumentParser()
    parser.add_argument("--aliases", action="store_true")
    args = parser.parse_args()
    aliases_only = args.aliases

    reader = csv.DictReader(sys.stdin)
    header = [
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
    writer = csv.DictWriter(sys.stdout, fieldnames=header)
    writer.writeheader()
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
        writer.writerow(
            {
                "raw_id": raw_id,
                "raw_type": raw_type,
                "name": name,
                "kana": kana,
                "lat": lat,
                "lon": lon,
                "elevation": None,
                "z_min": None,
                "last_updated_at": None,
            }
        )


if __name__ == "__main__":
    main()

# __END__
