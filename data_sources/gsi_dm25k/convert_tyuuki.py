#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import sys

from shared import generate_source_uuid

from convert_pua import convert_pua


#
# 度分秒形式の文字列を10進度に変換
#
def dms2deg(dms_str: str) -> float:
    r = re.match(r"^(\d+)(\d\d)(\d\d(\.\d+)?)$", dms_str)
    d = float(r.group(1))
    m = float(r.group(2))
    s = float(r.group(3))
    return d + (m / 60) + (s / 3600)


def main():
    reader = csv.DictReader(sys.stdin)
    header = [
        "source_uuid",
        "raw_remote_id",
        "name",
        "kana",
        "lat",
        "lon",
        "elevation_m",
        "poi_type_raw",
        "last_updated_at",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=header)
    writer.writeheader()
    for row in reader:
        cat1 = int(row["大分類コード"])
        cat2 = int(row["中分類コード"])
        cat3 = int(row["小分類コード"])
        poi_type_raw = f"{cat1}-{cat2}-{cat3}"
        if poi_type_raw != "3-1-2":
            continue  # 山のみ処理
        code = int(row["1/25_000地形図コード"])
        seq = int(row["注記番号"])
        raw_remote_id = f"{code}-{seq}"
        lat = dms2deg(row["注記代表点緯度"])
        lon = dms2deg(row["注記代表点経度"])
        name = convert_pua(row["注記文字"])
        alias_flag = False
        if name.startswith("（") and name.endswith("）"):
            name = name[1:-1]
            alias_flag = True
        kana = row["注記文字の読み"]
        if kana.startswith("（") and kana.endswith("）"):
            kana = kana[1:-1]
            assert alias_flag, "Alias flag mismatch"
        uuid = (
            generate_source_uuid("gsi_dm25k_poi", raw_remote_id)
            if not alias_flag
            else ""
        )
        writer.writerow(
            {
                "source_uuid": uuid,
                "raw_remote_id": raw_remote_id,
                "name": name,
                "kana": kana,
                "lat": lat,
                "lon": lon,
                "elevation_m": "",
                "poi_type_raw": poi_type_raw,
                "last_updated_at": "",
            }
        )


if __name__ == "__main__":
    main()

# __END__
