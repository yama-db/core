#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 国土地理院 基準点データ XMLファイルをCSV形式で出力

import csv
import sys
from xml.etree import ElementTree as ET

from common_lib.utils import generate_source_uuid

namespaces = {
    "gml": "http://www.opengis.net/gml/3.2",
    "": "http://fgd.gsi.go.jp/spec/2008/FGD_GMLSchema",
}

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

writer = csv.DictWriter(
    sys.stdout, fieldnames=header, quotechar="'", quoting=csv.QUOTE_MINIMAL
)
writer.writeheader()

for f in sys.argv[1:]:
    tree = ET.parse(f)
    root = tree.getroot()
    for tag in ["GCP", "ElevPt"]:
        for pt in root.findall(tag, namespaces):
            t = pt.find("type", namespaces).text  # 種別
            if t == "電子基準点":
                name = pt.find("name", namespaces).text  # 点名称
                if not name.endswith("（付）"):
                    continue
                poi_type_raw = t
            elif t == "三角点":
                name = pt.find("name", namespaces).text  # 点名称
                poi_type_raw = pt.find("gcpClass", namespaces).text
            elif t == "標高点（測点）":
                name = ""
                poi_type_raw = "標高点"
            else:
                continue
            raw_remote_id = pt.find("fid", namespaces).text  # 基盤地図情報レコードID
            uuid = generate_source_uuid("gsi-gcp-poi", raw_remote_id)
            lat, lon = pt.find(
                "pos/gml:Point/gml:pos", namespaces
            ).text.split()  # 緯度, 経度
            elevation_m = getattr(pt.find("alti", namespaces), "text", "")  # 標高
            last_update_at = pt.find(
                "devDate/gml:timePosition", namespaces
            ).text  # 整備完了日
            writer.writerow(
                {
                    "source_uuid": uuid,
                    "raw_remote_id": raw_remote_id,
                    "name": name,
                    "kana": "",
                    "lat": lat,
                    "lon": lon,
                    "elevation_m": elevation_m,
                    "poi_type_raw": poi_type_raw,
                    "last_updated_at": last_update_at,
                }
            )

# __END__
