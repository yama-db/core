#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 国土地理院 基準点データ XMLファイルをCSV形式で出力

import csv
import sys
from xml.etree import ElementTree as ET

namespaces = {
    "gml": "http://www.opengis.net/gml/3.2",
    "": "http://fgd.gsi.go.jp/spec/2008/FGD_GMLSchema",
}

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

z_min_mapping = {
    "電子基準点": 8,
    "一等三角点": 8,
    "二等三角点": 9,
    "三等三角点": 10,
    "四等三角点": 11,
    "経緯度原点": 11,
    "地殻変動観測点": 11,
    "標高点": 12,
}

writer = csv.DictWriter(sys.stdout, fieldnames=header)
writer.writeheader()

for f in sys.argv[1:]:
    tree = ET.parse(f)
    root = tree.getroot()
    for tag in ["GCP", "ElevPt"]:
        for pt in root.findall(tag, namespaces):
            t = pt.find("type", namespaces).text
            if t == "電子基準点":
                name = pt.find("name", namespaces).text
                if not name.endswith("（付）"):
                    continue
                raw_type = t
            elif t == "三角点":
                name = pt.find("name", namespaces).text
                raw_type = pt.find("gcpClass", namespaces).text
            elif t == "標高点（測点）":
                name = ""
                raw_type = "標高点"
            elif t == "水準点" or t == "多角点":
                continue
            elif t == "その他の国家基準点":
                name = pt.find("name", namespaces).text
                raw_type = pt.find("gcpClass", namespaces).text
            else:
                print(f"Warning: unknown type '{t}' is skipped", file=sys.stderr)
                continue
            raw_id = pt.find("fid", namespaces).text
            lat, lon = pt.find("pos/gml:Point/gml:pos", namespaces).text.split()
            z_min = z_min_mapping.get(raw_type, 13)
            elevation = getattr(pt.find("alti", namespaces), "text", "")
            if elevation is None or elevation == "":
                print(
                    f"Warning: elevation is not defined for {name}（{raw_type}）",
                    file=sys.stderr,
                )
                continue
            last_update_at = pt.find("devDate/gml:timePosition", namespaces).text
            writer.writerow(
                {
                    "raw_id": raw_id,
                    "raw_type": raw_type,
                    "name": name,
                    "kana": None,
                    "lat": lat,
                    "lon": lon,
                    "elevation": elevation,
                    "z_min": z_min,
                    "last_updated_at": last_update_at,
                }
            )

# __END__
