#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 国土地理院 基準点データ XMLファイルをTSV形式で出力

import json
import sys
from xml.etree import ElementTree as ET

namespaces = {
    "gml": "http://www.opengis.net/gml/3.2",
    "": "http://fgd.gsi.go.jp/spec/2008/FGD_GMLSchema",
}

header = [
    "raw_id",
    "raw_type",
    "names_json",
    "lat",
    "lon",
    "elevation",
    "last_updated_at",
    "grade",
]

point_grade_mapping = {
    "電子基準点": 0,
    "一等三角点": 1,
    "二等三角点": 2,
    "三等三角点": 3,
    "四等三角点": 4,
    "経緯度原点": 4,
    "地殻変動観測点": 4,
    "GPS固定点": 4,
    "標高点": 5,
    "その他": 6,
    "等高線": 7,
}

print("\t".join(header))
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
            grade = point_grade_mapping.get(raw_type, 7)  # デフォルトは7（等高線）
            names_json = {"name": name, "kana": ""}
            raw_id = pt.find("fid", namespaces).text
            lat, lon = pt.find("pos/gml:Point/gml:pos", namespaces).text.split()
            elevation = getattr(pt.find("alti", namespaces), "text", "")
            if elevation is None or elevation == "":
                print(
                    f"Warning: elevation is not defined for {name}（{raw_type}）",
                    file=sys.stderr,
                )
                continue
            last_updated_at = pt.find("devDate/gml:timePosition", namespaces).text
            output_row = [
                raw_id,
                raw_type,
                json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
                lat,
                lon,
                elevation,
                last_updated_at,
                grade,
            ]
            print("\t".join(map(str, output_row)))

# __END__
