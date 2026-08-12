#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 日本の主な山岳情報（1003山）をTSV形式に変換するスクリプト

import json
from argparse import ArgumentParser

import regex
from shared import extract_aliases

parser = ArgumentParser(description="日本の主な山岳情報（1003山）をTSV形式に変換")
parser.add_argument("geojson_file", help="GeoJSONファイルのパス")
args = parser.parse_args()
geojson_file = args.geojson_file


def extract_names_and_kanas(name_str, kana_str):
    m_name = regex.fullmatch(r"^([^（]*?)(?:（.*）)?＜(.*)＞$", name_str)
    m_kana = regex.fullmatch(r"^([^（]*?)(?:（.*）)?＜(.*)＞$", kana_str)
    if m_name and m_kana:
        area_name, names = m_name.groups()
        area_kana, kanas = m_kana.groups()
        names_json = extract_aliases(names, kanas)
        names_json.append({"type": "AREA", "name": area_name, "kana": area_kana})
    else:
        names_json = extract_aliases(name_str, kana_str)
    return names_json


fieldnames = [
    "raw_id",
    "raw_type",
    "names_json",
    "lat",
    "lon",
    "elevation",
    "z_min",
    "last_updated_at",
]

with open(geojson_file, "r", encoding="utf-8-sig") as f:
    print("\t".join(fieldnames))
    geojson = json.load(f)
    for feature in geojson["features"]:
        properties = feature["properties"]
        name_str = regex.sub(r"(\p{Han})ケ(\p{Han})", r"\1ヶ\2", properties["山名＜山頂名＞"])
        if name_str.endswith(("の頂", "の山腹")):
            continue
        kana_str = properties["山名よみ＜山頂名よみ＞"]
        names_json = extract_names_and_kanas(name_str, kana_str)
        output_row = [
            str(properties["連番"]),
            properties["種別"],
            json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
            str(properties["緯度"]),
            str(properties["経度"]),
            str(properties["標高値(m)"]),
            "",
            "",
        ]
        print("\t".join(output_row))

# __END__
