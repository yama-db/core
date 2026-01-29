#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import sys
from argparse import ArgumentParser
from itertools import product
from typing import Dict, List

import jaconv
from shared import generate_source_uuid

parser = ArgumentParser(description="Convert Wikidata CSV to internal format")
parser.add_argument("data_csv", help="Input CSV file from Wikidata query")
parser.add_argument("pedia_csv", help="Input CSV file from Wikipedia query")
args = parser.parse_args()
data_csv = args.data_csv
pedia_csv = args.pedia_csv

extracts = {}

with open(pedia_csv, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        item = row["item"]
        qid = item.split("/")[-1]
        extracts[qid] = row["extract"]


def extract_name_and_kana(extract: str, label: str) -> List[Dict[str, str]]:
    if m := re.search(r"^(.*?)[（\(]", label):
        label = m.group(1).strip()
    name = label
    kana = ""
    if m := re.fullmatch(r"([\u30A1-\u30FFー]+)(山?)", label):  # katakana only
        kana = m.group(1) + ("やま" if m.group(2) == "山" else "")
    elif m := re.search(r"^(.*?)（(.*?)）（([\u3041-\u3096ー]*?)）", extract):
        name = m.group(1).strip()
        kana = m.group(3).strip()
    elif m := re.search(r"^(.*?)[（\(](.*?)[）\)。]", extract):
        name = m.group(1).strip()
        kana = m.group(2).strip()
    if m := re.match(r"(.*)あたま/かしら", kana):
        kana = jaconv.kata2hira(m.group(1).strip())
        return [
            {"name": name, "kana": kana + "あたま"},
            {"name": name, "kana": kana + "かしら"},
        ]

    names = name.replace("または", "、").split(
        "、"
    )  # NOTE: 'または' を含む山名がないこと
    kanas = []
    for ka in re.sub(r"[／・/･]", "、", kana).split("、"):
        hira = jaconv.kata2hira(re.sub(r"\s+", "", ka))
        if re.fullmatch(r"[\u3041-\u3096ー]+", hira):
            kanas.append(hira)
    return [{"name": na, "kana": ka} for na, ka in product(names, kanas)]


with open(data_csv, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    fieldnames = [
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
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    ids = set()
    for row in reader:
        raw_remote_id = row["item"].split("/")[-1]
        if is_first := raw_remote_id not in ids:
            ids.add(raw_remote_id)
        else:
            continue
        source_uuid = generate_source_uuid("wikidata_poi", raw_remote_id)
        lat = lon = None
        if coord := row.get("coord"):
            if m := re.match(r"Point\(([-\d\.]+) ([-\d\.]+)\)", coord):
                lon = m.group(1)
                lat = m.group(2)
        elevation_m = row["elevation"] or None
        extract = extracts.get(raw_remote_id, "").replace("\n", "")
        label = itemLabel = row["itemLabel"]
        if m := re.search(r"^(.*?)[（\(]", itemLabel):
            label = m.group(1).strip()
        names_json = extract_name_and_kana(extract, label)
        for i, name_kana in enumerate(names_json):
            writer.writerow(
                {
                    "source_uuid": source_uuid if i == 0 else None,
                    "raw_remote_id": raw_remote_id,
                    "name": name_kana["name"],
                    "kana": name_kana["kana"],
                    "lat": lat,
                    "lon": lon,
                    "elevation_m": elevation_m,
                    "poi_type_raw": None,
                    "last_updated_at": None,
                }
            )

# __END__
