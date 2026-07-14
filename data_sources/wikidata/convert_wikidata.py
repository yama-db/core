#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import sys
from argparse import ArgumentParser
from datetime import datetime
from itertools import product
from typing import Dict, List

import jaconv

parser = ArgumentParser(description="Convert Wikidata CSV to internal format")
parser.add_argument("data_csv", help="Input CSV file from Wikidata query")
parser.add_argument("pedia_csv", help="Input CSV file from Wikipedia query")
args = parser.parse_args()
data_csv = args.data_csv
pedia_csv = args.pedia_csv

pedia = {}

with open(pedia_csv, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        item = row["item"]
        qid = item.split("/")[-1]
        if qid in pedia:
            print(f"Warning: Duplicate extract for {qid}", file=sys.stderr)
        extract = re.sub(
            r"^(?:この(?:項の)?)?「?(.+?)」?は、.*$",
            r"\1",
            row["extract"].replace("\n", ""),
        )
        timestamp = datetime.fromisoformat(row["timestamp"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        pedia[qid] = {
            "extract": row["extract"],
            "timestamp": row["timestamp"],
        }


def extract_name_and_kana(extract: str, label: str) -> List[Dict[str, str]]:
    formatted_extract = re.sub(
        r"^(?:この(?:項の)?)?「?(.+?)」?は、.*$", r"\1", extract.replace("\n", "")
    )
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
    else:
        print(
            f"Warning: Could not extract kana for {label} from ({extract})",
            file=sys.stderr,
        )
    if kana and (m := re.match(r"(.*)あたま/かしら", kana)):
        kana = jaconv.kata2hira(m.group(1).strip())
        return [
            {"name": name, "kana": kana + "あたま", "type": "ALIAS"},
            {"name": name, "kana": kana + "かしら", "type": "ALIAS"},
        ]

    names = name.replace("または", "、").split(
        "、"
    )  # NOTE: 'または' を含む山名がないこと
    kanas = []
    for ka in re.sub(r"[／・/･]", "、", kana).split("、"):
        hira = jaconv.kata2hira(re.sub(r"\s+", "", ka))
        if re.fullmatch(r"[\u3041-\u3096ー]+", hira):
            kanas.append(hira)
    if not kanas:
        kanas = [""]
    return [
        {"name": na, "kana": ka, "type": "ALIAS"} for na, ka in product(names, kanas)
    ]


exclude_raw_ids = [
    "Q11366434",  # 中津山地
    "Q11442517",  # 天子山地
    "Q11431086",  # 多賀火山
    "Q2618895",  # 阿寒岳
]

with open(data_csv, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
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
    print("\t".join(fieldnames))
    ids = set()
    for row in reader:
        raw_id = row["item"].split("/")[-1]  # Wikidata QID
        if raw_id in exclude_raw_ids:
            continue
        if raw_id in ids:
            continue
        ids.add(raw_id)
        lat = lon = None
        if not (coord := row.get("coord")):
            continue
        if not (m := re.match(r"Point\(([-\d\.]+) ([-\d\.]+)\)", coord)):
            print(f"Invalid coord format for {raw_id}: {coord}", file=sys.stderr)
            continue
        lon = m.group(1)
        lat = m.group(2)
        elevation = row["elevation"] or ""
        pedia_entry = pedia.get(raw_id)
        if not pedia_entry:
            print(f"No extract found for {raw_id}", file=sys.stderr)
            continue
        timestamp = pedia_entry.get("timestamp")
        formatted_timestamp = datetime.fromisoformat(timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        extract = pedia_entry.get("extract")
        label = itemLabel = row["itemLabel"]
        if m := re.search(r"^(.*?)[（\(]", itemLabel):
            label = m.group(1).strip()
        names_json = extract_name_and_kana(extract, label)
        names_json[0]["type"] = "MAIN"  # 最初の種別を MAIN に設定
        row = [
            raw_id,
            "山",
            json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
            lat,
            lon,
            elevation,
            "",
            formatted_timestamp,
        ]
        print("\t".join(row))

# __END__
