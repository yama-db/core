#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import sys
from argparse import ArgumentParser
from itertools import product
from typing import Dict, List

import jaconv

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
        if qid in extracts:
            print(f"Warning: Duplicate extract for {qid}", file=sys.stderr)
        extracts[qid] = re.sub(
            r"^(?:この(?:項の)?)?「?(.+?)」?は、.*$", r"\1", row["extract"]
        )


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
    else:
        print(
            f"Warning: Could not extract kana for {label} from ({extract})",
            file=sys.stderr,
        )
    if kana and (m := re.match(r"(.*)あたま/かしら", kana)):
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
    if not kanas:
        kanas = [""]
    return [{"name": na, "kana": ka} for na, ka in product(names, kanas)]


with open(data_csv, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    fieldnames = [
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
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    ids = set()
    for row in reader:
        raw_id = row["item"].split("/")[-1]
        if is_first := raw_id not in ids:
            ids.add(raw_id)
        else:
            continue
        lat = lon = None
        if not (coord := row.get("coord")):
            continue
        if not (m := re.match(r"Point\(([-\d\.]+) ([-\d\.]+)\)", coord)):
            print(f"Invalid coord format for {raw_id}: {coord}", file=sys.stderr)
            continue
        lon = m.group(1)
        lat = m.group(2)
        elevation = row["elevation"] or None
        extract = extracts.get(raw_id, "").replace("\n", "")
        label = itemLabel = row["itemLabel"]
        if m := re.search(r"^(.*?)[（\(]", itemLabel):
            label = m.group(1).strip()
        names_json = extract_name_and_kana(extract, label)
        for item in names_json:
            writer.writerow(
                {
                    "raw_id": raw_id,
                    "raw_type": "山",
                    "name": item["name"].strip("「」"),
                    "kana": item["kana"],
                    "lat": lat,
                    "lon": lon,
                    "elevation": elevation,
                    "z_min": None,
                    "last_updated_at": None,
                }
            )

# __END__
