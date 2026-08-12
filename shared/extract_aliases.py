#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import re
import sys

import regex

# 区切り文字の正規表現パターンをコンパイル
# [\p{P}\p{S}\p{Z}] は Unicode の句読点、記号、空白文字を表す
# 連続する区切り文字は1文字と見做すため、+ を付ける
delimiters = regex.compile(r"[\p{P}\p{S}\p{Z}]+")

type_order = {"MAIN": 0, "AREA": 1, "SUB_PEAK": 2, "ALIAS": 3}

yama_readings = ("やま", "さん", "ざん", "せん", "ぜん", "うら")
take_readings = ("たけ", "だけ", "だっか", "だき")
mine_readings = ("みね", "ほう", "ぽう", "ぼう", "ぽ", "うら", "なる")


def extract_aliases(name_str, kana_str):
    names = [x for x in delimiters.split(name_str) if x != ""]
    kanas = [x for x in delimiters.split(kana_str) if x != ""]
    len_names = len(names)
    len_kanas = len(kanas)
    max_len = max(len_names, len_kanas)

    names_json = []
    for i in range(max_len):
        name = names[i] if i < len_names else names[-1]
        kana = kanas[i] if i < len_kanas else ""
        if name.endswith("山") and kana and not kana.endswith(yama_readings):
            print(
                f"Warning: name '{name}' ends with '山' but kana '{kana}' does not end with any of {yama_readings}.",
                file=sys.stderr,
            )
            if i > 0:
                continue
        if name.endswith(("岳", "嶽")) and kana and not kana.endswith(take_readings):
            print(
                f"Warning: name '{name}' ends with '岳' or '嶽' but kana '{kana}' does not end with any of {take_readings}.",
                file=sys.stderr,
            )
            if i > 0:
                continue
        if name.endswith(("峰", "峯")) and kana and not kana.endswith(mine_readings):
            print(
                f"Warning: name '{name}' ends with '峰' or '峯' but kana '{kana}' does not end with any of {mine_readings}.",
                file=sys.stderr,
            )
            if i > 0:
                continue
        names_json.append(
            {"type": "MAIN" if i == 0 else "ALIAS", "name": name, "kana": kana}
        )
    names_json.sort(key=lambda x: type_order.get(x["type"], 99))
    return names_json


if __name__ == "__main__":
    try:
        tsv_file = "../data_sources/yamareco/archive/yamareco.tsv"
        with open(tsv_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                raw_id = row["raw_id"]
                raw_type = json.loads(row["raw_type"])
                if 1 not in raw_type:
                    continue
                name = html.unescape(row["name"])
                if re.match(r"^[一-四]等三角点", name):
                    continue
                kana = json.loads(row["kana"])["hira"]
                names_json = extract_aliases(name, kana)
                print(f"{raw_id}:{name} ({kana})")
                for item in names_json:
                    print(f"\t{item['type']}: {item['name']} ({item['kana']})")

    except FileNotFoundError:
        print(f"Error: '{tsv_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except csv.Error as e:
        print(f"CSV Error: {e}", file=sys.stderr)
        sys.exit(1)

# __END__
