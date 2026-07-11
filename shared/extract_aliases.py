#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import re
import sys

import regex

# 副峰の名称と読み
sub_peak_list = [
    {"name": "東峰", "kana": ["とうほう", "ひがしみね", "ひがしほう"]},
    {"name": "西峰", "kana": ["せいほう", "にしみね", "にしほう"]},
    {"name": "南峰", "kana": ["なんぽう", "みなみみね", "みなみほう"]},
    {"name": "北峰", "kana": ["ほくほう", "ほっぽう", "きたみね", "きたほう"]},
    {"name": "東峯", "kana": ["とうほう", "ひがしみね"]},
    {"name": "西峯", "kana": ["せいほう", "にしみね", "にしほう"]},
    {"name": "南峯", "kana": ["なんぽう", "みなみみね"]},
    {"name": "北峯", "kana": ["ほくほう", "ほっぽう", "きたみね"]},
    {"name": "東嶺", "kana": ["とうれい"]},
    {"name": "西嶺", "kana": ["せいれい"]},
    {"name": "南嶺", "kana": ["なんれい"]},
    {"name": "北嶺", "kana": ["ほくれい"]},
    {"name": "東岳", "kana": ["ひがしだけ"]},
    {"name": "西岳", "kana": ["にしだけ"]},
    {"name": "南岳", "kana": ["みなみだけ"]},
    {"name": "北岳", "kana": ["きただけ"]},
    {"name": "中央峰", "kana": ["ちゅうおうほう"]},
    {"name": "主峰", "kana": ["しゅほう"]},
    {"name": "中峰", "kana": ["ちゅうほう", "なかみね"]},
    {"name": "奥峰", "kana": ["おくほう", "おくみね"]},
    {"name": "前峰", "kana": ["ぜんほう", "まえみね"]},
    {"name": "男峰", "kana": ["なんぽう"]},
    {"name": "女峰", "kana": ["にょほう"]},
    {"name": "雄岳", "kana": ["おだけ"]},
    {"name": "雌岳", "kana": ["めだけ"]},
    {"name": "最高標高点", "kana": ["さいこうひょうこうてん"]},
    {"name": "最標高点", "kana": ["さいひょうこうてん"]},
    {"name": "独立標高点", "kana": ["どくりつひょうこうてん"]},
    {"name": "標高点", "kana": ["ひょうこうてん"]},
    {"name": "最高点", "kana": ["さいこうてん"]},
    {"name": "三角点峰", "kana": ["さんかくてんほう"]},
    {"name": "山頂", "kana": ["さんちょう"]},
    {"name": "一等三角点", "kana": ["いっとうさんかくてん"]},
    {"name": "二等三角点", "kana": ["にとうさんかくてん"]},
    {"name": "三等三角点", "kana": ["さんとうさんかくてん"]},
    {"name": "四等三角点", "kana": ["よんとうさんかくてん"]},
    {"name": "三角点", "kana": ["さんかくてん"]},
    {"name": "四峰", "kana": ["よんほう"]},
    {"name": "三峰", "kana": ["さんほう"]},
    {"name": "二峰", "kana": ["にほう", "にみね"]},
    {"name": "一峰", "kana": ["いちほう", "いちみね"]},
    {"name": "４峰", "kana": ["よんほう"]},
    {"name": "３峰", "kana": ["さんほう"]},
    {"name": "２峰", "kana": ["にほう", "にみね"]},
    {"name": "１峰", "kana": ["いちほう", "いちみね"]},
    {"name": "Ⅳ峰", "kana": ["よんほう"]},
    {"name": "Ⅲ峰", "kana": ["さんほう"]},
    {"name": "Ⅱ峰", "kana": ["にほう", "にみね"]},
    {"name": "Ⅰ峰", "kana": ["いちほう", "いちみね"]},
    {"name": "IV峰", "kana": ["よんほう"]},
    {"name": "III峰", "kana": ["さんほう"]},
    {"name": "II峰", "kana": ["にほう", "にみね"]},
    {"name": "I峰", "kana": ["いちほう", "いちみね"]},
    {"name": "四ノ岳", "kana": ["よんのたけ"]},
    {"name": "三ノ岳", "kana": ["さんのたけ"]},
    {"name": "二ノ岳", "kana": ["にのたけ"]},
    {"name": "一ノ岳", "kana": ["いちのたけ"]},
]

exclude_names = [
    "以東岳",
    "富良野西岳",
    "大東岳",
    "鈴北岳",
    "小東岳",
    "桧塚奥峰",
    "第一峰",
    "白倉南岳",
    "西雄岳",
    "奥雄岳",
    "荒雄岳",
]


def process_sub_peak_kana(kanas, sub_peak_kanas):
    for i in reversed(range(len(kanas))):
        kana = kanas[i]
        for sub_peak_kana in sub_peak_kanas:
            if kana.endswith(sub_peak_kana):
                if kana == sub_peak_kana:
                    if i == 0:
                        break
                    del kanas[i]
                else:
                    kanas[i] = kana.removesuffix(sub_peak_kana)
                return sub_peak_kana
    return sub_peak_kanas[0]


def process_sub_peak(names, kanas):
    results = []
    for i in reversed(range(len(names))):
        name = names[i]
        if name in exclude_names:
            continue
        for sub_peak in sub_peak_list:
            sub_peak_name = sub_peak["name"]
            if name.endswith(sub_peak_name):
                if name == sub_peak_name:
                    if i == 0:
                        break
                    del names[i]
                else:
                    names[i] = name.removesuffix(sub_peak_name)
                sub_peak_kana = process_sub_peak_kana(kanas, sub_peak["kana"])
                results.append(
                    {
                        "type": "SUB_PEAK",
                        "name": sub_peak_name,
                        "kana": sub_peak_kana,
                    }
                )
                break
    return results


# 区切り文字の正規表現パターンをコンパイル
# [\p{P}\p{S}\p{Z}] は Unicode の句読点、記号、空白文字を表す
# 連続する区切り文字は1文字と見做すため、+ を付ける
delimiters = regex.compile(r"[\p{P}\p{S}\p{Z}]+")

type_order = {"MAIN": 0, "AREA": 1, "SUB_PEAK": 2, "ALIAS": 3}


def extract_aliases(name_str, kana_str):
    names = [x for x in delimiters.split(name_str) if x != ""]
    kanas = [x for x in delimiters.split(kana_str) if x != ""]

    names_json = process_sub_peak(names, kanas)
    len_names = len(names)
    len_kanas = len(kanas)
    max_len = max(len_names, len_kanas)
    for i in range(max_len):
        name = names[i] if i < len_names else names[-1]
        kana = kanas[i] if i < len_kanas else ""
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
                # NOTE: 入力ミス修正
                kana = kana.replace("１むね", "いちみね")
                kana = kana.replace("２むね", "にみね")

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
