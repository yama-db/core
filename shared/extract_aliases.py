#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import sys

import Levenshtein
import regex

trans_table = str.maketrans({
    "(": "（",
    ")": "）",
    " ": "",
    "、": "・",
    "　": "・",
})

def extract_aliases(name, kana):
    data = []
    name = name.translate(trans_table)
    kana = kana.translate(trans_table)
    if not (regex.findall(r'[^\p{L}\p{N}]', name) or regex.findall(r'[^\p{L}\p{N}]', kana)):
        data.append((name, kana))
        return data
    m_name = re.fullmatch(r'(.+)[（\(](.+?)[）\)]', name)
    m_kana = re.fullmatch(r'(.+)[（\(](.+?)[）\)]', kana)
    if m_name and (
        m_name.group(2) in ["東峰", "西峰", "南峰", "北峰", "中央峰", "主峰", "中峰", "南岳", "三角点", "Ⅳ峰"]
        or m_name.group(2).endswith("ピーク")
    ):
        # 親峰（支峰）
        if m_kana:
            # FIXME: m_kana.group(2) は m_name.group(2) の読みと想定
            for k in m_kana.group(1).split("・"):
                data.append((m_name.group(1), k))
        else:
            for k in kana.split("・"):
                data.append((name, k))
        return data
    if m_name and m_kana:
        # 名称（別称）
        s_name = m_name.group(1).split("・")
        s_kana = m_kana.group(1).split("・")
        if len(s_name) == len(s_kana):
            for n, k in zip(s_name, s_kana):
                data.append((n, k))
        elif len(s_name) == 1:
            for k in s_kana:
                data.append((m_name.group(1), k))
        else:
            data.append((m_name.group(1), m_kana.group(1)))
        # 別称
        s_name = m_name.group(2).split("・")
        s_kana = m_kana.group(2).split("・")
        if len(s_name) == len(s_kana):
            for n, k in zip(s_name, s_kana):
                data.append((n, k))
        elif len(s_name) == 1:
            for k in s_kana:
                data.append((m_name.group(2), k))
        else:
            for n in s_name:
                if not n.endswith(("市", "町")):
                    data.append((n, m_kana.group(1)))
        return data
    if m_name:
        s_name = m_name.group(2).split("・")
        if len(s_name) > 1:
            data.append((m_name.group(1), kana))
            for n in s_name:
                if not n.endswith(("市", "町")):
                    k = kana if Levenshtein.jaro_winkler(n, m_name.group(1)) >= 0.6 else None
                    data.append((n, k))
            return data
        for i in range(1, 3):
            k = kana if i == 1 or Levenshtein.jaro_winkler(m_name.group(i), m_name.group(1)) >= 0.6 else None
            data.append((m_name.group(i), k))
        return data
    if m_kana:
        for k in m_kana.group(1).split("・"):
            data.append((name, k))
        return data

    s_name = name.split("・")
    s_kana = kana.split("・")
    if len(s_name) == len(s_kana):
        for n, k in zip(s_name, s_kana):
            data.append((n, k))
        return data
    if len(s_name) == 1:
        for k in s_kana:
            data.append((name, k))
        return data

    data.append((name, kana))
    return data


if __name__ == "__main__":
    try:
        tsv_file = "../data_sources/yamap/archive/yamap.tsv"
        with open(tsv_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter="\t")
            writer = csv.DictWriter(sys.stdout, fieldnames=["raw_remote_id", "name", "kana"])
            writer.writeheader()
            for row in reader:
                raw_remote_id = row["raw_remote_id"]
                name = row["name"]
                kana = json.loads(row["kana"])["hira"]
                if not kana:
                    continue
                for n, k in extract_aliases(name, kana):
                    writer.writerow({
                        "raw_remote_id": raw_remote_id,
                        "name": n,
                        "kana": k,
                    })
    except FileNotFoundError:
        print(f"Error: '{tsv_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except csv.Error as e:
        print(f"CSV Error: {e}", file=sys.stderr)
        sys.exit(1)

# __END__
