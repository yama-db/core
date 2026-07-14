#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mojimoji
import regex
from shared import extract_aliases

# コマンドライン引数の解析
parser = ArgumentParser(
    description="ベクタータイルから注記を抽出してTSVファイルに出力します"
)
parser.add_argument(
    "tiles_dir", help="ベクタータイルが格納されているディレクトリのパス"
)
parser.add_argument("csv_file", help="修正データのCSVファイル・パス")
args = parser.parse_args()
tiles_dir = args.tiles_dir
csv_file = args.csv_file

# 誤記訂正データの読み込み
try:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        corrections = {
            row["raw_id"]: {
                "name": row["name"],
                "kana": row["kana"],
                "name_fixed": row["name_fixed"],
                "kana_fixed": row["kana_fixed"],
            }
            for row in reader
        }
except FileNotFoundError:
    print(f"❌ ファイルが見つかりません: {csv_file}", file=sys.stderr)
    sys.exit(1)

# 外字・環境依存文字リスト
# https://www.gsi.go.jp/common/000255942.pdf
pua = {
    "E028": "瘤",
    "E06E": "那",
    "E084": "蓮",
    "E090": "巽",
    "E093": "馿",
    "E01F": "び",  # さんずいに屁
}


# gaijiFlgをもとに外字・環境依存文字を変換する
def translate_gaiji(name: str, gaiji_flg: str) -> str:
    pattern = gaiji_flg.strip("()")
    i = 0
    while pattern[i : i + 2] == "*_":
        i += 2
    n = len(pattern)
    j = 0
    while pattern[n - j - 2 : n - j] == "_*":
        j += 2
    gaiji_code = mojimoji.zen_to_han(pattern[i : n - j], kana=False)
    if gaiji_code == "FA10":  # NOTE: 塚（旧字）
        return name
    if gaiji_code.startswith("E"):
        if gaiji_code not in pua:
            print(f"❌ 未知のPUAコード {gaiji_code}", file=sys.stderr)
            sys.exit(1)
        gaiji_char = pua[gaiji_code]
    else:
        gaiji_char = chr(int(gaiji_code, 16))
    m = len(name)
    return name[: (i // 2)] + gaiji_char + name[m - (j // 2) :]


exclude_raw_ids = [
    "29066-12795-0",  # 袈裟丸山
    "28979-12869-0",  # 横岳
    "28977-12872-0",  # 権現岳
    "29218-12441-1",  # 七ッ森
    "29140-12961-0",  # 清澄山
    "29123-12740-2",  # 那須岳
    "28911-12835-2",  # 天狗岩
    "29037-12755-1",  # 三ッ石山
]


def extract_features(file_path):
    x = file_path.parent.name
    y = file_path.stem
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        for index, feature in enumerate(data["features"]):
            geometry = feature["geometry"]
            if geometry["type"] != "Point":
                continue
            coordinates = geometry["coordinates"]
            lon, lat = coordinates if len(coordinates) == 2 else (None, None)
            properties = feature["properties"]
            raw_type = properties["type"]
            if raw_type != "山":
                continue
            name = properties["name"]
            if name.endswith(("尾根", "山脈", "山地")):
                continue
            gaijiFlg = properties["gaijiFlg"] or "0"
            if gaijiFlg != "0":
                name = translate_gaiji(name, gaijiFlg)
            name = regex.sub(r"(\p{Han})ケ(\p{Han})", r"\1ヶ\2", name)
            kana = properties["kana"]
            raw_id = f"{x}-{y}-{index}"
            if raw_id in exclude_raw_ids:
                continue
            if raw_id in corrections:
                corrected = corrections[raw_id]
                if not (name == corrected["name"] and kana == corrected["kana"]):
                    print(
                        f"⚠️ 修正データと不一致: {raw_id} - {name}({kana}) vs {corrected['name']}({corrected['kana']})",
                        file=sys.stderr,
                    )
                    continue
                name = corrected["name_fixed"]
                kana = corrected["kana_fixed"]

            names_json = extract_aliases(name, kana)
            row = [
                raw_id,
                raw_type,
                json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
                str(lon),
                str(lat),
                "",
                "",
                properties["lfSpanFr"] or "",
            ]
            print("\t".join(row))


fieldnames = [
    "raw_id",
    "raw_type",
    "names_json",
    "lon",
    "lat",
    "elevation",
    "z_min",
    "last_updated_at",
]
print("\t".join(fieldnames))

base_dir = Path(tiles_dir) / "15"

for sub_dir in base_dir.iterdir():
    if not sub_dir.is_dir():
        continue
    for file_path in sub_dir.iterdir():
        if not (
            file_path.is_file()
            and file_path.suffix == ".geojson"
            and file_path.stat().st_size > 0
        ):
            continue
        try:
            extract_features(file_path)
        except Exception as e:
            print(f"❌ エラー: 想定外のエラー - {file_path}: {e}", file=sys.stderr)

# __END__
