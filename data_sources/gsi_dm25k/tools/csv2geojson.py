#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import sys
import urllib.request
from io import StringIO

categories = {
    "0-0-0": "不明",
    "2-2-0": "住所",  # 大字及び住居表示
    "2-3-0": "通称地名",  # 字及び通称
    "3-1-2": "山岳",  # 山、岳、峰、丘、塚、尾根、頭
    "3-2-1": "湖沼",  # 湖、沼、池、潟、人工湖、沼、浦（水部）
    "3-2-2": "河川",  # 河川、用水、運河
    "3-2-3": "谷沢",  # 谷、沢、峡、雪渓
    "3-2-7": "滝",
    "3-2-10": "内水島",  # 島
    "3-3-1": "平原",  # 高原、原、平、原野、松原
    "3-4-1": "岩石",  # 岩、溶岩、礫、石、頭、地点名
    "3-4-2": "地形",  # 崖、崩れ、断層、地質
    "3-4-3": "洞窟",  # 鍾乳洞、風穴、岩屋
    "3-4-5": "湧水",  # 湧水、噴泉
    "3-5-1": "海域",  # 海、湾、灘、淵、浦・瀬（水部）
    "3-5-4": "海岬",  # 岬、鼻、崎、碕、半島、尻
    "3-6-3": "海島",  # 島
    "3-6-4": "岩礁",  # 瀬（陸部）、はえ、岩礁、根
    "4-1-5": "歩道",  # 自然歩道、登山道
    "4-1-9": "橋梁",  # 橋
    "4-1-10": "隧道",  # トンネル、覆道、洞門
    "4-1-11": "峠道",  # 峠、坂、越
    "5-1-1": "公園",  # 庭園、緑地、公園運動、公園、競技場、射撃場
    "5-1-2": "ゴルフ",  # ゴルフ場
    "5-1-12": "産業",  # 工場、倉庫、造船所、石油備蓄基地
    "5-2-1": "ダム",  # ダム
    "5-2-5": "神社",  # 神社、鳥居
    "5-2-6": "寺院",  # 寺院、教会、庵、祠、石仏、観音、不動尊
    "5-2-8": "遺跡",  # 城址、遺跡、貝塚、旧跡
    "5-2-9": "墓地",  # 墓地、陵墓、納骨堂、古墳
    "5-2-11": "樹木",  # 独立樹、記念樹、並木
    "5-2-13": "電力",  # 発電所、変電所
    "7-3-1": "山小屋",  # 山小屋、ヒュッテ、ロッヂ、避難小屋
}


def convert_csv_to_geojson(url, output_file):
    try:
        # 1. URLからデータを取得
        print(f"Fetching data from: {url}...")
        with urllib.request.urlopen(url) as response:
            content = response.read().decode("utf-8")

        # 2. CSVとして解析
        f = StringIO(content)
        reader = csv.DictReader(f)

        # ヘッダーの正規化（小文字・トリミング）
        fieldnames = [field.strip().lower() for field in reader.fieldnames]

        # 必要なカラムの存在チェック
        required = {"lat", "lon"}
        if not required.issubset(set(fieldnames)):
            print(f"Error: CSVに必須カラム 'lat', 'lon' が含まれていません。")
            print(f"検出されたカラム: {reader.fieldnames}")
            sys.exit(1)

        features = []
        for row in reader:
            # カラム名を小文字でアクセスするためのマッピング
            row_lower = {k.strip().lower(): v for k, v in row.items()}

            try:
                lat = float(row_lower.get("lat"))
                lon = float(row_lower.get("lon"))
                cat1 = int(row_lower.get("cat1", 0))
                cat2 = int(row_lower.get("cat2", 0))
                cat3 = int(row_lower.get("cat3", 0))
                key = f"{cat1}-{cat2}-{cat3}"
                category = categories.get(key, "不明")
                gaiji = row_lower.get("gaiji", "")
            except (ValueError, TypeError):
                # 数値でないデータ（ヘッダー再掲や空行）はスキップ
                continue

            # GeoJSON Featureの構成
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],  # GeoJSONは [経度, 緯度] の順
                },
                "properties": {
                    "name": row_lower.get("name", ""),
                    "よみ": row_lower.get("kana", ""),
                    "外字": gaiji if gaiji.startswith("❗") else ",".join(gaiji),
                    "種別": category,
                    "_iconUrl": "https://map.jpn.org/icon/951003.png",
                    "_iconSize": [24, 24],
                    "_iconAnchor": [12, 12],
                },
            }
            features = features + [feature]

        # 3. GeoJSON 構造の作成
        geojson = {"type": "FeatureCollection", "features": features}

        # 4. ファイルへ保存
        with open(output_file, "w", encoding="utf-8") as f_out:
            json.dump(geojson, f_out, ensure_ascii=False, indent=2)

        print(f"Success! Saved {len(features)} features to {output_file}")

    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="URLからCSVを読み込みGeoJSONに変換します。"
    )
    parser.add_argument("url", help="対象となるCSVのURL")
    parser.add_argument(
        "-o",
        "--output",
        default="output.geojson",
        help="出力ファイル名 (デフォルト: output.geojson)",
    )

    args = parser.parse_args()
    convert_csv_to_geojson(args.url, args.output)

# __END__
