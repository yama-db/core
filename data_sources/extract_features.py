#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mojimoji

from common_lib.utils import generate_source_uuid

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
    if gaiji_code.startswith("E"):
        assert gaiji_code in pua, f"未知のPUAコード {gaiji_code} が見つかりました。"
        gaiji_char = pua[gaiji_code]
    else:
        gaiji_char = chr(int(gaiji_code, 16))
    m = len(name)
    name = name[: (i // 2)] + gaiji_char + name[m - (j // 2) :]
    return name


def extract_features(file_path, writer):
    x = file_path.parent.name
    y = file_path.stem
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for i, feature in enumerate(data.get("features", [])):
                geometry = feature.get("geometry", {})
                geom_type = geometry.get("type", "")
                if geom_type != "Point":
                    continue
                coordinates = geometry.get("coordinates", [])
                lon, lat = coordinates if len(coordinates) == 2 else (None, None)
                properties = feature.get("properties", {})
                poi_type_raw = properties.get("type", "")
                if poi_type_raw != "山":
                    continue
                name0 = properties.get("name", "")
                kana = properties.get("kana", "")
                if "," in kana:
                    print(f"⚠️ 複数の読み仮名が見つかりました: {kana} - {file_path}", file=sys.stderr)
                    kana = kana.split(",")[0]
                lfSpanFr = properties.get("lfSpanFr", "")
                gaijiFlg = properties.get("gaijiFlg", "")
                if gaijiFlg and gaijiFlg != "0":
                    name = translate_gaiji(name0, gaijiFlg)
                else:
                    name = name0
                raw_remote_id = f"{x}-{y}-{i}"
                uuid = generate_source_uuid("gsi_natural", raw_remote_id)
                writer.writerow({
                    "source_uuid": uuid,
                    "raw_remote_id": raw_remote_id,
                    "name": name,
                    "kana": kana,
                    "lat": lat,
                    "lon": lon,
                    "elevation_m": "",
                    "poi_type_raw": poi_type_raw,
                    "last_updated_at": lfSpanFr,
                })

    except FileNotFoundError:
        print(f"❌ エラー: ファイルが見つかりません - {file_path}", file=sys.stderr)
    except json.JSONDecodeError as e:
        print(
            f"❌ エラー: JSON形式が不正です - {file_path}\n   詳細: {e}",
            file=sys.stderr,
        )
    except PermissionError:
        print(f"❌ エラー: 読み取り権限がありません - {file_path}", file=sys.stderr)
    except Exception as e:
        print(f"❌ エラー: 想定外のエラー - {file_path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    # コマンドライン引数の解析
    parser = ArgumentParser(description="ベクタータイルから注記を抽出してCSVファイルに出力します")
    parser.add_argument("tiles_dir", help="ベクタータイルが格納されているディレクトリのパス")
    args = parser.parse_args()

    header = [
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
    writer = csv.DictWriter(
        sys.stdout, fieldnames=header, quotechar="'", quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()

    base_dir = Path(args.tiles_dir) / "15"

    for sub_dir in base_dir.iterdir():
        if not sub_dir.is_dir():
            continue
        for file_path in sub_dir.iterdir():
            if not (file_path.is_file() and file_path.suffix == ".geojson" and file_path.stat().st_size > 0):
                continue
            extract_features(file_path, writer)

# __END__
