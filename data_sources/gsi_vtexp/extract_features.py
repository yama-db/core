#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import mojimoji
from shared import generate_source_uuid

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
    if gaiji_code.startswith("E"):
        assert gaiji_code in pua, f"未知のPUAコード {gaiji_code} が見つかりました。"
        gaiji_char = pua[gaiji_code]
    else:
        gaiji_char = chr(int(gaiji_code, 16))
    m = len(name)
    name = name[: (i // 2)] + gaiji_char + name[m - (j // 2) :]
    return name


# 山名中の全角数字は半角数字に変換する
trans_table = str.maketrans("０１２３４５６７８９", "0123456789")

# 誤記訂正データの読み込み
with open("raw/gsi_vtexp_corrections.csv", "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    corrections = {
        row["raw_remote_id"]: {
            "name": row["name"],
            "kana": row["kana"],
            "name_fixed": row["name_fixed"],
            "kana_fixed": row["kana_fixed"],
        }
        for row in reader
    }


def extract_features(file_path, writer):
    x = file_path.parent.name
    y = file_path.stem
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, feature in enumerate(data["features"]):
                geometry = feature["geometry"]
                if geometry["type"] != "Point":
                    continue
                coordinates = geometry["coordinates"]
                lon, lat = coordinates if len(coordinates) == 2 else (None, None)
                properties = feature["properties"]
                poi_type_raw = properties["type"]
                if poi_type_raw != "山":
                    continue
                name = properties["name"]
                if name.endswith(("尾根", "山脈", "山地")):
                    continue
                gaijiFlg = properties["gaijiFlg"] or "0"
                if gaijiFlg != "0":
                    name = translate_gaiji(name, gaijiFlg)
                kana = properties["kana"]
                raw_remote_id = f"{x}-{y}-{index}"
                if raw_remote_id in corrections:
                    corr = corrections[raw_remote_id]
                    assert (
                        name == corr["name"]
                    ), f"名前の不一致: {name} != {corr['name']} ({raw_remote_id})"
                    assert (
                        kana == corr["kana"]
                    ), f"かなの不一致: {kana} != {corr['kana']} ({raw_remote_id})"
                    name = corr["name_fixed"]
                    kana = corr["kana_fixed"]
                uuid = generate_source_uuid("gsi_vtexp_poi", raw_remote_id)
                for i, kana in enumerate(properties["kana"].split(",")):
                    writer.writerow(
                        {
                            "source_uuid": uuid if i == 0 else None,
                            "raw_remote_id": raw_remote_id,
                            "name": name.translate(trans_table),
                            "kana": kana.strip(),
                            "lon": lon,
                            "lat": lat,
                            "elevation_m": None,
                            "poi_type_raw": poi_type_raw,
                            "last_updated_at": properties["lfSpanFr"] or None,
                        }
                    )

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
    parser = ArgumentParser(
        description="ベクタータイルから注記を抽出してCSVファイルに出力します"
    )
    parser.add_argument(
        "tiles_dir", help="ベクタータイルが格納されているディレクトリのパス"
    )
    args = parser.parse_args()

    fieldnames = [
        "source_uuid",
        "raw_remote_id",
        "name",
        "kana",
        "lon",
        "lat",
        "elevation_m",
        "poi_type_raw",
        "last_updated_at",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    base_dir = Path(args.tiles_dir) / "15"

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
            extract_features(file_path, writer)

# __END__
