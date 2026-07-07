#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re

import pandas as pd
import requests
from bs4 import BeautifulSoup


def scrape_yamareco_list(url):
    # ページの取得
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    response.encoding = response.apparent_encoding  # 文字化け対策

    if response.status_code != 200:
        print(
            f"Error: ページを取得できませんでした（ステータスコード: {response.status_code}）"
        )
        return

    # パース（解析）
    soup = BeautifulSoup(response.text, "html.parser")

    # 表（table）の特定
    table = soup.find("table", {"class": "ptlist"})

    if not table:
        print("Error: 目的の表が見つかりませんでした。")
        return

    # pandasを使ってテーブルを読み込む
    # header=0 で最初の行をヘッダーとして認識
    df_list = pd.read_html(str(table), header=0)
    df = df_list[0]

    # データのクリーンアップと整形
    if "名前" in df.columns:

        def split_mountain_name(text):
            if pd.isna(text):
                return text, ""

            # 正規表現の解説:
            # ^(.*?): 行頭から最短一致で任意の文字列（山名）をキャプチャ
            # （(.*?)）: 全角括弧の中身（読み仮名）をキャプチャ
            # ?: 括弧の部分はあってもなくても良い（読み仮名がない場合に対応）
            match = re.search(r"^(.*)（(.*?)）$", str(text))

            if match:
                name = match.group(1).strip()
                kana = match.group(2).strip()
                return name, kana
            else:
                # 括弧がない場合は、そのままの名前を返し、読みは空にする
                return str(text).strip(), ""

        # 山名と読み仮名の2列を作成
        df[["山名", "読み仮名"]] = df["名前"].apply(
            lambda x: pd.Series(split_mountain_name(x))
        )

        # 元の「名前」列が不要な場合は削除
        # df = df.drop(columns=['名前'])

    if "標高" in df.columns:

        def clean_elevation(text):
            if pd.isna(text):
                return text

            # 最初に現れる数値（整数または小数）だけを抽出
            # 例: "2038.2m var..." -> "2038.2"
            match = re.search(r"(\d+\.?\d*)", str(text))
            if match:
                return match.group(1)
            return text

        # 「標高」列にクリーンアップ処理を適用
        df["標高"] = df["標高"].apply(clean_elevation)

    # 不要な列の削除（存在する場合のみ）
    if "山行記録" in df.columns:
        df = df.drop(columns=["山行記録"])
    if "おすすめルート" in df.columns:
        df = df.drop(columns=["おすすめルート"])

    # ID列を空文字で作成
    df["ID"] = ""

    # 列を指定した順番に入れ替える
    new_order = ["項番", "山名", "読み仮名", "標高", "ID", "名前"]

    # 存在する列だけを安全に抽出（エラー回避のため）
    df = df[new_order]

    # 項目名（ヘッダー）を新しい名前に書き換える
    df.columns = [
        "raw_remote_id",
        "name",
        "kana",
        "elevation_m",
        "unified_poi_id",
        "description_text",
    ]

    # CSVへの書き出し
    output_file = "30_gifu100.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"成功: '{output_file}' に保存しました。")
    print(df.head())  # 最初の数行を表示


if __name__ == "__main__":
    target_url = "https://www.yamareco.com/modules/yamainfo/ptlist.php?groupid=118"
    scrape_yamareco_list(target_url)

# __END__
