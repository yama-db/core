#!/usr/bin/env python3
# -*- config: utf-8 -*-

import sys
from argparse import ArgumentParser

import pandas

# 解析用のコマンドライン引数
parser = ArgumentParser(description="Generate regions CSV from Wikidata and KSJ data")
parser.add_argument("ksj_file", help="Path to the KSJ AdminiBoundary_CD.xlsx file")
parser.add_argument("wikidata_file", help="Path to the Wikidata query CSV file")
args = parser.parse_args()
ksj_file = args.ksj_file
wikidata_file = args.wikidata_file

# Wikidata Query Serviceから取得したCSVファイルを読み込む
wd = pandas.read_csv(wikidata_file, dtype=str)

# NOTE: QIDが重複する行政区域外のデータを除外する
wd = wd[~wd["itemLabel"].isin(["東京都庁", "読谷村文化センター"])]

# 全国市区町村コードの末尾1桁を削除して、行政区域コードに変換する
wd["code"] = wd["parentTaxon"].str[:5]
# WikidataのQIDを抽出する
wd["qid"] = wd["item"].str.replace("http://www.wikidata.org/entity/", "", regex=False)

# 国土数値情報ダウンロードサービスから取得した行政区域コードExcelファイルを読み込む
ksj = pandas.read_excel(
    ksj_file,
    sheet_name="行政区域コード",
    usecols=[0, 1, 2, 5],
    skiprows=[0, 1],
    names=["code", "prefecture", "city", "revision"],
    dtype=str,
)

# 変更履歴がないものだけに絞る
is_empty = ksj["revision"].isna() | (ksj["revision"] == "") | (ksj["revision"] == "nan")
ksj = ksj[is_empty]

# Wikidataのデータと結合する
merged_ksj = pandas.merge(ksj, wd, on="code", how="left")

# 都道府県のQIDを取得するために、都道府県のコードを抽出する
merged_ksj["pref_code"] = merged_ksj["code"].str[:2] + "000"

# 都道府県のQIDを取得するために、都道府県のQIDを抽出する
is_pref_row = (merged_ksj["city"] == "") | merged_ksj["city"].isna()
pref_qid_map = merged_ksj[is_pref_row].set_index("pref_code")["qid"].to_dict()

# 都道府県のQIDを結合する
merged_ksj["pref_qid"] = merged_ksj["pref_code"].map(pref_qid_map)

merged_ksj.to_csv(
    sys.stdout,
    index=False,
    header=["jis_code", "pref_name", "city_name", "city_qid", "pref_qid"],
    columns=["code", "prefecture", "city", "qid", "pref_qid"],
)

# __END__
