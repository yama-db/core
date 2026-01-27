#!/usr/bin/env python3
# -*- config: utf-8 -*-

import csv
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
wd = pandas.read_csv(
    wikidata_file,
).astype({"item": str, "itemLabel": str, "parentTaxon": int})

# 東京都庁は東京都と重複しているので除外する
wd = wd[wd["itemLabel"] != "東京都庁"]
# 全国市区町村コードの末尾1桁を削除して、行政区域コードに変換する
wd["code"] = wd["parentTaxon"] // 10
# WikidataのQIDを抽出する
wd["qid"] = wd["item"].str.replace("http://www.wikidata.org/entity/", "")

# 国土数値情報ダウンロードサービスから取得した行政区域コードExcelファイルを読み込む
ksj = pandas.read_excel(
    ksj_file,
    sheet_name="行政区域コード",
    usecols=[0, 1, 2, 5],
    skiprows=[0, 1],
    names=["code", "prefecture", "city", "revision"],
).astype({"code": int, "prefecture": str, "city": str, "revision": str})

# 変更履歴がないものだけに絞る
ksj = ksj[ksj["revision"] == "nan"]

# 市区町村名から"nan"を削除する
ksj["city"] = ksj["city"].str.replace("nan", "")

# Wikidataのデータと結合する
merged_ksj = pandas.merge(ksj, wd, on="code", how="left")
# codeを5桁の文字列に変換する
merged_ksj["code"] = merged_ksj["code"].map("{:05d}".format)

merged_ksj.to_csv(
    sys.stdout,
    index=False,
    header=["jis_code", "pref_name", "city_name", "wikidata_qid"],
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["code", "prefecture", "city", "qid"],
)
