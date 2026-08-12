#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 書籍のPOIファイルを stg_book_pois テーブルに登録

import csv
import json
import os
import sys
from argparse import ArgumentParser

from shared import db_util, extract_aliases, generate_source_uuid

# コマンドライン引数の解析
parser = ArgumentParser(description="書籍のCSVファイルをDBに登録")
parser.add_argument("table_name", choices=["stg_book_web_pois"], help="テーブル名")
parser.add_argument(
    "-t", "--truncate", action="store_true", help="登録前にテーブルを空にする"
)
parser.add_argument("targets", nargs="*", help="処理対象のCSVファイル (複数指定可)")
args = parser.parse_args()
table_name = args.table_name
truncate = args.truncate
targets = args.targets

if not targets:
    print("処理対象のファイルが指定されていません。", file=sys.stderr)
    sys.exit(1)

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    if truncate:
        db_util.truncate_table(cursor, table_name)

    values = []
    for csv_file in targets:
        # ファイル名から source_id を取得
        base_name = os.path.basename(csv_file)
        source_id = int(base_name.split("_")[0])

        # source_id から情報源名を取得
        cursor.execute(
            """
            SELECT display_name
            FROM information_sources
            WHERE source_table = %s AND id = %s
            """,
            (table_name, source_id),
        )
        result = cursor.fetchone()
        if result is None:
            print(
                f"Error: {csv_file} is not registered in information_sources table.",
                file=sys.stderr,
            )
            continue
        display_name = result["display_name"]
        print(f"Importing {source_id}:{display_name} from {csv_file}", file=sys.stderr)

        # CSVファイルの読み込み
        with open(csv_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_id = row["raw_id"]
                raw_type = "山"  # FIXME:書籍のPOIはすべて山として扱う
                uuid = generate_source_uuid(f"{table_name}_{source_id}_{raw_type}", raw_id)
                names_json = extract_aliases(row["name"], row["kana"])
                values.append(
                    (
                        uuid.bytes,
                        source_id,
                        raw_id,
                        raw_type,
                        row["mountain_id"] or None,
                        json.dumps(names_json, separators=(",", ":"), ensure_ascii=False),
                        row["elevation"] or None,
                    )
                )

    cursor.executemany(
        f"""
        INSERT INTO `{table_name}` (
            source_uuid, source_id, raw_id, raw_type, mountain_id, names_json, elevation
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        """,
        values,
    )
    print(f"Inserted {cursor.rowcount} rows into {table_name}")
    success = True

except Exception as err:
    print(f"Error during DB session: {err}")
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
