#!/user/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from argparse import ArgumentParser

from shared import db_util

# コマンドライン引数の解析
parser = ArgumentParser(description="POIのCSVファイルをDBに登録")
parser.add_argument("csv_file", help="POIのCSVファイル・パス")
args = parser.parse_args()
csv_file = args.csv_file

# 優先名称修正ファイルの読み込み
try:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
except FileNotFoundError:
    print(f"'{csv_file}' not found. Skipping preferred names update.")
    sys.exit(1)

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    for row in rows:
        mountain_id = row["mountain_id"]
        name = row["name"]
        display_name = row["display_name"]
        print(
            f"Setting preferred for ID {mountain_id} '{name}' to display name '{display_name}'.",
            file = sys.stderr
        )
        cursor.execute(
            f"""
            UPDATE poi_names AS p
            JOIN information_sources AS s ON p.source_id = s.id
            SET p.is_preferred = (
                CASE
                    WHEN s.display_name = %s AND p.name_type = 'MAIN' THEN 1
                    ELSE 0
                END
            )
            WHERE p.mountain_id = %s
            """,
            (display_name, mountain_id),
        )

    # unified_pois の代表名称を更新（山域名は除外）
    cursor.execute(
        """
        UPDATE mountain_pois AS m
        JOIN poi_names AS p ON m.id = p.mountain_id
        LEFT JOIN poi_hierarchies AS h ON m.id = h.parent_id
        SET
            m.main_name = p.poi_name,
            m.main_kana = p.poi_kana
        WHERE p.is_preferred
            AND h.parent_id IS NULL
        """,
    )
    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
