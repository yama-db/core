#!/user/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from pathlib import Path

import mysql.connector

# 優先名称修正ファイルの読み込み
csv_file = "raw/preferred.csv"
try:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
except FileNotFoundError:
    print(f"'{csv_file}' not found. Skipping preferred names update.")
    sys.exit(1)

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        option_groups=["client", "mysql"],
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
    sys.exit(1)

for row in rows:
    unified_poi_id = row["unified_poi_id"]
    name = row["name"]
    display_name = row["display_name"]
    print(
        f"Setting preferred for ID {unified_poi_id} '{name}' to display name '{display_name}'."
    )
    try:
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
            WHERE p.unified_poi_id = %s
            """,
            (display_name, unified_poi_id),
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during preferred name update: {e}")
        conn.rollback()
        sys.exit(1)

# unified_pois の代表名称を更新（山域名は除外）
try:
    cursor.execute(
        """
        UPDATE unified_pois AS u
        JOIN poi_names AS p ON u.id = p.unified_poi_id
        LEFT JOIN poi_hierarchies AS h ON u.id = h.parent_id
        SET
            u.representative_name = p.name_text,
            u.representative_kana = p.name_reading
        WHERE p.is_preferred
            AND h.parent_id IS NULL
        """,
    )
    conn.commit()
except mysql.connector.Error as e:
    print(f"MySQL Error during altering preferred names: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# __END__
