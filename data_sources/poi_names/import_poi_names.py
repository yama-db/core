#!/user/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from pathlib import Path

import mysql.connector

# MySQL接続の確立
try:
    my_cnf = Path(sys.prefix).parent / ".my.cnf"
    conn = mysql.connector.connect(
        option_files=str(my_cnf),
        autocommit=False,
    )
    cursor = conn.cursor(dictionary=True)
except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
    sys.exit(1)


# POI名称インポート関数
def import_poi_names(table_name, source_type):
    print(f"Importing POI names from {table_name} for source type {source_type}...")
    if source_type == "BOOK":
        source_id_subquery = "s.source_id"
    else:
        source_id_subquery = f"(SELECT id FROM information_sources WHERE display_name = '{source_type}' LIMIT 1)"

    try:
        cursor.execute(
            f"""
            INSERT INTO poi_names (
                unified_poi_id,
                source_uuid,
                source_id,
                name_text,
                name_normalized,
                name_reading,
                name_type,
                is_preferred
            )
            SELECT 
                p.unified_poi_id,
                s.source_uuid,
                {source_id_subquery},
                j.name_text,
                j.name_text AS name_normalized,
                j.name_reading,
                IF(j.idx = 1, 'MAIN', 'ALIAS') AS name_type,
                FALSE AS is_preferred
            FROM {table_name} AS s
            JOIN poi_links AS p ON s.source_uuid = p.source_uuid
            JOIN JSON_TABLE(
                s.names_json,
                '$[*]' COLUMNS (
                    idx FOR ORDINALITY,
                    name_text VARCHAR(255) PATH '$.name',
                    name_reading VARCHAR(255) PATH '$.kana'
                )
            ) AS j
            WHERE p.source_type = %s
            """,
            (source_type,),
        )
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL Error during import: {e}")
        conn.rollback()
        sys.exit(1)


# 既存のPOI名称をクリア
try:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE poi_names")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
except mysql.connector.Error as e:
    print(f"MySQL Error during truncation: {e}")
    conn.rollback()
    sys.exit(1)

import_poi_names("stg_gsi_vtexp_pois", "GSI_VTEXP")
import_poi_names("stg_gsi_dm25k_pois", "GSI_DM25K")
import_poi_names("stg_yamap_pois", "YAMAP")
import_poi_names("stg_yamareco_pois", "YAMARECO")
import_poi_names("stg_wikidata_pois", "WIKIDATA")
import_poi_names("stg_legacy_pois", "LEGACY")
import_poi_names("stg_book_pois", "BOOK")

# 優先名称の設定
try:
    cursor.execute("UPDATE poi_names SET is_preferred = 0")
    cursor.execute(
        """
        UPDATE poi_names
        JOIN (
            SELECT 
                p.id,
                ROW_NUMBER() OVER (
                    PARTITION BY p.unified_poi_id 
                    ORDER BY 
                        s.reliability_level ASC, -- 信頼度の高い情報源を優先
                        p.id ASC                 -- 同じなら登録順
                ) as name_rank
            FROM poi_names AS p
            JOIN information_sources AS s ON p.source_id = s.id
            WHERE p.name_type = 'MAIN'      -- メイン名称のみ対象
            AND p.name_reading IS NOT NULL  -- 読みがあるものを優先
            AND p.name_reading <> ''
        ) AS ranked_names ON poi_names.id = ranked_names.id
        SET poi_names.is_preferred = 1
        WHERE ranked_names.name_rank = 1
        """,
    )
    conn.commit()
except mysql.connector.Error as e:
    print(f"MySQL Error during preferred name update: {e}")
    conn.rollback()
    sys.exit(1)

# 手動での優先名称修正の適用
try:
    with open("raw/preferred.csv", "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            unified_poi_id = row["unified_poi_id"]
            name = row["name"]
            source_id = row["source_id"]
            print(
                f"Setting preferred for ID {unified_poi_id} ({name}) to source ID {source_id}."
            )
            cursor.execute(
                f"""
                UPDATE poi_names
                SET is_preferred = CASE WHEN source_id = %s THEN 1 ELSE 0 END
                WHERE unified_poi_id = %s
                """,
                (source_id, unified_poi_id),
            )
        conn.commit()

except mysql.connector.Error as e:
    print(f"MySQL Error during altering preferred names: {e}")
    conn.rollback()
except FileNotFoundError:
    print("No preferred names corrections found.")
finally:
    cursor.close()
    conn.close()

# __END__
