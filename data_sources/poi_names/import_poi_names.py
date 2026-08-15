#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from argparse import ArgumentParser

from shared import db_util

parser = ArgumentParser(description="統合POI名称を作成")
parser.add_argument(
    "table_name",
    choices=[
        "stg_gsi_1003_pois",
        "stg_gsi_dm25k_pois",
        "stg_gsi_vtexp_pois",
        "stg_yamap_pois",
        "stg_yamareco_pois",
        "stg_wikidata_pois",
        "stg_legacy_pois",
        "stg_book_web_pois",
    ],
    help="POIデータソースのテーブル名",
)
parser.add_argument(
    "-t",
    "--truncate",
    action="store_true",
    help="既存のリンクを削除してから処理を実行 (デフォルト: False)",
)
args = parser.parse_args()
table_name = args.table_name
truncate = args.truncate

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    # 既存のPOI名称をクリア
    if truncate:
        db_util.truncate_table(cursor, "poi_names")

    print(f"Importing POI names from {table_name}...")
    if table_name == "stg_book_web_pois":
        source_id_subquery = "s.source_id"
    else:
        source_id_subquery = f"(SELECT id AS source_id FROM information_sources WHERE source_table = '{table_name}' LIMIT 1)"

    cursor.execute(
        """
        SELECT src_char, dst_char
        FROM char_trans_map
        WHERE hit_count > 0
        ORDER BY hit_count DESC
        """
    )
    rows = cursor.fetchall()
    print(f"Loaded {len(rows)} character mappings from char_trans_map.")
    normalization_table = str.maketrans({row['src_char']: row['dst_char'] for row in rows})

    cursor.execute(
        f"""
        SELECT 
            p.mountain_id,
            s.source_uuid,
            {source_id_subquery},
            j.poi_name,
            j.poi_name AS poi_name_normalized,
            j.poi_kana,
            j.poi_type AS name_type,
            0 AS is_preferred
        FROM `{table_name}` AS s
        JOIN poi_links AS p ON s.source_uuid = p.source_uuid
        JOIN JSON_TABLE(
            s.names_json,
            '$[*]' COLUMNS (
                poi_name VARCHAR(255) PATH '$.name',
                poi_kana VARCHAR(255) PATH '$.kana',
                poi_type VARCHAR(255) PATH '$.type'
            )
        ) AS j
        LEFT JOIN poi_hierarchies AS h ON p.mountain_id = h.child_id
        WHERE j.poi_kana IS NOT NULL AND j.poi_kana <> ''
        AND (h.child_id IS NULL OR h.parent_name <> j.poi_name OR j.poi_type IN ('MAIN', 'AREA'))
        """,
    )
    values = []
    for row in cursor.fetchall():
        row["poi_name_normalized"] = row["poi_name"].translate(
            normalization_table
        )
        values.append(tuple(row.values()))

    cursor.executemany(
        f"""
        INSERT INTO poi_names (
            mountain_id,
            source_uuid,
            source_id,
            poi_name,
            poi_name_normalized,
            poi_kana,
            name_type,
            is_preferred
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        values,
    )

    # 優先名称の設定
    cursor.execute(
        f"""
        WITH target_names AS (
            SELECT DISTINCT
                mountain_id,
                poi_name_normalized,
                poi_kana
            FROM poi_names
            WHERE source_id = 9 AND name_type = 'MAIN'
        ),
        ranked_sources AS (
            SELECT 
                pn.id,
                ROW_NUMBER() OVER (
                    PARTITION BY pn.mountain_id, pn.poi_name_normalized, pn.poi_kana
                    ORDER BY 
                        CASE WHEN pn.name_type IN ('MAIN', 'AREA') THEN 0 ELSE 1 END ASC,
                        src.reliability_level ASC,
                        pn.id ASC
                ) AS rn
            FROM poi_names AS pn
            JOIN target_names AS tn 
                ON pn.mountain_id = tn.mountain_id
            AND pn.poi_name_normalized = tn.poi_name_normalized
            AND pn.poi_kana = tn.poi_kana
            JOIN information_sources AS src 
                ON pn.source_id = src.id
        )
        UPDATE poi_names AS pn
        JOIN ranked_sources AS rs ON pn.id = rs.id
        SET pn.is_preferred = IF(rs.rn = 1, 1, 0);
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
