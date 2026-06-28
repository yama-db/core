#!/user/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
from argparse import ArgumentParser

from shared import config, db_util

parser = ArgumentParser(description="統合POI名称を作成")
parser.add_argument(
    "table_name",
    choices=[
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
        db_util.truncate_table(cursor, 'poi_names')

    print(f"Importing POI names from {table_name}...")
    if table_name == "stg_book_web_pois":
        source_id_subquery = "s.source_id"
    else:
        source_id_subquery = f"(SELECT id AS source_id FROM information_sources WHERE source_table = '{table_name}' LIMIT 1)"

    cursor.execute(
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
        )
        SELECT 
            p.mountain_id,
            s.source_uuid,
            {source_id_subquery},
            j.poi_name,
            j.poi_name AS poi_name_normalized,
            j.poi_kana,
            IF(j.idx = 1, 'MAIN', 'ALIAS') AS name_type,
            0 AS is_preferred
        FROM `{table_name}` AS s
        JOIN poi_links AS p ON s.source_uuid = p.source_uuid
        JOIN JSON_TABLE(
            s.names_json,
            '$[*]' COLUMNS (
                idx FOR ORDINALITY,
                poi_name VARCHAR(255) PATH '$.name',
                poi_kana VARCHAR(255) PATH '$.kana'
            )
        ) AS j
        WHERE j.poi_kana IS NOT NULL AND j.poi_kana <> ''
        """,
    )

    # 優先名称の設定
    cursor.execute(
        """
        CREATE TEMPORARY TABLE min_distances AS
        SELECT 
            pl.mountain_id,
            MIN(pl.linked_distance) AS min_distance
        FROM poi_links AS pl
        JOIN poi_names AS pn ON pl.source_uuid = pn.source_uuid
        WHERE pn.name_type = 'MAIN'
            AND pn.poi_kana IS NOT NULL
            AND pn.poi_kana <> ''
        GROUP BY pl.mountain_id
        """,
    )
    cursor.execute("ALTER TABLE min_distances ADD PRIMARY KEY (mountain_id)")

    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE best_sources AS
        SELECT 
            sub.mountain_id,
            sub.source_id
        FROM (
            SELECT 
                pl.mountain_id,
                pl.source_id,
                ROW_NUMBER() OVER (
                    PARTITION BY pl.mountain_id
                    ORDER BY isrc.reliability_level
                ) as rank_idx
            FROM poi_links AS pl
            JOIN poi_names AS pn ON pl.source_uuid = pn.source_uuid
            JOIN information_sources AS isrc ON pl.source_id = isrc.id
            JOIN min_distances AS md ON pl.mountain_id = md.mountain_id
            WHERE pn.name_type = 'MAIN'
                AND pn.poi_kana IS NOT NULL
                AND pn.poi_kana <> ''
                AND pl.linked_distance <= GREATEST({config.EPS}, md.min_distance)
        ) AS sub
        WHERE sub.rank_idx = 1
        """,
    )
    cursor.execute("ALTER TABLE best_sources ADD PRIMARY KEY (mountain_id)")

    cursor.execute(
        """
        UPDATE poi_names AS pn
        JOIN poi_links AS pl ON pn.source_uuid = pl.source_uuid
        JOIN mountain_pois AS m ON pl.mountain_id = m.id
        LEFT JOIN best_sources AS bs ON pl.mountain_id = bs.mountain_id
        SET pn.is_preferred = CASE 
            WHEN bs.source_id IS NOT NULL
                AND pl.source_id = bs.source_id
                AND pn.name_type = 'MAIN'
            THEN 1 ELSE 0 
        END
        WHERE m.is_used
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
