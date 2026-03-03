#!/user/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
from pathlib import Path

import mysql.connector

from shared import config

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


# POI名称インポート関数
def import_poi_names(table_name, source_type):
    print(f"Importing POI names from {table_name} for source type {source_type}...")
    if source_type == "BOOK":
        source_id_subquery = "s.source_id"
    else:
        source_id_subquery = f"(SELECT id AS source_id FROM information_sources WHERE display_name = '{source_type}' LIMIT 1)"

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
                0 AS is_preferred
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
    cursor.execute(
        """
        CREATE TEMPORARY TABLE min_distances AS
        SELECT 
            pl.unified_poi_id, 
            MIN(pl.distance_m) AS min_distance
        FROM poi_links AS pl
        JOIN poi_names AS pn ON pl.source_uuid = pn.source_uuid
        WHERE pn.name_type = 'MAIN'
            AND pn.name_reading IS NOT NULL
            AND pn.name_reading <> ''
        GROUP BY pl.unified_poi_id
        """,
    )
    cursor.execute("ALTER TABLE min_distances ADD PRIMARY KEY (unified_poi_id)")

    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE best_sources AS
        SELECT 
            sub.unified_poi_id,
            sub.source_id
        FROM (
            SELECT 
                pl.unified_poi_id,
                pl.source_id,
                ROW_NUMBER() OVER (
                    PARTITION BY pl.unified_poi_id 
                    ORDER BY isrc.reliability_level
                ) as rank_idx
            FROM poi_links AS pl
            JOIN poi_names AS pn ON pl.source_uuid = pn.source_uuid
            JOIN information_sources AS isrc ON pl.source_id = isrc.id
            JOIN min_distances AS md ON pl.unified_poi_id = md.unified_poi_id
            WHERE pn.name_type = 'MAIN'
                AND pn.name_reading IS NOT NULL
                AND pn.name_reading <> ''
                AND pl.distance_m <= GREATEST({config.EPS}, md.min_distance)
        ) AS sub
        WHERE sub.rank_idx = 1
        """,
    )
    cursor.execute("ALTER TABLE best_sources ADD PRIMARY KEY (unified_poi_id)")

    cursor.execute(
        """
        UPDATE poi_names AS pn
        JOIN poi_links AS pl ON pn.source_uuid = pl.source_uuid
        JOIN unified_pois AS u ON pl.unified_poi_id = u.id
        LEFT JOIN best_sources AS bs ON pl.unified_poi_id = bs.unified_poi_id
        SET pn.is_preferred = CASE 
            WHEN bs.source_id IS NOT NULL
                AND pl.source_id = bs.source_id
                AND pn.name_type = 'MAIN'
            THEN 1 ELSE 0 
        END
        WHERE u.display_lat != 0 AND u.display_lon != 0
        """,
    )
    conn.commit()

except mysql.connector.Error as e:
    print(f"MySQL Error during preferred name update: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# __END__
