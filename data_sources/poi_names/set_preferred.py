#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from shared import db_util

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    # 既存の優先名称フラグをリセット
    cursor.execute("UPDATE poi_names SET is_preferred = 0")

    # 優先名称の設定
    cursor.execute(
        """
        WITH target_names AS (
            SELECT DISTINCT
                mountain_id,
                poi_name,
                poi_kana
            FROM poi_names
            WHERE source_id = 9 AND name_type = 'MAIN'
        ),
        ranked_sources AS (
            SELECT 
                pn.id,
                ROW_NUMBER() OVER (
                    PARTITION BY pn.mountain_id, pn.poi_name, pn.poi_kana
                    ORDER BY 
                        CASE WHEN pn.name_type IN ('MAIN', 'AREA') THEN 0 ELSE 1 END ASC,
                        src.reliability_level ASC,
                        pn.id ASC
                ) AS rn
            FROM poi_names AS pn
            JOIN target_names AS tn 
                ON pn.mountain_id = tn.mountain_id
            AND pn.poi_name = tn.poi_name
            AND pn.poi_kana = tn.poi_kana
            JOIN information_sources AS src 
                ON pn.source_id = src.id
        )
        UPDATE poi_names AS pn
        JOIN ranked_sources AS rs ON pn.id = rs.id
        SET pn.is_preferred = IF(rs.rn = 1, 1, 0)
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
