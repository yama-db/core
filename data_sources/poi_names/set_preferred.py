#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from shared import config, db_util

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
                        src.reliability_rank ASC,
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

    # 代表子POIについて、最小表示Zレベルを再設定
    cursor.execute(
        """
        SELECT
            m.id,
            m.grade
        FROM mountain_pois AS m
        JOIN poi_hierarchies AS h
            ON m.id = h.child_id
            AND h.is_representative
            AND m.is_used
        """,
    )
    rows = cursor.fetchall()
    params = []
    for row in rows:
        grade = row["grade"]
        z_min = config.z_min_mapping[grade]
        z_min = min(z_min, 11)
        params.append((z_min, row["id"]))
    if params:
        cursor.executemany(
            "UPDATE mountain_pois SET z_min = %s WHERE id = %s",
            params,
        )

    success = True

except Exception as err:
    print(f"Error during DB session: {err}", file=sys.stderr)
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
