#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from pathlib import Path

import Levenshtein
import mysql.connector

EPS = 100  # 位置の許容誤差[m]


def main():

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

    # 現在の最大IDを取得
    cursor.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM unified_pois")
    max_id = cursor.fetchone()["max_id"]
    print(f"Current max unified_pois.id: {max_id}")

    # YamapのPOIで、まだunified_poisに登録されていないものを処理
    cursor.execute(
        """
        SELECT
            source_uuid,
            raw_remote_id,
            names_json->>'$[0].name' AS name
        FROM stg_yamap_pois
        LEFT JOIN poi_links USING (source_uuid)
        WHERE poi_links.source_uuid IS NULL AND names_json->>'$[0].kana' <> ''
        ORDER BY raw_remote_id
        """,
    )
    count = 0
    for row in cursor.fetchall():
        source_uuid = row["source_uuid"]
        raw_remote_id = row["raw_remote_id"]
        name = row["name"]
        if name.endswith(("ピーク", "最高点", "圏峰", "城跡", "城址", "公園", "展望台")):
            print(f"Yamap {raw_remote_id} '{name}' is ignored due to suffix.")
            continue

        # unified_poisに既に登録されているか確認
        cursor.execute(
            """
            SELECT id, representative_name
            FROM unified_pois
            WHERE ST_Within(
                representative_geom,
                ST_Buffer((
                    SELECT geom FROM stg_yamap_pois WHERE source_uuid = %s
                ), %s)
            )
            LIMIT 1
            """,
            (source_uuid, EPS)
        )
        if result := cursor.fetchone():
            id = result["id"]
            representative_name = result["representative_name"]
            print(
                f"Yamap {raw_remote_id} '{name}' is already registered as ID: {id} '{representative_name}', skipping."
            )
            continue
        # YamarecoのPOIで近接するものを探す
        cursor.execute(
            """
            SELECT
                source_uuid,
                raw_remote_id,
                names_json->>'$[0].name' AS name
            FROM stg_yamareco_pois
            WHERE ST_Within(
                geom,
                ST_Buffer((
                    SELECT geom FROM stg_yamap_pois WHERE source_uuid = %s
                ), %s)
            )
            LIMIT 1
            """,
            (source_uuid, EPS)
        )
        if not (result := cursor.fetchone()):
            print(f"No nearby Yamareco POI found for Yamap {raw_remote_id} '{name}', skipping.")
            continue
        # 名前の類似度を計算
        similarity = Levenshtein.jaro_winkler(name, result["name"])
        if similarity < 0.8:
            print(f"Yamap '{name}' and Yamareco '{result['name']}' similarity {similarity:.2f} is too low, skipping.")
            continue
        # unified_poisに登録
        count += 1
        print(f"Yamap {raw_remote_id} '{name}' is registering as new unified_poi.")
        cursor.execute(
            """
            INSERT INTO unified_pois (
                category_id,
                representative_name,
                representative_kana,
                representative_geom,
                elevation_m
            )
            SELECT
                'mountain',
                names_json->>'$[0].name',
                names_json->>'$[0].kana',
                geom,
                elevation_m
            FROM stg_yamap_pois
            WHERE source_uuid = %s
            """,
            (source_uuid,),
        )
        conn.commit()

    print(f"{count} rows inserted.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

# __END__
