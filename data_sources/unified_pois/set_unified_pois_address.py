#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

import mysql.connector

EPS = 40  # 位置の許容誤差[m]

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

cursor.execute(
    "SELECT id FROM unified_pois WHERE display_lat != 0 AND display_lon != 0"
)
update_data = []
count = 0
for row in cursor.fetchall():
    id = row["id"]
    cursor.execute(
        """
        SELECT GROUP_CONCAT(DISTINCT r.pref_name ORDER BY r.jis_code SEPARATOR '・') AS prefs
        FROM unified_pois AS u
        JOIN administrative_boundaries AS b ON ST_Intersects(b.geom, ST_Buffer(u.representative_geom, %s))
        JOIN administrative_regions r USING (jis_code)
        WHERE u.id = %s
        GROUP BY u.id
        """,
        (EPS, id),
    )
    result = cursor.fetchone()
    if result and result["prefs"]:
        update_data.append((result["prefs"], id))
    else:
        print(f"ID {id} の住所情報が見つかりません。")
    count += 1
    if count % 1000 == 0:
        print(f"{count} 件処理中...")

if update_data:
    cursor.executemany(
        "UPDATE unified_pois SET address_text = %s WHERE id = %s", update_data
    )
    conn.commit()
    print(f"{len(update_data)} 件の住所情報を更新しました。")

cursor.close()
conn.close()

# __END__
