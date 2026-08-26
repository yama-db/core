#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import defaultdict

from shared import db_util

# MySQL接続の確立
conn = None
cursor = None
success = False

try:
    conn, cursor = db_util.db_open()

    cursor.execute("SELECT src_char FROM char_trans_map")
    src_chars = {row["src_char"] for row in cursor.fetchall()}

    cursor.execute("SELECT poi_name, COUNT(*) AS cnt FROM poi_names GROUP BY poi_name")
    poi_data = cursor.fetchall()

    counts = defaultdict(int)
    samples = defaultdict(list)

    for row in poi_data:
        name = row["poi_name"]
        cnt = row["cnt"]
        matched_chars = set(name) & src_chars
        for c in matched_chars:
            counts[c] += cnt
            if len(samples[c]) < 20:  # 上位20件を保持
                samples[c].append(name)

    update_data = [(counts[c], ",".join(samples[c]), c) for c in src_chars]
    cursor.executemany(
        """
        UPDATE char_trans_map
        SET hit_count = %s, sample_names = %s
        WHERE src_char = %s
        """,
        update_data,
    )
    success = True  # 成功フラグを立てる

except Exception as err:
    print(f"Error during DB session: {err}")
    raise
finally:
    if conn or cursor:
        db_util.db_close(conn, cursor, success=success)

# __END__
