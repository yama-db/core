#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import mysql.connector


class CrawlerDB:
    # queue status
    STATUS_QUEUED = 0  # 未調査 (初期状態)
    STATUS_SUCCESS = 1  # 完了 (正常取得)
    STATUS_NOT_FOUND = 2  # 欠番 (404 Not Found)
    STATUS_INVALID = 3  # 無効 (データが不正、または空)
    STATUS_PARSE_ERROR = 4  # 解析失敗 (JSON構造が想定外など)
    STATUS_NETWORK_ERROR = -1  # 通信失敗 (タイムアウト、5xxエラー等)
    STATUS_RATE_LIMIT = -2  # レート制限 (429 Too Many Requests)

    FIELDNAME = [
        "raw_remote_id",
        "name",
        "kana",
        "lon",
        "lat",
        "elevation_m",
        "poi_type_raw",
        "last_updated_at",
    ]

    def __init__(self, pois_table=None, queue_table=None):
        self.pois_table = pois_table
        self.queue_table = queue_table
        self.field_names = CrawlerDB.FIELDNAME
        try:
            self.connection = mysql.connector.connect(
                option_files=os.path.expanduser("~/.my.cnf"),
                autocommit=False,
            )
        except mysql.connector.Error as err:
            print(f"Error connecting to database: {err}")
            self.connection = None

    def commit(self):
        if self.connection:
            self.connection.commit()

    def __exit__(self):
        if self.connection:
            self.connection.close()

    def truncate_tables(self):
        conn = self.connection
        with conn.cursor(dictionary=True) as cur:
            cur.execute(f"TRUNCATE TABLE {self.pois_table}")
            cur.execute(f"TRUNCATE TABLE {self.queue_table}")

    def create_tables_if_not_exist(self):
        conn = self.connection
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.pois_table} (
                    raw_remote_id BIGINT,
                    name VARCHAR(255),
                    kana VARCHAR(255),
                    lon DOUBLE,
                    lat DOUBLE,
                    elevation_m DOUBLE,
                    poi_type_raw VARCHAR(255),
                    last_updated_at DATETIME,
                    PRIMARY KEY (raw_remote_id)
                )
                """,
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.queue_table} (
                    raw_remote_id BIGINT,
                    status TINYINT DEFAULT 0,  -- 0:未調査, 1:成功, 2:欠番(404), 3:無効(200), 4:解析失敗, -1:通信エラー
                    last_checked DATETIME,
                    PRIMARY KEY (raw_remote_id)
                )
                """,
            )

    def get_max_id(self):
        conn = self.connection
        max_id = 0
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                f"""
                SELECT COALESCE(MAX(raw_remote_id), 0) AS max_id
                FROM {self.queue_table}
                WHERE status = {self.STATUS_SUCCESS}
                """,
            )
            max_id = cur.fetchone()["max_id"]
        return max_id

    def save_to_database(self, target_id, data):
        fields_str = ", ".join(self.field_names)
        placeholders = ", ".join(["%s"] * len(self.field_names))
        update_str = ", ".join(
            [f"{f}=VALUES({f})" for f in self.field_names if f != "raw_remote_id"]
        )

        conn = self.connection
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                f"""
                INSERT INTO {self.pois_table} ({fields_str})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_str}
                """,
                tuple(data[i] for i in self.field_names),
            )

    def update_queue_status(self, target_id, status):
        conn = self.connection
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                f"""
                INSERT INTO {self.queue_table}
                VALUES (%s, %s, NOW())
                ON DUPLICATE KEY UPDATE status=VALUES(status), last_checked=VALUES(last_checked)
                """,
                (target_id, status),
            )


# __END__
