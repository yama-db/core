#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ヤマレコの山データをクローリングしてSQLiteデータベースに保存

import json
import random
import sqlite3
import time
from pathlib import Path

import requests

import crawler_utils as utils

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "yamareco_pois.sqlite3"


def analyze_yamareco_poi(poidata):
    raw_remote_id = poidata.get("ptid")
    if not raw_remote_id:
        return None
    kana = {"hira": poidata.get("yomi", ""), "en": poidata.get("name_en", "")}
    poi_type_raw = poidata.get("types", [])
    return {
        "raw_remote_id": raw_remote_id,
        "name": poidata.get("name", ""),
        "kana": json.dumps(kana, ensure_ascii=False),
        "lat": poidata.get("lat", ""),
        "lon": poidata.get("lon", ""),
        "elevation_m": poidata.get("elevation"),
        "poi_type_raw": json.dumps(poi_type_raw, ensure_ascii=False),
        "last_updated_at": None,
    }


def run_crawler(conn, target_ids, wait_range):
    """統合クローラーメインループ"""
    cur = conn.cursor()
    session = requests.Session()
    session.headers.update({"User-Agent": utils.USER_AGENT})

    for raw_remote_id in target_ids:
        current_wait = random.uniform(*wait_range)
        url = "https://api.yamareco.com/api/v1/searchPoi"
        params = {
            "page": 1,
            "name": "",
            "type_id": 0,
            "area_id": 0,
            "ptid": raw_remote_id,
        }
        try:
            response = session.post(url, data=params)
            if response.status_code == 429:
                print("[!] 429 Too Many Requests. Stopping...")
                session.close()
                return False
            response.raise_for_status()
            json_data = response.json()
            poilist = json_data.get("poilist", [])
            if len(poilist) > 0:
                if data := analyze_yamareco_poi(poilist[0]):
                    utils.save_to_database(conn, data)
                    new_status = 1  # Success
                else:
                    new_status = 4  # Failed to parse data
            else:
                new_status = 2  # not assigned

        except requests.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 404:
                new_status = 2  # not assigned
                current_wait = 0.3
            else:
                print(f"[!] HTTP Error {status_code} for ID {raw_remote_id}")
                new_status = -1  # Communication error

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"[!] Connection error/timeout for ID {raw_remote_id}: {e}")
            new_status = -1
            current_wait = 5

        utils.update_queue_status(conn, raw_remote_id, new_status)
        time.sleep(current_wait)

    session.close()
    return True


def main():
    with sqlite3.connect(DB_PATH, timeout=15) as conn:
        conn.row_factory = sqlite3.Row
        utils.setup_database(conn)

        mode = utils.get_system_mode(conn, max_initial_id=100000)
        print(f"[*] Mode: {mode}")

        # モード別設定
        if mode == "INITIAL_CRAWL":
            utils.refill_queue(conn, 100000, 5000)
            limit, wait = 200, (0.8, 1.5)
        elif mode == "MAINTENANCE_UPDATE":
            limit, wait = 100, (1.5, 3.0)
        else:  # IDLE_WATCH
            utils.refill_queue(conn, 1000, 200)  # 常に少し先を見る
            limit, wait = 50, (3.0, 5.0)

        target_ids = utils.fetch_targets(conn, mode, limit)
        if target_ids:
            run_crawler(conn, target_ids, wait)
        utils.print_progress_summary(conn)


if __name__ == "__main__":
    main()

# __END__
