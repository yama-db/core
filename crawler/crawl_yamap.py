#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ヤマップのランドマークデータをクローリングしてSQLiteデータベースに保存

import json
import random
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import crawler_utils as utils

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "yamap_pois.sqlite3"


def analyze_landmark_data(landmark):
    raw_remote_id = landmark.get("id")
    if not raw_remote_id:
        return None
    name = landmark.get("name", "")
    kana = {"hira": landmark.get("nameHira", ""), "en": landmark.get("nameEn", "")}
    coord = landmark.get("coord")  # [経度, 緯度] の形式
    if coord and len(coord) >= 2:
        return {
            "raw_remote_id": raw_remote_id,
            "name": name,
            "kana": json.dumps(kana, ensure_ascii=False),
            "lat": coord[1],
            "lon": coord[0],
            "elevation_m": landmark.get("altitude"),
            "poi_type_raw": landmark.get("landmarkTypeId"),
            "last_updated_at": datetime.fromtimestamp(
                landmark.get("updatedAt"), timezone.utc
            ).strftime("%Y-%m-%d"),
        }
    return None


def run_crawler(conn, target_ids, wait_range):
    """統合クローラーメインループ"""
    cur = conn.cursor()
    session = requests.Session()
    session.headers.update({"User-Agent": utils.USER_AGENT})

    for raw_remote_id in target_ids:
        current_wait = random.uniform(*wait_range)
        url = f"https://yamap.com/landmarks/{raw_remote_id}"
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 429:
                print("[!] 429 Too Many Requests. Stopping...")
                session.close()
                return False
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.find("script", id="__NEXT_DATA__")
            if script_tag and script_tag.string:
                json_data = json.loads(script_tag.string)
                props = json_data.get("props", {})
                page_props = props.get("pageProps", {})
                if landmark := page_props.get("restLandmark"):
                    if data := analyze_landmark_data(landmark):
                        utils.save_to_database(conn, data)
                        new_status = 1  # Success
                    else:
                        new_status = 4  # Failed to parse data
                else:
                    error = page_props.get("error", {})
                    new_status = 2 if error.get("status") == 404 else 4
            else:
                new_status = 4  # Failed to parse data

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

        mode = utils.get_system_mode(conn, max_initial_id=300000)
        print(f"[*] Mode: {mode}")

        # モード別設定
        if mode == "INITIAL_CRAWL":
            utils.refill_queue(conn, 10000, 2000)
            limit, wait = 200, (0.6, 1.2)
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
