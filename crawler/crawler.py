#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import time
from argparse import ArgumentParser
from datetime import datetime, timezone

import requests

from crawler_utils import CrawlerDB
from http_util import USER_AGENT, get_with_retry, post_with_retry


def fetch_yamap_data(session, target_id):
    url = f"https://yamap.com/landmarks/{target_id}"
    res, code = get_with_retry(
        session, url
    )  # code: 0=success, -1=network error, -2=rate limit
    if code < 0:
        return None, (
            CrawlerDB.STATUS_RATE_LIMIT
            if code == -2
            else CrawlerDB.STATUS_NETWORK_ERROR
        )
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', res.text
    )
    if not m:
        return None, CrawlerDB.STATUS_INVALID
    data = json.loads(m.group(1))
    landmark = data.get("props", {}).get("pageProps", {}).get("restLandmark")
    if not landmark:
        return None, CrawlerDB.STATUS_INVALID
    assert landmark.get("id") == target_id
    kana = {"hira": landmark.get("nameHira", ""), "en": landmark.get("nameEn", "")}
    coord = landmark.get("coord")
    assert coord and len(coord) >= 2
    last_updated_at = datetime.fromtimestamp(
        landmark.get("updatedAt"), timezone.utc
    ).strftime("%Y-%m-%d")

    return {
        "raw_remote_id": target_id,
        "name": landmark.get("name"),
        "kana": json.dumps(kana, ensure_ascii=False),
        "lon": coord[0],
        "lat": coord[1],
        "elevation_m": landmark.get("altitude"),
        "poi_type_raw": landmark.get("landmarkTypeId"),
        "last_updated_at": last_updated_at,
    }, CrawlerDB.STATUS_SUCCESS


def fetch_yamareco_data(session, target_id):
    url = "https://api.yamareco.com/api/v1/searchPoi"
    payload = {
        "page": 1,
        "type_id": 0,
        "ptid": target_id,
    }
    res, code = post_with_retry(
        session, url, payload
    )  # code: 0=success, -1=network error, -2=rate limit
    if code < 0:
        return None, (
            CrawlerDB.STATUS_RATE_LIMIT
            if code == -2
            else CrawlerDB.STATUS_NETWORK_ERROR
        )
    data = res.json()
    if data.get("err") != 0:
        return None, CrawlerDB.STATUS_INVALID
    poilist = data.get("poilist", [])
    if len(poilist) == 0:
        return None, CrawlerDB.STATUS_INVALID
    poi = data["poilist"][0]
    kana = {"hira": poi.get("yomi", ""), "en": poi.get("name_en", "")}
    poi_type_raw = [int(d["type_id"]) for d in poi.get("types", []) if "type_id" in d]
    return {
        "raw_remote_id": target_id,
        "name": poi.get("name", ""),
        "kana": json.dumps(kana, ensure_ascii=False),
        "lat": poi.get("lat", ""),
        "lon": poi.get("lon", ""),
        "elevation_m": poi.get("elevation"),
        "poi_type_raw": json.dumps(poi_type_raw),
        "last_updated_at": None,
    }, CrawlerDB.STATUS_SUCCESS


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "site_name", choices=["yamap", "yamareco"], help="Site to crawl"
    )
    parser.add_argument("--truncate", action="store_true", help="Truncate tables")
    parser.add_argument("--step", type=int, default=0, help="Number of POIs to crawl")
    parser.add_argument(
        "--max-failures", type=int, default=800, help="Max consecutive failures"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Interval between requests (seconds)",
    )
    parser.add_argument("--timeout", type=int, default=0, help="Request timeout (seconds)")
    args = parser.parse_args()
    site_name = args.site_name.lower()
    truncate = args.truncate
    step = args.step
    max_failures = args.max_failures
    interval = args.interval
    timeout = args.timeout
    if not (step > 0 or timeout > 0):
        print("[!] Either --step or --timeout must be greater than 0.")
        return

    if site_name == "yamareco":
        pois_table = "yamareco_pois"
        queue_table = "yamareco_queue"
        fetch_func = fetch_yamareco_data
    else:
        pois_table = "yamap_pois"
        queue_table = "yamap_queue"
        fetch_func = fetch_yamap_data

    db = CrawlerDB(pois_table=pois_table, queue_table=queue_table)
    db.create_tables_if_not_exist()
    if truncate:
        db.truncate_tables()
        return

    start = db.get_max_id() + 1
    print(f"[*] Crawling {site_name} starts from ID {start}")
    count = 0  # Number of successful entries
    n_write = 0  # Number of DB writes
    n_failures = 0  # Number of consecutive failures
    target_id = start
    start_time = time.time()
    try:
        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})
            while True:
                if step > 0 and target_id >= start + step:
                    break
                if max_failures > 0 and n_failures >= max_failures:
                    print(
                        f"[!] Reached maximum consecutive failures ({n_failures}). Stopping crawler."
                    )
                    break
                elapsed_time = time.time() - start_time
                if timeout > 0 and elapsed_time >= timeout:
                    print(
                        f"[*] Timeout reached after {elapsed_time:.2f} seconds. Stopping crawler."
                    )
                    break
                # Fetch data
                data, status = fetch_func(session, target_id)
                if status == CrawlerDB.STATUS_RATE_LIMIT:
                    print(
                        f"[!] Rate limit encountered at ID {target_id}. Stopping crawler."
                    )
                    break
                if status == CrawlerDB.STATUS_SUCCESS:
                    db.save_to_database(target_id, data)
                    count += 1
                    n_write += 1
                    n_failures = 0
                else:
                    n_failures += 1
                db.update_queue_status(target_id, status)
                n_write += 1
                if n_write % 100 == 0:  # CONFIG: Commit every 100 writes
                    db.commit()
                target_id += 1
                if interval > 0:
                    time.sleep(interval)
    finally:
        db.commit()

    print(f"[*] Total successful entries: {count} / {target_id - start}")


if __name__ == "__main__":
    main()

# __END __
