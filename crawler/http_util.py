#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
from argparse import ArgumentParser

import bs4
import requests

USER_AGENT = "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def get_with_retry(session, url):
    wait_time = 1
    for i in range(5):
        try:
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                return res, 0
            if res.status_code not in [500, 502, 503, 504]:
                break
            time.sleep(wait_time << i)
        except requests.RequestException as e:
            print(f"Request error: {e}")
            sys.exit(1)
    return None, -1


def post_with_retry(session, url, payload):
    wait_time = 2
    for i in range(8):
        try:
            res = session.post(url, data=payload, timeout=10)
            if res.status_code == 200:
                return res, 0
            if res.status_code == 429:
                return None, -2
            if res.status_code not in [500, 502, 503, 504]:
                break
            time.sleep(wait_time << i)
        except requests.RequestException as e:
            print(f"Request error: {e}")
            sys.exit(1)
    return None, -1


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("site_name", help="Site name (Yamareco or YAMAP)")
    parser.add_argument("target_id", help="Target POI ID to test fetching")
    args = parser.parse_args()
    site_name = args.site_name.lower()
    target_id = args.target_id

    with requests.Session() as session:
        session.headers.update({"User-Agent": USER_AGENT})
        if site_name == "yamareco":
            url = "https://api.yamareco.com/api/v1/searchPoi"
            payload = {
                "page": 1,
                "type_id": 0,
                "ptid": target_id,
            }
            res, code = post_with_retry(session, url, payload)
            if res:
                print(res.text)
            else:
                print("Failed with code:", code)
        elif site_name == "yamap":
            url = f"https://yamap.com/landmarks/{target_id}"
            res, code = get_with_retry(session, url)
            if res:
                soup = bs4.BeautifulSoup(res.text, "html.parser")
                script = soup.find("script", id="__NEXT_DATA__")
                assert script and script.string
                print(script.string)
            else:
                print("Failed with code:", code)
        else:
            print("Unknown site name. Please use 'Yamareco' or 'YAMAP'.")

# __END__
