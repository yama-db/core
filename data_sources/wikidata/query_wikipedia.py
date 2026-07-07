#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
import urllib.parse
from argparse import ArgumentParser

import requests

parser = ArgumentParser(description="Query Wikipedia for mountain data")
parser.add_argument("csv_file", help="Input file with wikipedia URLs")
args = parser.parse_args()


def get_wikipedia_extract(url: str) -> tuple[str, str]:
    parsed_url = urllib.parse.unquote(url)
    title = parsed_url.split("/")[-1]
    api_url = "https://ja.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|revisions",
        "exintro": True,
        "explaintext": True,
        "exsentences": 1,
        "titles": title,
        "rvprop": "timestamp",
    }
    headers = {
        "User-Agent": "MyMountainReadingBot/1.0 (contact: user@example.com)",
    }
    extract = ""
    try:
        response = requests.get(api_url, params=params, headers=headers)
        data = response.json()
        pages = data.get("query", {}).get("pages", {})
        page_id = next(iter(pages))
        extract = pages[page_id].get("extract", "")
        revisions = pages[page_id].get("revisions", [])
        timestamp = revisions[0].get("timestamp") if revisions else None
    except requests.RequestException as e:
        print(f"Request error fetching {url}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        sys.exit(1)

    return extract, timestamp


with open(args.csv_file, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    writer = csv.DictWriter(sys.stdout, fieldnames=["item", "extract", "timestamp"])
    writer.writeheader()
    qids = set()
    for row in reader:
        item = row.get("item")
        qid = item.split("/")[-1]
        if qid in qids:
            continue
        qids.add(qid)
        wikipedia_url = row.get("wikipedia_url")
        if not wikipedia_url:
            continue
        extract, timestamp = get_wikipedia_extract(wikipedia_url)
        print(f"Fetched extract for {item}: '{extract}'", file=sys.stderr)
        writer.writerow(
            {
                "item": item,
                "extract": extract,
                "timestamp": timestamp,
            }
        )

# __END__
