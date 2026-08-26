#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import random
import sys
import time
import urllib.parse
from argparse import ArgumentParser

import requests

parser = ArgumentParser(description="Query Wikipedia for mountain data")
parser.add_argument("csv_file", help="Input file with wikipedia URLs")
args = parser.parse_args()

# Read the Wikidata CSV file and extract the Wikipedia titles
titles = {}
with open(args.csv_file, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        item = row.get("item")
        wikipedia_url = row.get("wikipedia_url")
        if not wikipedia_url:
            continue
        parsed_url = urllib.parse.unquote(wikipedia_url)
        title = parsed_url.split("/")[-1]
        titles[item] = title

writer = csv.DictWriter(sys.stdout, fieldnames=["item", "extract", "timestamp"])
writer.writeheader()

api_url = "https://ja.wikipedia.org/w/api.php"
headers = {
    "User-Agent": "YamaDBCrawler/1.0 (+mailto:anineco@gmail.com)",
}
CHUNK_SIZE = 20

for i in range(0, len(titles), CHUNK_SIZE):
    items = list(titles.keys())[i : i + CHUNK_SIZE]
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|revisions|pageprops",
        "ppprop": "wikibase_item",
        "exintro": True,
        "explaintext": True,
        "exsentences": 1,
        "exlimit": "max",
        "titles": "|".join(titles[item] for item in items),
        "rvprop": "timestamp",
    }
    try:
        response = requests.get(api_url, params=params, headers=headers)
    except Exception as e:
        print(f"Error fetching {api_url}: {e}", file=sys.stderr)
        sys.exit(1)
    data = response.json()
    pages = data.get("query", {}).get("pages", {})
    for page_id, page in pages.items():
        pageprops = page.get("pageprops", {})
        item = pageprops.get("wikibase_item")
        if not item:
            print(
                f"Warning: No wikibase_item found for page {page.get('title')}",
                file=sys.stderr,
            )
            continue
        extract = page.get("extract", "")
        revisions = page.get("revisions", [])
        timestamp = revisions[0].get("timestamp") if revisions else None
        print(f"Fetched extract for {item}: '{extract}'", file=sys.stderr)
        writer.writerow(
            {
                "item": item,
                "extract": extract,
                "timestamp": timestamp,
            }
        )
    time.sleep(random.uniform(3.0, 4.0))

# __END__
