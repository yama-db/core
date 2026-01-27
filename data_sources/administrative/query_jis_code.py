#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys

import requests

fieldnames = [
    "item",
    "itemLabel",
    "parentTaxon",
]
writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
writer.writeheader()

url = "https://query.wikidata.org/sparql"
headers = {
    "User-Agent": "YamaDBProject/1.0 (contact: anineco@gmail.com)",
    "Accept": "application/sparql-results+json",
}
query = """
    SELECT
        ?item ?itemLabel ?parentTaxon
    WHERE {
        ?item wdt:P429 ?parentTaxon.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
    }
    LIMIT 10000
"""
response = requests.get(url, params={"query": query}, headers=headers, timeout=30)
results = (
    response.json()["results"]["bindings"] if response.status_code == 200 else None
)
for result in results:
    row = {field: result.get(field, {}).get("value", "") for field in fieldnames}
    writer.writerow(row)

# __END__
