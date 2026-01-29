#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys

import requests

fieldnames = [
    "item",
    "itemLabel",
    "coord",
    "elevation",
    "nativeLabel",
    "kanaQualifier",
    "wikipedia_url",
]
writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
writer.writeheader()

url = "https://query.wikidata.org/sparql"
headers = {
    "User-Agent": "MyPythonApp/1.0",
    "Accept": "application/sparql-results+json",
}
query = """
    SELECT DISTINCT
        ?item ?itemLabel ?coord ?elevation ?nativeLabel ?kanaQualifier ?wikipedia_url
    WHERE {
        ?item wdt:P31 wd:Q8502 .
        ?item wdt:P17 wd:Q17 .
        ?wikipedia_url schema:about ?item ;
                       schema:isPartOf <https://ja.wikipedia.org/> .
        OPTIONAL { ?item wdt:P625 ?coord . }
        OPTIONAL { ?item wdt:P2044 ?elevation . }
        OPTIONAL {
            ?item p:P1705 ?stmt .
            ?stmt ps:P1705 ?nativeLabel .
            OPTIONAL { ?stmt pq:P1814 ?kanaQualifier . }
        }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
    }
    ORDER BY ASC(?item) DESC(?elevation)
    LIMIT 10000
"""
response = requests.get(url, params={"query": query}, headers=headers)
results = (
    response.json()["results"]["bindings"] if response.status_code == 200 else None
)
for result in results:
    row = {field: result.get(field, {}).get("value", "") for field in fieldnames}
    writer.writerow(row)

# __END__
