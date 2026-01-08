#!/bin/bash

# 地理院タイル目録
# https://github.com/gsi-cyberjapan/mokuroku-spec

# wget -P private_data https://cyberjapandata.gsi.go.jp/xyz/std/mokuroku.csv.gz

gzcat private_data/mokuroku.csv.gz \
| sed -n '/^15\//s/\.png,.*$//p' \
| while IFS=/ read -r z x y; do
    if [ -f tiles/$z/$x/$y.geojson ]; then
        echo "Tile z=$z, x=$x, y=$y already exists, skipping download."
        continue
    fi
    echo "Downloading tile z=$z, x=$x, y=$y"
    mkdir -p tiles/$z/$x
    wget -q -O tiles/$z/$x/$y.geojson "https://cyberjapandata.gsi.go.jp/xyz/experimental_nnfpt/$z/$x/$y.geojson"
done
