#!/opt/local/bin/bash -eu
# Extract script for GSI GCP data

mkdir -p work
rm -rf work/*

VERSION=20260401

files=(
    "FG-GML-chubu-ALL1-%VERSION%-Z001.zip"
    "FG-GML-chubu-ALL1-%VERSION%-Z002.zip"
    "FG-GML-chugoku-ALL1-%VERSION%-Z001.zip"
    "FG-GML-chugoku-ALL1-%VERSION%-Z002.zip"
    "FG-GML-hokkaido-ALL1-%VERSION%-Z001.zip"
    "FG-GML-hokkaido-ALL1-%VERSION%-Z002.zip"
    "FG-GML-hokuriku-ALL1-%VERSION%-Z001.zip"
    "FG-GML-hokuriku-ALL1-%VERSION%-Z002.zip"
    "FG-GML-kanto1-ALL1-%VERSION%-Z001.zip"
    "FG-GML-kanto1-ALL1-%VERSION%-Z002.zip"
    "FG-GML-kanto2-ALL1-%VERSION%-Z001.zip"
    "FG-GML-kanto2-ALL1-%VERSION%-Z002.zip"
    "FG-GML-kanto3-ALL1-%VERSION%-Z001.zip"
    "FG-GML-kinki-ALL1-%VERSION%-Z001.zip"
    "FG-GML-kinki-ALL1-%VERSION%-Z002.zip"
    "FG-GML-kyushu_okinawa-ALL1-%VERSION%-Z001.zip"
    "FG-GML-kyushu_okinawa-ALL1-%VERSION%-Z002.zip"
    "FG-GML-shikoku-ALL1-%VERSION%-Z001.zip"
    "FG-GML-tohoku-ALL1-%VERSION%-Z001.zip"
    "FG-GML-tohoku-ALL1-%VERSION%-Z002.zip"
)

for t in "${files[@]}"; do
    z="archive/${t//%VERSION%/$VERSION}"
    if [ ! -f $z ]; then
        echo "Missing file: $z"
        exit 1
    fi
    echo Extracting $z
    subdir=$(basename $z .zip)
    unzip -q -d work/$subdir $z
done

declare -A unziped

for z in work/*/FG-GML-*-ALL-*.zip; do
    b=$(basename $z)
    if [ -z "${unziped[$b]:-}" ]; then
        echo Extracting $z
        set +e
        unzip -q -d work $z '*-GCP-*' '*-ElevPt-*'
        set -e
        unziped[$b]=$z
    fi
done
