#!/opt/local/bin/bash -eu
# Extract script for GSI GCP data for January 1, 2026

VERSION=20260101

rm -rf work/*

for z in archive/FG-GML-*-ALL1-${VERSION}-*.zip; do
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
