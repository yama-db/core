#!/bin/bash

die() {
    echo "❌ エラー: $1" >&2
    exit 1
}

SOURCE_TABLES="
stg_gsi_1003_pois
stg_gsi_dm25k_pois
stg_gsi_vtexp_pois
stg_yamap_pois
stg_yamareco_pois
stg_wikidata_pois
stg_legacy_pois
stg_book_web_pois
"

i=0
for table_name in $SOURCE_TABLES; do
    if [ $i -eq 0 ]; then
        TRUNCATE='--truncate'
    else
        TRUNCATE=''
    fi
    python3 import_poi_names.py $TRUNCATE $table_name || die "Failed to import $table_name"
    ((i++))
done
python3 set_preferred.py || die "Failed to set preferred name"
