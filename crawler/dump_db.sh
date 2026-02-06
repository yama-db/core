#!/bin/bash
SITE=$1
if [ "$SITE" == "yamareco" ]; then
    SQL='
    SELECT raw_remote_id, name, kana, lat, lon, elevation_m, poi_type_raw, last_checked AS last_updated_at
    FROM yamareco_pois
    JOIN yamareco_queue USING (raw_remote_id)
    WHERE JSON_CONTAINS(poi_type_raw, 1)
    '
else
    SQL='
    SELECT raw_remote_id, name, kana, lat, lon, elevation_m, poi_type_raw, last_updated_at
    FROM yamap_pois
    WHERE poi_type_raw IN ("19", "999")
    '
fi
mysql --defaults-file=crawler.my.cnf -B -e "$SQL"
