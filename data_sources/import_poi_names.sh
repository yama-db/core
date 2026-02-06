#!/bin/bash

PROJECT_ROOT=$(dirname "$VIRTUAL_ENV")

import_poi_names() {
    local table_name=$1
    local source_type=$2

    echo "Importing POI names from $table_name for source type $source_type..."

    local source_id_subquery
    if [ "$source_type" == "BOOK" ]; then
        source_id_subquery="s.source_id"
    else
        source_id_subquery="(SELECT id FROM information_sources WHERE display_name = '$source_type' LIMIT 1)"
    fi

    mysql --defaults-file=$PROJECT_ROOT/.my.cnf <<EOS
INSERT INTO poi_names (
    unified_poi_id,
    source_uuid,
    source_id,
    name_text,
    name_normalized,
    name_reading,
    name_type,
    is_preferred
)
SELECT 
    p.unified_poi_id,
    s.source_uuid,
    $source_id_subquery,
    j.name_text,
    j.name_text AS name_normalized,
    j.name_reading,
    IF(j.idx = 1, 'MAIN', 'ALIAS') AS name_type,
    FALSE AS is_preferred
FROM $table_name AS s
JOIN poi_links AS p ON s.source_uuid = p.source_uuid
JOIN JSON_TABLE(
    s.names_json,
    '\$[*]' COLUMNS (
        idx FOR ORDINALITY,
        name_text VARCHAR(255) PATH '\$.name',
        name_reading VARCHAR(255) PATH '\$.kana'
    )
) AS j
WHERE p.source_type = '$source_type';
EOS
}

mysql --defaults-file=$PROJECT_ROOT/.my.cnf <<EOS
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE poi_names;
SET FOREIGN_KEY_CHECKS = 1;
EOS

import_poi_names "stg_gsi_vtexp_pois" "GSI_VTEXP"
import_poi_names "stg_gsi_dm25k_pois" "GSI_DM25K"
import_poi_names "stg_yamap_pois" "YAMAP"
import_poi_names "stg_yamareco_pois" "YAMARECO"
import_poi_names "stg_wikidata_pois" "WIKIDATA"
import_poi_names "stg_legacy_pois" "LEGACY"
import_poi_names "stg_book_pois" "BOOK"

mysql --defaults-file=$PROJECT_ROOT/.my.cnf <<EOS
UPDATE poi_names SET is_preferred = 0;
UPDATE poi_names
JOIN (
    SELECT 
        p.id,
        ROW_NUMBER() OVER (
            PARTITION BY p.unified_poi_id 
            ORDER BY 
                s.reliability_level ASC,    -- 信頼度の高い情報源を優先
                p.id ASC                     -- 同じなら登録順
        ) as name_rank
    FROM poi_names AS p
    JOIN information_sources AS s ON p.source_id = s.id
    WHERE p.name_type = 'MAIN'              -- メイン名称のみ対象
      AND p.name_reading IS NOT NULL        -- 読みがあるものを優先
      AND p.name_reading <> ''
) AS ranked_names ON poi_names.id = ranked_names.id
SET poi_names.is_preferred = 1
WHERE ranked_names.name_rank = 1;
EOS

echo "POI names import completed."
