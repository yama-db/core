#!/bin/bash

PROJECT_ROOT=$(dirname "$VIRTUAL_ENV")

mysql --defaults-file=$PROJECT_ROOT/.my.cnf --safe-updates=0 <<EOS
SELECT
    parent_id,
    COUNT(*) AS total_children,
    SUM(CASE WHEN is_representative THEN 1 ELSE 0 END) AS representative_count
FROM poi_hierarchies
GROUP BY parent_id
HAVING representative_count != 1;

UPDATE mountain_pois AS m
LEFT JOIN poi_hierarchies AS p
  ON p.parent_id = m.id AND p.is_representative
SET m.main_child_id = p.child_id
WHERE m.is_used;
EOS
