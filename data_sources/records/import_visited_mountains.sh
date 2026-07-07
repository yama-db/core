#!/bin/bash
if [ -z "${VIRTUAL_ENV}" ]; then
    echo "エラー: Python仮想環境（venv）がアクティブになっていません。" >&2
    exit 1
fi
PROJECT_ROOT=$(dirname "$VIRTUAL_ENV")
if [ ! -r "${PROJECT_ROOT}/.my.cnf" ]; then
    echo "エラー: ${PROJECT_ROOT}/.my.cnf が存在しないか、読み込み権限がありません。" >&2
    exit 1
fi
SQL='
TRUNCATE TABLE anineco_mountain.visited_mountains;
INSERT INTO anineco_mountain.visited_mountains (
    mountain_record_id,
    climb_date,
    climb_order,
    mountain_id
)
SELECT 
    rec,
    summit,
    ascent_order,
    id
FROM 
    anineco_tozan.explored
'
mysql --defaults-file=$PROJECT_ROOT/.my.cnf --safe-updates=0 --batch -e "$SQL"
