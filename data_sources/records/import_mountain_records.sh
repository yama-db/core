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
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE anineco_mountain.mountain_records;
SET FOREIGN_KEY_CHECKS=1;
INSERT INTO anineco_mountain.mountain_records (
    start_date,
    end_date,
    published_at,
    title,
    summary,
    public_url,
    image_url
)
SELECT 
    start,
    end,
    issue,
    title,
    summary,
    link,
    image
FROM 
    anineco_tozan.record
ORDER BY rec
'
mysql --defaults-file=$PROJECT_ROOT/.my.cnf --safe-updates=0 --batch -e "$SQL"
