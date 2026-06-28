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
mysql --defaults-file=$PROJECT_ROOT/.my.cnf < schema.sql
