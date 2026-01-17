#!/bin/bash
# 15分おきに実行するcron設定例
# */15 * * * * bash /var/services/homes/tad/web/script/run_crawl.sh

# 実行ディレクトリへ移動
cd $(dirname $0)
#PYTHON=./venv/bin/python3
PYTHON=../venv/bin/python3

echo "--- Starting All Crawlers: $(date +'%Y-%m-%d %H:%M:%S') ---"
echo "[*] Running YAMAP Crawler..."
$PYTHON crawl_yamap.py
echo "[*] Running Yamareco Crawler..."
$PYTHON crawl_yamareco.py
echo "--- Finished All Crawlers: $(date +'%Y-%m-%d %H:%M:%S') ---"
