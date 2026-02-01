#!/bin/bash
SITE=$1
MODE=$2
export HOME=/home/anineco
cd $(dirname $0)
PYTHON=../venv/bin/python3

INTERVAL=0.2
TIMEOUT=3000

# Step.1 yamareco を全件クロール
# Step.2 yamap を全件クロール
# 毎時00分に起動、50分で完了させる
# night (00:00-06:00) 
# day   (09:00-17:00)

if [ "$SITE" == "yamareco" ]; then
    if [ "$MODE" == "night" ]; then
        STEP=9000
    else
        STEP=5400
    fi
else
    if [ "$MODE" == "night" ]; then
        STEP=3800
    else
        STEP=3000
    fi
fi

# 実行
$PYTHON crawler.py "$SITE" --timeout "$TIMEOUT" --interval "$INTERVAL" # --step "$STEP"
