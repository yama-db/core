#!/bin/bash
SITE=$1
MODE=$2
export HOME=/var/services/homes/tad
cd $(dirname $0)
PYTHON=../venv/bin/python3

if [ "$MODE" == "night" ]; then
    if [ "$SITE" == "yamap" ]; then
        STEP=2500
        INTERVAL=0.1
    else
        STEP=1000
        INTERVAL=0.2
    fi
else
    if [ "$SITE" == "yamap" ]; then
        STEP=200
        INTERVAL=0.2
    else
        STEP=500
        INTERVAL=0.4
    fi
fi

# 実行
$PYTHON crawler.py "$SITE" --step "$STEP" --interval "$INTERVAL"
