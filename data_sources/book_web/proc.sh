#!/bin/bash
for csv_file in raw/[0-9]*.csv; do
  mv $csv_file $csv_file.orig
  echo 'raw_id,name,kana,elevation,mountain_id,description' > $csv_file
  tail -n +2 $csv_file.orig >> $csv_file
done
