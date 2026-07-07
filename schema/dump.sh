#!/opt/local/bin/bash
tables=()
for table in $(sed -n -E 's/^CREATE TABLE ([^ ]+) \($/\1/p' schema.sql); do
  if [[ "$table" != "administrative_boundaries" && "$table" != "stg_gsi_gcp_pois" ]]; then
    tables+=("$table")
  fi
done
mysqldump --defaults-file=../.my.cnf anineco_test "${tables[@]}" 2>/dev/null | gzip > backup.sql.gz
