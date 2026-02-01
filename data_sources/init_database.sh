#!/bin/bash
USER=anineco
DATABASE=anineco_test

PROJECT_ROOT=$(dirname "$VIRTUAL_ENV")

mysql -u root -p <<EOS
CREATE DATABASE IF NOT EXISTS $DATABASE
DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
GRANT ALL PRIVILEGES ON $DATABASE.* to '$USER'@'%';
EOS

mysql --defaults-file=$PROJECT_ROOT/.my.cnf < $PROJECT_ROOT/schema/schema.sql
