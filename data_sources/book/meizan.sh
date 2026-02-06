#!/usr/bin/env bash
echo "
select
  seqno as raw_remote_id,
  m.name,
  m.kana,
  alt as elevation_m,
  id as unified_poi_id
from
  meizan as m
join
  geom
using (id)
where cat=$1
order by seqno;
" | mysql -csv | sed -e "s/\t/,/g"
