#!/bin/bash



sqlite3 "$@" <<EOF
# .dump metric_enum


##
WITH a(source, host, metric, timestamp, val, val2, val3, val4) as (SELECT * from  mqtt_floats),
     b(id, name) as (SELECT * from  metric_enum)

SELECT a.source, a.host, b.name, datetime(a.timestamp, 'unixepoch'), a.val, a.val2, a.val3, a.val4 
from a JOIN b ON a.metric = b.id;


##
WITH a(source, host, metric, timestamp, val) as (SELECT * from  mqtt_str),
     b(id, name) as (SELECT * from  metric_enum)

SELECT a.source, a.host, b.name, datetime(a.timestamp, 'unixepoch'), a.val 
from a JOIN b ON a.metric = b.id;

##
.quit
EOF
