CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.nginx_logs
(
 remote_addr String,
 upstream_addr  String,
 request_time String,
 upstream_response_time String,
 request String,
 status String,
 bytes_sent UInt32,
 time DateTime,
 event_date Date
) ENGINE = Log()

-- MergeTree(event_date, (status), 8192)