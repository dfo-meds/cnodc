CREATE TABLE IF NOT EXISTS stations (

    station_uuid TEXT,
    wmo_id TEXT,
    wigos_id TEXT,
    station_name TEXT,
    station_id TEXT,
    station_type TEXT,
    service_start_date TEXT,
    service_end_date TEXT,
    instrumentation TEXT,
    metadata TEXT,
    map_to_uuid TEXT,
    status TEXT,
    embargo_data_days INT

);

DROP TABLE IF EXISTS records;

CREATE TABLE IF NOT EXISTS records (

    record_uuid TEXT,
    record_hash TEXT,
    display TEXT,
    record_content TEXT,
    lat REAL,
    lon REAL,
    datetime TEXT,
    has_errors INT

);


CREATE TABLE IF NOT EXISTS actions (

    record_uuid TEXT,
    action_text TEXT

);
