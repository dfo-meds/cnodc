
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS actions;
DROP TABLE IF EXISTS stations;


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


CREATE TABLE IF NOT EXISTS records (

    record_content TEXT,
    lat TEXT,
    lat_qc INT,
    lon TEXT,
    lon_qc INT,
    datetime TEXT,
    datetime_qc INT,
    has_errors INT,
    record_hash TEXT,
    display TEXT,

    actions TEXT,
    record_uuid TEXT,
    downloaded INT,
    platform_id TEXT

);


CREATE TABLE IF NOT EXISTS actions (

    record_uuid TEXT,
    action_text TEXT,
    is_saved INT DEFAULT 0

);
