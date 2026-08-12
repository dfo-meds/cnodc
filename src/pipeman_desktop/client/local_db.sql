
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS actions;
DROP TABLE IF EXISTS stations;
DROP TABLE IF EXISTS platforms;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS observations;

CREATE TABLE IF NOT EXISTS files (

    source_uuid TEXT,
    filename TEXT,
    file_path TEXT,
    source: TEXT,
    program: TEXT,
    history: TEXT,
    received_date: TEXT,
    metadata: TEXT,
    is_payload: INT

);

CREATE TABLE IF NOT EXISTS platforms (

    platform_uuid TEXT,
    wmo_id TEXT,
    wigos_id TEXT,
    ship_code TEXT,
    platform_name TEXT,
    platform_id TEXT,
    platform_type TEXT,
    service_start_date TEXT,
    service_end_date TEXT,
    metadata TEXT,
    map_to_uuid TEXT,
    status TEXT,
    embargo_data_days INT,
    actions TEXT

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
    platform_id TEXT,
    source_uuid TEXT,
    received_date TEXT

);


CREATE TABLE IF NOT EXISTS observations (

    record_content TEXT,
    display TEXT,

    actions TEXT,
    received_date TEXT,
    downloaded INT,
    record_uuid TEXT
);


CREATE TABLE IF NOT EXISTS actions (

    record_uuid TEXT,
    action_text TEXT,
    is_saved INT DEFAULT 0

);
