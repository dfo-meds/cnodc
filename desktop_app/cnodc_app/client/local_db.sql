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