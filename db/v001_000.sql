CREATE EXTENSION IF NOT EXISTS postgis;

-- Function that automatically sets the modified date on update
CREATE OR REPLACE FUNCTION update_modified_date()
RETURNS TRIGGER AS $$
BEGIN
    NEW.db_modified_date = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'obs_type') THEN
        CREATE TYPE obs_type AS ENUM (
            'SURFACE',
            'AT_DEPTH',
            'PROFILE',
            'OTHER'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'source_status') THEN
        CREATE TYPE source_status AS ENUM (
            'NEW',
            'QUEUED',
            'IN_PROGRESS',
            'ERROR',
            'COMPLETE'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'qc_status') THEN
        CREATE TYPE qc_status AS ENUM (
            'NEW',
            'QUEUED',
            'IN_PROGRESS',
            'MANUAL_REVIEW',
            'COMPLETE',
            'ERRORED'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'obs_status') THEN
        CREATE TYPE obs_status AS ENUM (
            'UNVERIFIED',
            'DUBIOUS',
            'VERIFIED',
            'DISCARDED',
            'DUPLICATE',
            'ARCHIVED'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'station_status') THEN
        CREATE TYPE station_status AS ENUM (
            'ACTIVE',
            'HISTORICAL',
            'REMOVED',
            'REPLACED'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_status') THEN
        CREATE TYPE user_status AS ENUM (
            'ACTIVE',
            'INACTIVE'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'queue_status') THEN
        CREATE TYPE queue_status AS ENUM (
            'UNLOCKED',
            'LOCKED',
            'DELAYED_RELEASE',
            'COMPLETE',
            'ERROR'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'processing_level') THEN
        CREATE TYPE processing_level AS ENUM (
            'RAW',
            'ADJUSTED',
            'REAL_TIME',
            'DELAYED_MODE'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_status') THEN
        CREATE TYPE user_status AS ENUM (
            'ACTIVE',
            'INACTIVE'
        );
    END IF;
END$$;


CREATE TABLE IF NOT EXISTS nodb_users (
    username            VARCHAR(126)    NOT NULL    PRIMARY KEY,
    phash               BYTEA,
    salt                BYTEA,
    old_phash           BYTEA                       DEFAULT NULL,
    old_salt            BYTEA                       DEFAULT NULL,
    old_expiry          TIMESTAMPTZ                 DEFAULT NULL,
    status              user_status     NOT NULL    DEFAULT 'ACTIVE',
    roles               JSON
);


CREATE TABLE IF NOT EXISTS nodb_permissions (
    role_name           VARCHAR(126)    NOT NULL,
    permission          VARCHAR(126)    NOT NULL
);


CREATE TABLE IF NOT EXISTS nodb_logins (
    username            VARCHAR(126)    NOT NULL    REFERENCES nodb_users(username),
    login_time          TIMESTAMPTZ     NOT NULL,
    login_addr          VARCHAR(126)    NOT NULL,
    instance_name       VARCHAR(126)    NOT NULL
);


CREATE INDEX IF NOT EXISTS ix_nodb_logins_login_time ON nodb_logins USING brin(login_time);


CREATE TABLE IF NOT EXISTS nodb_sessions (
    session_id          VARCHAR(126)    NOT NULL    PRIMARY KEY,
    start_time          TIMESTAMPTZ     NOT NULL,
    expiry_time         TIMESTAMPTZ     NOT NULL,
    username            VARCHAR(126)    NOT NULL    REFERENCES nodb_users(username),
    session_data        JSON
);


CREATE INDEX IF NOT EXISTS ix_nodb_sessions_username ON nodb_sessions(username);


CREATE TABLE IF NOT EXISTS nodb_upload_workflows (
    workflow_name       VARCHAR(126)    NOT NULL    PRIMARY KEY,
    configuration       JSON,
    is_active           BOOLEAN         NOT NULL    DEFAULT TRUE
);


CREATE TABLE IF NOT EXISTS nodb_scanned_files (
    file_path           TEXT            NOT NULL,
    modified_date       TIMESTAMPTZ                 DEFAULT NULL,
    scanned_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    was_processed       BOOLEAN                     DEFAULT FALSE
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_scanned_files_unique ON nodb_scanned_files(file_path, modified_date);


-- Source Files Table
CREATE TABLE IF NOT EXISTS nodb_source_files (
    source_uuid         UUID            NOT NULL    DEFAULT gen_random_uuid(),
    received_date       DATE            NOT NULL,

    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    source_path         TEXT,
    file_name           TEXT            NOT NULL,

    original_uuid       UUID,
    original_idx        INTEGER,

    metadata            JSON,
    history             JSON,

    status              source_status   NOT NULL    DEFAULT 'NEW',

    PRIMARY KEY(source_uuid, received_date)
) PARTITION BY RANGE(received_date);


-- Indexes
CREATE INDEX IF NOT EXISTS ix_nodb_source_files_source_path ON nodb_source_files(source_path);
CREATE INDEX IF NOT EXISTS ix_nodb_source_files_created_date ON nodb_source_files USING BRIN(db_created_date);
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_source_files_original_data ON nodb_source_files(received_date, original_uuid, original_idx) WHERE original_uuid IS NOT NULL;

-- Partition tables for 1800 to 2030
CREATE TABLE IF NOT EXISTS nodb_source_files_1800_1970 PARTITION OF nodb_source_files FOR VALUES FROM ('1800-01-01') TO ('1970-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_1970_1980 PARTITION OF nodb_source_files FOR VALUES FROM ('1970-01-01') TO ('1980-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_1980_1990 PARTITION OF nodb_source_files FOR VALUES FROM ('1980-01-01') TO ('1990-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_1990_2000 PARTITION OF nodb_source_files FOR VALUES FROM ('1990-01-01') TO ('2000-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2000_2001 PARTITION OF nodb_source_files FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2001_2002 PARTITION OF nodb_source_files FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2002_2003 PARTITION OF nodb_source_files FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2003_2004 PARTITION OF nodb_source_files FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2004_2005 PARTITION OF nodb_source_files FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2005_2006 PARTITION OF nodb_source_files FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2006_2007 PARTITION OF nodb_source_files FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2007_2008 PARTITION OF nodb_source_files FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2008_2009 PARTITION OF nodb_source_files FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2009_2010 PARTITION OF nodb_source_files FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2010_2011 PARTITION OF nodb_source_files FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2011_2012 PARTITION OF nodb_source_files FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2012_2013 PARTITION OF nodb_source_files FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2013_2014 PARTITION OF nodb_source_files FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2014_2015 PARTITION OF nodb_source_files FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2015_2016 PARTITION OF nodb_source_files FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2016_2017 PARTITION OF nodb_source_files FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2017_2018 PARTITION OF nodb_source_files FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2018_2019 PARTITION OF nodb_source_files FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2019_2020 PARTITION OF nodb_source_files FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2020_2021 PARTITION OF nodb_source_files FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2021_2022 PARTITION OF nodb_source_files FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2022_2023 PARTITION OF nodb_source_files FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2023_2024 PARTITION OF nodb_source_files FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2024_2025 PARTITION OF nodb_source_files FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2025_2026 PARTITION OF nodb_source_files FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2026_2027 PARTITION OF nodb_source_files FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2027_2028 PARTITION OF nodb_source_files FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2028_2029 PARTITION OF nodb_source_files FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2029_2030 PARTITION OF nodb_source_files FOR VALUES FROM ('2029-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS nodb_source_files_2030_2031 PARTITION OF nodb_source_files FOR VALUES FROM ('2030-01-01') TO ('2031-01-01');


-- Trigger for source files table modified date maintenance
CREATE OR REPLACE TRIGGER update_source_file_modified_date
    BEFORE UPDATE ON nodb_source_files
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();

-- Table for station records
CREATE TABLE IF NOT EXISTS nodb_stations (
    station_uuid        UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    wmo_id              VARCHAR(126),
    wigos_id            VARCHAR(126),
    station_name        VARCHAR(126),
    station_id          VARCHAR(126),
    station_type        VARCHAR(126)    NOT NULL,
    service_start_date  TIMESTAMPTZ     NOT NULL,
    service_end_date    TIMESTAMPTZ,
    instrumentation     JSONB,
    metadata            JSON,
    map_to_uuid         UUID                        REFERENCES nodb_stations(station_uuid),
    status              station_status  NOT NULL    DEFAULT 'ACTIVE',
    embargo_data_days   INTEGER
);


CREATE INDEX IF NOT EXISTS idx_nodb_stations_wmo_id ON nodb_stations(wmo_id) WHERE wmo_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_stations_wigos_id ON nodb_stations(wigos_id) WHERE wigos_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_stations_station_name ON nodb_stations(station_name) WHERE station_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_stations_station_id ON nodb_stations(station_id) WHERE station_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_stations_service_start_date ON nodb_stations(service_start_date);
CREATE INDEX IF NOT EXISTS idx_nodb_stations_service_end_date ON nodb_stations(service_end_date);
CREATE INDEX IF NOT EXISTS idx_nodb_stations_instrumentation ON nodb_stations USING GIN(instrumentation);


-- QC batch items (batches are processed as a single item)
CREATE TABLE IF NOT EXISTS nodb_qc_batches (
    batch_uuid          UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    qc_metadata         JSON,
    status              qc_status     NOT NULL
);


-- Trigger for QC batch table modified date maintenance
CREATE OR REPLACE TRIGGER update_qc_batch_modified_date
    BEFORE UPDATE ON nodb_qc_batches
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();



-- Table for observations
CREATE TABLE IF NOT EXISTS nodb_obs (
    obs_uuid            UUID            NOT NULL    DEFAULT gen_random_uuid(),
    received_date       DATE            NOT NULL,

    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    station_uuid        UUID                        REFERENCES nodb_stations(station_uuid),
    mission_name        VARCHAR(126),
    source_name         VARCHAR(126)    NOT NULL,
    instrument_type     VARCHAR(126)    NOT NULL,
    program_name        VARCHAR(126)    NOT NULL,

    obs_time            TIMESTAMPTZ,
    min_depth           REAL,
    max_depth           REAL,
    location            GEOGRAPHY(GEOMETRY, 4326),

    observation_type    obs_type,

    surface_parameters  JSONB,
    profile_parameters  JSONB,
    processing_level    processing_level        NOT NULL    DEFAULT 'RAW',
    embargo_date        TIMESTAMPTZ,

    PRIMARY KEY (obs_uuid, received_date)
) PARTITION BY RANGE(received_date);


CREATE INDEX IF NOT EXISTS nodb_obs_station_uuid ON nodb_obs(station_uuid);
CREATE INDEX IF NOT EXISTS nodb_obs_mission_name ON nodb_obs(mission_name);
CREATE INDEX IF NOT EXISTS nodb_obs_source_name ON nodb_obs(source_name);
CREATE INDEX IF NOT EXISTS nodb_obs_platform_name ON nodb_obs(instrument_type);
CREATE INDEX IF NOT EXISTS nodb_obs_program_name ON nodb_obs(program_name);
CREATE INDEX IF NOT EXISTS nodb_obs_obs_time ON nodb_obs(obs_time) WHERE obs_time IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodb_obs_min_depth ON nodb_obs(min_depth) WHERE min_depth IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodb_obs_max_depth ON nodb_obs(max_depth) WHERE max_depth IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodb_obs_obs_type ON nodb_obs(observation_type) WHERE observation_type != 'PROFILE';
CREATE INDEX IF NOT EXISTS nodb_obs_embargo_date ON nodb_obs(embargo_date) WHERE embargo_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodb_obs_location ON nodb_obs USING GIST(location);
CREATE INDEX IF NOT EXISTS nodb_obs_single_params ON nodb_obs USING GIN(surface_parameters);
CREATE INDEX IF NOT EXISTS nodb_obs_profile_params ON nodb_obs USING GIN(profile_parameters);


-- Trigger for QC batch table modified date maintenance
CREATE OR REPLACE TRIGGER update_obs_modified_date
    BEFORE UPDATE ON nodb_obs
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- Partition tables for 1800 to 2030
CREATE TABLE IF NOT EXISTS nodb_obs_1800_1970 PARTITION OF nodb_obs FOR VALUES FROM ('1800-01-01') TO ('1970-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_1970_1980 PARTITION OF nodb_obs FOR VALUES FROM ('1970-01-01') TO ('1980-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_1980_1990 PARTITION OF nodb_obs FOR VALUES FROM ('1980-01-01') TO ('1990-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_1990_2000 PARTITION OF nodb_obs FOR VALUES FROM ('1990-01-01') TO ('2000-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2000_2001 PARTITION OF nodb_obs FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2001_2002 PARTITION OF nodb_obs FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2002_2003 PARTITION OF nodb_obs FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2003_2004 PARTITION OF nodb_obs FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2004_2005 PARTITION OF nodb_obs FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2005_2006 PARTITION OF nodb_obs FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2006_2007 PARTITION OF nodb_obs FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2007_2008 PARTITION OF nodb_obs FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2008_2009 PARTITION OF nodb_obs FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2009_2010 PARTITION OF nodb_obs FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2010_2011 PARTITION OF nodb_obs FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2011_2012 PARTITION OF nodb_obs FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2012_2013 PARTITION OF nodb_obs FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2013_2014 PARTITION OF nodb_obs FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2014_2015 PARTITION OF nodb_obs FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2015_2016 PARTITION OF nodb_obs FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2016_2017 PARTITION OF nodb_obs FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2017_2018 PARTITION OF nodb_obs FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2018_2019 PARTITION OF nodb_obs FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2019_2020 PARTITION OF nodb_obs FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2020_2021 PARTITION OF nodb_obs FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2021_2022 PARTITION OF nodb_obs FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2022_2023 PARTITION OF nodb_obs FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2023_2024 PARTITION OF nodb_obs FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2024_2025 PARTITION OF nodb_obs FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2025_2026 PARTITION OF nodb_obs FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2026_2027 PARTITION OF nodb_obs FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2027_2028 PARTITION OF nodb_obs FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2028_2029 PARTITION OF nodb_obs FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2029_2030 PARTITION OF nodb_obs FOR VALUES FROM ('2029-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2030_2031 PARTITION OF nodb_obs FOR VALUES FROM ('2030-01-01') TO ('2031-01-01');



CREATE TABLE IF NOT EXISTS nodb_obs_data (
    obs_uuid            UUID            NOT NULL,
    received_date       DATE            NOT NULL,

    source_file_uuid    UUID            NOT NULL,
    message_idx         INTEGER         NOT NULL,
    record_idx          INTEGER         NOT NULL,

    status              obs_status      NOT NULL,

    data_record         BYTEA,

    process_metadata    JSON,
    qc_tests            JSON,

    duplicate_uuid      UUID,
    duplicate_received_date DATE,

    batch_uuid          UUID,

    PRIMARY KEY (obs_uuid, received_date),
    FOREIGN KEY (obs_uuid, received_date) REFERENCES nodb_obs(obs_uuid, received_date),
    FOREIGN KEY (source_file_uuid, received_date) REFERENCES nodb_source_files(source_uuid, received_date),
    FOREIGN KEY (duplicate_uuid, duplicate_received_date) REFERENCES nodb_obs(obs_uuid, received_date)
) PARTITION BY RANGE(received_date);


CREATE INDEX IF NOT EXISTS ix_nodb_obs_data_status ON nodb_obs_data(status) WHERE status != 'VERIFIED';

-- Unique index on observations source info to ensure we don't duplicate records from the same file.
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_obs_data_source_info ON nodb_obs_data(received_date, source_file_uuid, message_idx, record_idx);


-- Partition tables for 1980 to 2040
CREATE TABLE IF NOT EXISTS nodb_obs_data_1800_1970 PARTITION OF nodb_obs_data FOR VALUES FROM ('1800-01-01') TO ('1970-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_1970_1980 PARTITION OF nodb_obs_data FOR VALUES FROM ('1970-01-01') TO ('1980-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_1980_1990 PARTITION OF nodb_obs_data FOR VALUES FROM ('1980-01-01') TO ('1990-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_1990_2000 PARTITION OF nodb_obs_data FOR VALUES FROM ('1990-01-01') TO ('2000-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2000_2001 PARTITION OF nodb_obs_data FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2001_2002 PARTITION OF nodb_obs_data FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2002_2003 PARTITION OF nodb_obs_data FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2003_2004 PARTITION OF nodb_obs_data FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2004_2005 PARTITION OF nodb_obs_data FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2005_2006 PARTITION OF nodb_obs_data FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2006_2007 PARTITION OF nodb_obs_data FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2007_2008 PARTITION OF nodb_obs_data FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2008_2009 PARTITION OF nodb_obs_data FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2009_2010 PARTITION OF nodb_obs_data FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2010_2011 PARTITION OF nodb_obs_data FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2011_2012 PARTITION OF nodb_obs_data FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2012_2013 PARTITION OF nodb_obs_data FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2013_2014 PARTITION OF nodb_obs_data FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2014_2015 PARTITION OF nodb_obs_data FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2015_2016 PARTITION OF nodb_obs_data FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2016_2017 PARTITION OF nodb_obs_data FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2017_2018 PARTITION OF nodb_obs_data FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2018_2019 PARTITION OF nodb_obs_data FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2019_2020 PARTITION OF nodb_obs_data FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2020_2021 PARTITION OF nodb_obs_data FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2021_2022 PARTITION OF nodb_obs_data FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2022_2023 PARTITION OF nodb_obs_data FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2023_2024 PARTITION OF nodb_obs_data FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2024_2025 PARTITION OF nodb_obs_data FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2025_2026 PARTITION OF nodb_obs_data FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2026_2027 PARTITION OF nodb_obs_data FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2027_2028 PARTITION OF nodb_obs_data FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2028_2029 PARTITION OF nodb_obs_data FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2029_2030 PARTITION OF nodb_obs_data FOR VALUES FROM ('2029-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2030_2031 PARTITION OF nodb_obs_data FOR VALUES FROM ('2030-01-01') TO ('2031-01-01');


-- Table for working data
CREATE TABLE IF NOT EXISTS nodb_working (
    working_uuid            UUID            NOT NULL    PRIMARY KEY,
    db_created_date         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    source_file_uuid        UUID            NOT NULL,
    received_date           DATE            NOT NULL,
    message_idx             INTEGER         NOT NULL,
    record_idx              INTEGER         NOT NULL,

    qc_metadata             JSON,
    qc_batch_id             UUID                        REFERENCES nodb_qc_batches(batch_uuid),
    data_record             BYTEA,
    station_uuid            VARCHAR(126),
    obs_time                TIMESTAMPTZ,
    location                GEOGRAPHY(GEOMETRY, 4326),

    record_uuid             UUID,

    FOREIGN KEY (record_uuid, received_date) REFERENCES nodb_obs (obs_uuid, received_date),
    FOREIGN KEY (source_file_uuid, received_date) REFERENCES nodb_source_files (source_uuid, received_date)
);


CREATE INDEX IF NOT EXISTS idx_nodb_working_batch_id ON nodb_working(qc_batch_id);
CREATE INDEX IF NOT EXISTS idx_nodb_working_station_uuid ON nodb_working(station_uuid);
CREATE INDEX IF NOT EXISTS idx_nodb_working_obs_time ON nodb_working(obs_time);
CREATE INDEX IF NOT EXISTS idx_nodb_working_location ON nodb_working USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_nodb_working_record ON nodb_working(record_uuid, received_date);
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_working_source_info ON nodb_working(received_date, source_file_uuid, message_idx, record_idx);


-- Trigger for QC batch table modified date maintenance
CREATE OR REPLACE TRIGGER update_working_modified_date
    BEFORE UPDATE ON nodb_working
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- Table for managing the queue
CREATE TABLE IF NOT EXISTS nodb_queues (
    queue_uuid          UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    status              queue_status    NOT NULL    DEFAULT 'UNLOCKED',
    locked_by           VARCHAR(126)                DEFAULT NULL,
    locked_since        TIMESTAMPTZ                 DEFAULT NULL,
    delay_release       TIMESTAMPTZ                 DEFAULT NULL,

    escalation_level    INTEGER         NOT NULL    DEFAULT 0,
    queue_name          VARCHAR(126)    NOT NULL,
    subqueue_name       VARCHAR(126)                DEFAULT NULL,
    unique_item_name    VARCHAR(126)                DEFAULT NULL,
    priority            INTEGER         NOT NULL    DEFAULT 0,

    data                TEXT

);


CREATE INDEX IF NOT EXISTS idx_nodb_queues_status ON nodb_queues(status);
CREATE INDEX IF NOT EXISTS idx_nodb_queues_locked_since ON nodb_queues(locked_since) WHERE locked_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_queues_queue_name ON nodb_queues(queue_name, unique_item_name);



-- Trigger for queue table
CREATE OR REPLACE TRIGGER update_queues_modified_date
    BEFORE UPDATE ON nodb_queues
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


CREATE TABLE IF NOT EXISTS gts_messages (

    received_time       TIMESTAMPTZ     NOT NULL,
    message_time        TIMESTAMPTZ,
    message_format      VARCHAR(126)    NOT NULL,
    gts_header          VARCHAR(126)    NOT NULL,
    message_source      VARCHAR(126)    NOT NULL,
    subset_count        INTEGER         NOT NULL,
    cruise_ids          JSONB           NOT NULL,
    message_hash        BYTEA           NOT NULL

) PARTITION BY RANGE(received_time);


CREATE INDEX IF NOT EXISTS idx_gts_messages_received_time ON gts_messages USING BRIN(received_time);
CREATE INDEX IF NOT EXISTS idx_gts_messages_gts_header ON gts_messages(gts_header);
CREATE INDEX IF NOT EXISTS idx_gts_messages_message_source ON gts_messages(message_source);
CREATE INDEX IF NOT EXISTS idx_gts_messages_cruise_ids ON gts_messages USING GIN(cruise_ids);


-- Partition tables for 1980 to 2040
CREATE TABLE IF NOT EXISTS gts_messages_1950_1960 PARTITION OF gts_messages FOR VALUES FROM ('1950-01-01') TO ('1960-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_1960_1970 PARTITION OF gts_messages FOR VALUES FROM ('1960-01-01') TO ('1970-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_1970_1980 PARTITION OF gts_messages FOR VALUES FROM ('1970-01-01') TO ('1980-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_1980_1990 PARTITION OF gts_messages FOR VALUES FROM ('1980-01-01') TO ('1990-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_1990_2000 PARTITION OF gts_messages FOR VALUES FROM ('1990-01-01') TO ('2000-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2000_2005 PARTITION OF gts_messages FOR VALUES FROM ('2000-01-01') TO ('2005-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2005_2006 PARTITION OF gts_messages FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2006_2007 PARTITION OF gts_messages FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2007_2008 PARTITION OF gts_messages FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2008_2009 PARTITION OF gts_messages FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2009_2010 PARTITION OF gts_messages FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2010_2011 PARTITION OF gts_messages FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2011_2012 PARTITION OF gts_messages FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2012_2013 PARTITION OF gts_messages FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2013_2014 PARTITION OF gts_messages FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2014_2015 PARTITION OF gts_messages FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2015_2016 PARTITION OF gts_messages FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2016_2017 PARTITION OF gts_messages FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2017_2018 PARTITION OF gts_messages FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2018_2019 PARTITION OF gts_messages FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2019_2020 PARTITION OF gts_messages FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2020_2021 PARTITION OF gts_messages FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2021_2022 PARTITION OF gts_messages FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2022_2023 PARTITION OF gts_messages FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2023_2024 PARTITION OF gts_messages FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2024_2025 PARTITION OF gts_messages FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2025_2026 PARTITION OF gts_messages FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2026_2027 PARTITION OF gts_messages FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2027_2028 PARTITION OF gts_messages FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2028_2029 PARTITION OF gts_messages FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2029_2030 PARTITION OF gts_messages FOR VALUES FROM ('2029-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2030_2031 PARTITION OF gts_messages FOR VALUES FROM ('2030-01-01') TO ('2031-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2031_2032 PARTITION OF gts_messages FOR VALUES FROM ('2031-01-01') TO ('2032-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2032_2033 PARTITION OF gts_messages FOR VALUES FROM ('2032-01-01') TO ('2033-01-01');
CREATE TABLE IF NOT EXISTS gts_messages_2033_2031 PARTITION OF gts_messages FOR VALUES FROM ('2033-01-01') TO ('2031-01-01');



CREATE TABLE IF NOT EXISTS gts_summary (

    bulletin_origin         VARCHAR(126)    NOT NULL,
    bulletin_data_type      VARCHAR(126)    NOT NULL,
    bulletin_repeat_type    VARCHAR(126),
    bulletin_count          BIGINT          NOT NULL        DEFAULT 0,
    subset_count            BIGINT          NOT NULL        DEFAULT 0,
    bulletin_date           DATE          NOT NULL,
    PRIMARY KEY (bulletin_data_type, bulletin_origin, bulletin_repeat_type, bulletin_date)
) PARTITION BY RANGE(bulletin_date);


-- Partition tables for 2022 to 2024
CREATE TABLE IF NOT EXISTS gts_summary_2010_2011 PARTITION OF gts_summary FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2011_2012 PARTITION OF gts_summary FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2012_2013 PARTITION OF gts_summary FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2013_2014 PARTITION OF gts_summary FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2014_2015 PARTITION OF gts_summary FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2015_2016 PARTITION OF gts_summary FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2016_2017 PARTITION OF gts_summary FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2017_2018 PARTITION OF gts_summary FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2018_2019 PARTITION OF gts_summary FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2019_2020 PARTITION OF gts_summary FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2020_2021 PARTITION OF gts_summary FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2021_2022 PARTITION OF gts_summary FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2022_2023 PARTITION OF gts_summary FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2023_2024 PARTITION OF gts_summary FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2024_2025 PARTITION OF gts_summary FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2025_2026 PARTITION OF gts_summary FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2026_2027 PARTITION OF gts_summary FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2027_2028 PARTITION OF gts_summary FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2028_2029 PARTITION OF gts_summary FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2029_2030 PARTITION OF gts_summary FOR VALUES FROM ('2029-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS gts_summary_2030_2031 PARTITION OF gts_summary FOR VALUES FROM ('2030-01-01') TO ('2031-01-01');



-- Procedure to clean-up queue table and create partitions
CREATE OR REPLACE PROCEDURE run_nodb_maintenance(
    release_locks_older_than_seconds INTEGER DEFAULT 3600,
    delete_completed_older_than_seconds INTEGER DEFAULT 86400,
    delete_errors_older_than_seconds INTEGER DEFAULT 2592000
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_date  TIMESTAMPTZ;
    end_date    TIMESTAMPTZ;
    source_table_name  TEXT;
    obs_table_name  TEXT;
    obs_data_table_name  TEXT;
    gts_messages_name  TEXT;
    gts_summary_name  TEXT;
BEGIN

    -- Unlock old locked rows
    UPDATE nodb_queues
    SET
        status = 'UNLOCKED',
        locked_by = NULL,
        locked_since = NULL
    WHERE
        locked_by = 'LOCKED'
        AND locked_since < (CURRENT_TIMESTAMP(0) - (release_locks_older_than_seconds * INTERVAL '1 SECOND'));

    -- Remove completed items
    DELETE FROM nodb_queues
    WHERE
        status = 'COMPLETE'
        AND modified_date < (CURRENT_TIMESTAMP(0) - (delete_completed_older_than_seconds * INTERVAL '1 second'));

    -- Remove errored items
    DELETE FROM nodb_queues
    WHERE
        status = 'ERROR'
        AND modified_date < (CURRENT_TIMESTAMP(0) - (delete_errors_older_than_seconds * INTERVAL '1 second'));

    -- Create next years nodb_source_files partition if it doesn't exist
    start_date := DATE_TRUNC('year', CURRENT_DATE + interval '1 year');
    end_date := DATE_TRUNC('year', CURRENT_DATE + interval '2 year');
    source_table_name := 'nodb_source_files_' || DATE_PART('year', start_date)::text;
    obs_table_name := 'nodb_obs_' || DATE_PART('year', start_date)::text;
    obs_data_table_name := 'nodb_obs_data_' || DATE_PART('year', start_date)::text;
    gts_messages_name := 'gts_messages_' || DATE_PART('year', start_date)::text;
    gts_summary_name := 'gts_summary_' || DATE_PART('year', start_date)::text;
    IF NOT EXISTS (SELECT relname FROM pg_class WHERE relname = source_table_name) THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || source_table_name || ' PARTITION OF nodb_source_files FOR VALUES FROM (''' || start_date::text || ''') TO (''' || end_date::text || ''');';
    END IF;
    IF NOT EXISTS (SELECT relname FROM pg_class WHERE relname = obs_table_name) THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || obs_table_name || ' PARTITION OF nodb_obs FOR VALUES FROM (''' || start_date::text || ''') TO (''' || end_date::text || ''');';
    END IF;
    IF NOT EXISTS (SELECT relname FROM pg_class WHERE relname = obs_data_table_name) THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || obs_data_table_name || ' PARTITION OF nodb_obs_data FOR VALUES FROM (''' || start_date::text || ''') TO (''' || end_date::text || ''');';
    END IF;
    IF NOT EXISTS (SELECT relname FROM pg_class WHERE relname = gts_messages_name) THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || gts_messages_name || ' PARTITION OF gts_messages FOR VALUES FROM (''' || start_date::text || ''') TO (''' || end_date::text || ''');';
    END IF;
    IF NOT EXISTS (SELECT relname FROM pg_class WHERE relname = gts_summary_name) THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || gts_summary_name || ' PARTITION OF gts_summary FOR VALUES FROM (''' || start_date::text || ''') TO (''' || end_date::text || ''');';
    END IF;


END; $$;


-- Function to get the next queue item
CREATE OR REPLACE FUNCTION next_queue_item(
    qname VARCHAR(126),
    app_id VARCHAR(126),
    subqueue_name VARCHAR(126) DEFAULT NULL,
    max_level INTEGER DEFAULT 0
)
RETURNS UUID
AS $next_item$
DECLARE
    item_key UUID;
    selected_key UUID;
BEGIN
    IF subqueue_name IS NULL THEN
        SELECT q.queue_uuid INTO item_key
        FROM nodb_queues q
        WHERE
            q.queue_name = qname
            AND q.escalation_level <= max_level
            AND (
                q.status = 'UNLOCKED'
                OR (
                    q.status = 'DELAYED_RELEASE'
                    AND q.delay_release <= CURRENT_TIMESTAMP(0)
                )
            )
            AND (
                q.unique_item_name IS NULL
                OR q.unique_item_name NOT IN (
                    SELECT q2.unique_item_name
                    FROM nodb_queues q2
                    WHERE
                        q2.queue_name = qname
                        AND q2.status = 'LOCKED'
                )
            )
        ORDER BY priority DESC
        LIMIT 1
        FOR NO KEY UPDATE;
    ELSE
        SELECT q.queue_uuid INTO item_key
        FROM nodb_queues q
        WHERE
            q.queue_name = qname
            AND q.escalation_level <= max_level
            AND q.subqueue_name = subqueue_name
            AND (
                q.status = 'UNLOCKED'
                OR (
                    q.status = 'DELAYED_RELEASE'
                    AND q.delay_release <= CURRENT_TIMESTAMP(0)
                )
            )
            AND (
                q.unique_item_name IS NULL
                OR q.unique_item_name NOT IN (
                    SELECT q2.unique_item_name
                    FROM nodb_queues q2
                    WHERE
                        q2.queue_name = qname
                        AND q2.status = 'LOCKED'
                )
            )
        ORDER BY priority DESC
        LIMIT 1
        FOR NO KEY UPDATE;
    END IF;
    IF FOUND THEN
        UPDATE nodb_queues
        SET
            status = 'LOCKED',
            locked_by = app_id,
            locked_since = CURRENT_TIMESTAMP(0)
        WHERE
            queue_uuid = item_key
            AND status IN ('UNLOCKED', 'DELAYED_RELEASE')
        RETURNING queue_uuid INTO selected_key;
        RETURN selected_key;
    END IF;
    RETURN NULL;
END; $next_item$ LANGUAGE plpgsql;
