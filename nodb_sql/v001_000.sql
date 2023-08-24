-- Function that automatically sets the modified date on update
CREATE OR REPLACE FUNCTION update_modified_date()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_date = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'source_status') THEN
        CREATE TYPE source_status AS ENUM (
            'NEW',
            'QUEUED',
            'QUEUE_ERROR',
            'IN_PROGRESS',
            'ERROR',
            'COMPLETE'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'qc_status') THEN
        CREATE TYPE qc_status AS ENUM (
            'UNCHECKED'
            'IN_PROGRESS',
            'REVIEW'
            'ERROR',
            'COMPLETE',
            'DISCARD'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'wobs_status') THEN
        CREATE TYPE wobs_status AS ENUM (
            'NEW',
            'AUTO_QUEUED',
            'AUTO_IN_PROGRESS',
            'USER_QUEUED',
            'USER_IN_PROGRESS',
            'USER_CHECKED',
            'QUEUE_ERROR',
            'ERROR'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'obs_status') THEN
        CREATE TYPE obs_status AS ENUM (
            'UNVERIFIED',
            'VERIFIED',
            'RTQC_PASS'
            'DMQC_PASS',
            'DISCARDED',
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'station_status') THEN
        CREATE TYPE station_status AS ENUM (
            'INCOMPLETE',
            'ACTIVE',
            'INACTIVE'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_status') THEN
        CREATE TYPE user_status AS ENUM (
            'ACTIVE',
            'INACTIVE'
        );
    END IF;
END$$


CREATE TABLE IF NOT EXISTS nodb_users (
    username            VARCHAR(127)    NOT NULL    PRIMARY KEY,
    phash               BYTEA,
    salt                BYTEA,
    status              user_status
);


CREATE TABLE IF NOT EXISTS nodb_logins (
    pkey                UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    username            VARCHAR(127)    NOT NULL    REFERENCES nodb_users(username),
    login_time          TIMESTAMPTZ     NOT NULL,
    login_addr          VARCHAR(127)    NOT NULL
);


CREATE TABLE IF NOT EXISTS nodb_sessions (
    pkey                UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    session_id          VARCHAR(127)    NOT NULL    UNIQUE,
    start_time          TIMESTAMPTZ     NOT NULL,
    expiry_time         TIMESTAMPTZ     NOT NULL,
    username            VARCHAR(127)    NOT NULL    REFERENCES nodb_users(username),
    session_data        JSON
);


-- Source Files Table
CREATE TABLE IF NOT EXISTS nodb_source_files (
    pkey                UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    created_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    modified_date       TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    source_path         TEXT            NOT NULL    UNIQUE,
    persistent_path     TEXT                        UNIQUE,
    file_name           TEXT            NOT NULL,

    original_uuid       UUID,
    original_idx        INTEGER,

    metadata            JSON,

    history             TEXT,

    status              source_status   NOT NULL    DEFAULT 'NEW',
);


-- Unique index on station original info to ensure we don't duplicate
CREATE UNIQUE INDEX ix_nodb_source_files_org
    ON nodb_source_files(original_uuid, original_idx);




-- Trigger for source files table modified date maintenance
CREATE TRIGGER update_source_file_modified_date
    BEFORE UPDATE ON nodb_source_files
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- Table documenting the QC processes
CREATE TABLE IF NOT EXISTS nodb_qc_process (
    machine_name        VARCHAR(127)    NOT NULL    PRIMARY KEY,
    version_no          INTEGER,
    rt_qc_steps         JSON,
    dm_qc_steps         JSON,
    dm_qc_freq_days     INTEGER,
    dm_qc_delay_days    INTEGER
);


-- Table documenting the station types
CREATE TABLE IF NOT EXISTS nodb_station_types (
    machine_name        VARCHAR(127)    NOT NULL    PRIMARY KEY,
    default_metadata    JSON,
    qc_process_name     VARCHAR(127)                REFERENCES nodb_qc_process(machine_name)
);


-- Table for station records
CREATE TABLE IF NOT EXISTS nodb_stations (
    pkey                UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    station_type_name   VARCHAR(127)                REFERENCES nodb_station_types(machine_name),
    wmo_id              VARCHAR(127),
    wigos_id            VARCHAR(127),
    station_name        VARCHAR(127),
    station_id          VARCHAR(127),
    map_to_uuid         UUID                        REFERENCES nodb_stations(pkey),
    created_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    modified_date       TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    metadata            JSON,
    status              station_status  NOT NULL
);


-- Trigger for stations table modified date maintenance
CREATE TRIGGER update_station_modified_date
    BEFORE UPDATE ON nodb_stations
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- QC batch items (batches are processed as a single item)
CREATE TABLE IF NOT EXISTS nodb_qc_batches (
    pkey                UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    created_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    modified_date       TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    qc_process_name     VARCHAR(127),
    qc_current_step     INTEGER,
    qc_metadata         JSON,
    working_status      wobs_status     NOT NULL
);


-- Trigger for QC batch table modified date maintenance
CREATE TRIGGER update_qc_batch_modified_date
    BEFORE UPDATE ON nodb_qc_batches
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- Table for observations
CREATE TABLE IF NOT EXISTS nodb_obs (
    pkey                UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    created_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    modified_date       TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    source_file_uuid    UUID            NOT NULL    REFERENCES nodb_source_files(pkey),
    message_idx         INTEGER         NOT NULL,
    record_idx          INTEGER         NOT NULL,

    station_uuid        UUID            NOT NULL    REFERENCES nodb_stations(pkey),
    mission_name        TEXT,

    obs_time            TIMESTAMPTZ,
    latitude            REAL,
    longitude           REAL,

    status              obs_status      NOT NULL,
    duplicate_uuid      UUID                        REFERENCES nodb_obs(pkey),

    metadata            JSON,
    search_data         JSONB,
    data_record         BYTEA
);


-- Index the searchable metadata
CREATE INDEX ix_nodb_search ON nodb_observations USING gin(search_data);


-- Unique index on observations source info to ensure we don't duplicate records from the same file.
CREATE UNIQUE INDEX ix_nodb_observations_smr
    ON nodb_obs(source_file_uuid, record_idx, message_idx);


-- Trigger for QC batch table modified date maintenance
CREATE TRIGGER update_obs_modified_date
    BEFORE UPDATE ON nodb_obs
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();



-- Table for extra QC data for observations
CREATE TABLE IF NOT EXISTS nodb_working_obs (
    pkey                UUID            NOT NULL    PRIMARY KEY REFERENCES nodb_obs(pkey),
    created_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    modified_date       TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    qc_test_status      qc_status,
    qc_batch_id         UUID                        REFERENCES nodb_qc_batches(pkey),
    qc_metadata         JSON

    station_uuid        UUID                        REFERENCES nodb_stations(pkey),

    metadata            JSONB,
    data_record         BYTEA
);


-- Trigger for QC batch table modified date maintenance
CREATE TRIGGER update_working_obs_modified_date
    BEFORE UPDATE ON nodb_working_obs
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();

