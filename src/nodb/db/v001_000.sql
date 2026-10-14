DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        CREATE EXTENSION IF NOT EXISTS postgis;
    END IF;
END$$;

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
            'DELAYED_MODE',
            'UNKNOWN'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_status') THEN
        CREATE TYPE user_status AS ENUM (
            'ACTIVE',
            'INACTIVE'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'platform_status') THEN
        CREATE TYPE platform_status AS ENUM (
            'ACTIVE',
            'INCOMPLETE',
            'HISTORICAL',
            'REMOVED',
            'REPLACED'
        );
    END IF;
END$$;


CREATE TABLE IF NOT EXISTS nodb_version (

    lookup              INTEGER     DEFAULT 1       PRIMARY KEY,
    version_major       INTEGER                     DEFAULT NULL,
    version_minor       INTEGER                     DEFAULT NULL

);
COMMENT ON TABLE nodb_version IS "Stores the current version of the NODB to manage updates";
COMMENT ON COLUMN nodb_version.lookup IS "A look-up column for us to find the version (always 1)";
COMMENT ON COLUMN nodb_version.version_major IS "The major version (i.e. X in version X.Y)";
COMMENT ON COLUMN nodb_version.version_minor IS "The minor version (i.e. Y in version X.Y)";


CREATE TABLE IF NOT EXISTS nodb_users (

    username            VARCHAR(126)    NOT NULL    PRIMARY KEY,
    display             VARCHAR(126)                DEFAULT NULL,
    email               VARCHAR(1024)               DEFAULT NULL,

    language_pref       CHAR(2)         NOT NULL    DEFAULT 'en',
    locked_until        TIMESTAMPTZ                 DEFAULT NULL,

    phash               BYTEA,
    salt                BYTEA,

    old_phash           BYTEA                       DEFAULT NULL,
    old_salt            BYTEA                       DEFAULT NULL,
    old_expiry          TIMESTAMPTZ                 DEFAULT NULL,

    status              user_status     NOT NULL    DEFAULT 'ACTIVE',

    roles               JSON
);
COMMENT ON TABLE nodb_users IS "Stores users for the MEDSID component";
COMMENT ON COLUMN nodb_users.username IS "The username that the user logs in with";
COMMENT ON COLUMN nodb_users.display IS "The name as it is displayed to other users";
COMMENT ON COLUMN nodb_users.email IS "The user's email address";
COMMENT ON COLUMN nodb_users.language_pref IS "The user's preferred language (en or fr)";
COMMENT ON COLUMN nodb_users.locked_until IS "If set, the user cannot access the system until this time";
COMMENT ON COLUMN nodb_users.phash IS "The hash of the password";
COMMENT ON COLUMN nodb_users.salt IS "The salt used when hashing the password";
COMMENT ON COLUMN nodb_users.old_phash IS "Hash of the previous password";
COMMENT ON COLUMN nodb_users.old_salt IS "Salt of the previous password";
COMMENT ON COLUMN nodb_users.old_expiry IS "If set, the user can use their old password until this date";
COMMENT ON COLUMN nodb_users.status IS "Allows us to flag the user as INACTIVE so they can't login";
COMMENT ON COLUMN nodb_users.roles IS "A list of roles that the user has";


CREATE TABLE IF NOT EXISTS nodb_permissions (
    role_name           VARCHAR(126)    NOT NULL,
    permission          VARCHAR(126)    NOT NULL
);
COMMENT ON TABLE nodb_permissions IS "Stores the relationship between roles and permissions";
COMMENT ON COLUMN nodb_permissions.role_name IS "The name of a role that the user can be assigned";
COMMENT ON COLUMN nodb_permissions.permission IS "The name of a permission that users assigned that role will have";


CREATE TABLE IF NOT EXISTS nodb_logins (
    username            VARCHAR(126)    NOT NULL    REFERENCES nodb_users(username),
    login_time          TIMESTAMPTZ     NOT NULL,
    login_addr          VARCHAR(126)    NOT NULL,
    instance_name       VARCHAR(126)    NOT NULL
);
COMMENT ON TABLE nodb_logins IS "Stores a history of logins for users for security purposes";
COMMENT ON COLUMN nodb_logins.username IS "The user who logged in";
COMMENT ON COLUMN nodb_logins.login_time IS "The time the user logged in";
COMMENT ON COLUMN nodb_logins.login_addr IS "The IP address the user logged in from";
COMMENT ON COLUMN nodb_logins.instance_name IS "The instance name of the server where the user logged in";

CREATE INDEX IF NOT EXISTS ix_nodb_logins_login_time ON nodb_logins USING brin(login_time);


CREATE TABLE IF NOT EXISTS nodb_sessions (
    session_id          VARCHAR(126)    NOT NULL    PRIMARY KEY,
    start_time          TIMESTAMPTZ     NOT NULL,
    expiry_time         TIMESTAMPTZ     NOT NULL,
    username            VARCHAR(126)    NOT NULL    REFERENCES nodb_users(username),
    session_data        JSON
);
COMMENT ON TABLE nodb_sessions IS "Stores active user sessions";
COMMENT ON COLUMN nodb_sessions.session_id IS "The session ID";
COMMENT ON COLUMN nodb_sessions.start_time IS "The time the session started";
COMMENT ON COLUMN nodb_sessions.expiry_time IS "The time the session with expiry";
COMMENT ON COLUMN nodb_sessions.username IS "The user whose session is";
COMMENT ON COLUMN nodb_sessions.session_data IS "Data stored in the session";


CREATE INDEX IF NOT EXISTS ix_nodb_sessions_username ON nodb_sessions(username);


CREATE TABLE IF NOT EXISTS nodb_upload_workflows (
    workflow_name       VARCHAR(126)    NOT NULL    PRIMARY KEY,
    configuration       JSON,
    is_active           BOOLEAN         NOT NULL    DEFAULT TRUE
);
COMMENT ON TABLE nodb_upload_workflows IS "Stores workflow configuration, documenting how data is processed";
COMMENT ON COLUMN nodb_upload_workflows.workflow_name IS "The name of the workflow";
COMMENT ON COLUMN nodb_upload_workflows.configuration IS "Configuration for the workflow, stored as a JSON dictionary";
COMMENT ON COLUMN nodb_upload_workflows.is_active IS "Whether this workflow can be started";


CREATE TABLE IF NOT EXISTS nodb_scanned_files (
    file_path           TEXT            NOT NULL,
    modified_date       TIMESTAMPTZ                 DEFAULT NULL,
    scanned_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    was_processed       BOOLEAN                     DEFAULT FALSE,
    was_errored         BOOLEAN                     DEFAULT FALSE
);
COMMENT ON TABLE nodb_scanned_files IS "Stores files seen by the file scanner to track their download status";
COMMENT ON COLUMN nodb_scanned_files.file_path IS "The external file path of the file";
COMMENT ON COLUMN nodb_scanned_files.modified_date IS "The date the file was modified";
COMMENT ON COLUMN nodb_scanned_files.scanned_date IS "The date the file was first scanned";
COMMENT ON COLUMN nodb_scanned_files.was_processed IS "Set to true once the file is downloaded";
COMMENT ON COLUMN nodb_scanned_files.was_errored IS "Set to true if the file raised an error";

CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_scanned_files_unique ON nodb_scanned_files(file_path, modified_date);


CREATE TABLE IF NOT EXISTS nodb_source_files (
    source_uuid         UUID            NOT NULL    DEFAULT gen_random_uuid(),
    received_date       DATE            NOT NULL,

    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    source_path         TEXT,
    file_name           TEXT            NOT NULL,
    source_name         VARCHAR(126)    NOT NULL,
    program_name        VARCHAR(126)    NOT NULL,

    original_uuid       UUID,
    original_idx        INTEGER,

    metadata            JSON,
    history             JSON,

    status              source_status   NOT NULL    DEFAULT 'NEW',

    PRIMARY KEY(source_uuid, received_date)
);
COMMENT ON TABLE nodb_source_files IS "Stores a reference to a file that was used to create observation entries";
COMMENT ON COLUMN nodb_source_files.source_uuid IS "Unique identifier";
COMMENT ON COLUMN nodb_source_files.received_date IS "The date the file was received";
COMMENT ON COLUMN nodb_source_files.db_created_date IS "The date the source file row was created";
COMMENT ON COLUMN nodb_source_files.db_modified_date IS "The date the source file row was last modified";
COMMENT ON COLUMN nodb_source_files.source_path IS "The path to where the source file is stored";
COMMENT ON COLUMN nodb_source_files.file_name IS "The name of the file";
COMMENT ON COLUMN nodb_source_files.source_name IS "An identifier of who provided us with the source file";
COMMENT ON COLUMN nodb_source_files.program_name IS "An identifier of the program that manages this source file";
COMMENT ON COLUMN nodb_source_files.original_uuid IS "The original file, when this file is a subset of an original";
COMMENT ON COLUMN nodb_source_files.original_idx IS "The index of the message in the original file, when this file is a subset of the original";
COMMENT ON COLUMN nodb_source_files.metadata IS "Metadata about the source file";
COMMENT ON COLUMN nodb_source_files.history IS "A history of how the source file was processed";
COMMENT ON COLUMN nodb_source_files.status IS "The status of the source file";

CREATE INDEX IF NOT EXISTS ix_nodb_source_files_source_path ON nodb_source_files(source_path);
CREATE INDEX IF NOT EXISTS ix_nodb_source_files_created_date ON nodb_source_files USING BRIN(db_created_date);
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_source_files_original_data ON nodb_source_files(received_date, original_uuid, original_idx) WHERE original_uuid IS NOT NULL;

-- Trigger for source files table modified date maintenance
CREATE OR REPLACE TRIGGER update_source_file_modified_date
    BEFORE UPDATE ON nodb_source_files
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- Table for missions
CREATE TABLE IF NOT EXISTS nodb_missions (

    mission_uuid    UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    mission_id      VARCHAR(126)    NOT NULL,
    metadata        JSON,
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ
);
COMMENT ON TABLE nodb_missions IS "Stores unique missions to collect oceanographic data";
COMMENT ON COLUMN nodb_missions.mission_uuid IS "A unique identifier for this mission in the database";
COMMENT ON COLUMN nodb_missions.mission_id IS "The official MEDS identifier for this mission";
COMMENT ON COLUMN nodb_missions.metadata IS "Additional metadata about the mission";
COMMENT ON COLUMN nodb_missions.start_date IS "The datetime when the mission began collecting data";
COMMENT ON COLUMN nodb_missions.end_date IS "The datetime when the mission stopped collecting data";


-- Table for platform records
CREATE TABLE IF NOT EXISTS nodb_platforms (
    platform_uuid       UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    wmo_id              VARCHAR(126),
    wigos_id            VARCHAR(126),
    platform_name       VARCHAR(126),
    platform_id         VARCHAR(126),
    platform_type       VARCHAR(126)    NOT NULL,
    service_start_date  TIMESTAMPTZ,
    service_end_date    TIMESTAMPTZ,
    instrumentation     JSONB,
    metadata            JSON,
    map_to_uuid         UUID                        REFERENCES nodb_platforms(platform_uuid),
    status              platform_status  NOT NULL   DEFAULT 'ACTIVE',
    embargo_data_days   INTEGER
);
COMMENT ON TABLE nodb_platforms IS "Stores platforms that are responsible for collecting ocean data";
COMMENT ON COLUMN nodb_platforms.platform_uuid IS "A unique identifier for the platform in the database";
COMMENT ON COLUMN nodb_platforms.wmo_id IS "The 5 or 7 digit WMO identifier for the platform";
COMMENT ON COLUMN nodb_platforms.wigos_id IS "The WIGOS identifier as a string delimited by hyphens (e.g. 0-0-0-ABC)";
COMMENT ON COLUMN nodb_platforms.platform_name IS "The full name of the platform, e.g. the ship's name";
COMMENT ON COLUMN nodb_platforms.platform_id IS "A short identifier for the platform, e.g. the ship's callsign";
COMMENT ON COLUMN nodb_platforms.platform_type IS "The platform type, e.g. glider or ship";
COMMENT ON COLUMN nodb_platforms.service_start_date IS "The first date this platform was in service";
COMMENT ON COLUMN nodb_platforms.service_end_date IS "The last date this platform was in service";
COMMENT ON COLUMN nodb_platforms.instrumentation IS "A list of instruments on the platform";
COMMENT ON COLUMN nodb_platforms.metadata IS "Additional metadata about the platform";
COMMENT ON COLUMN nodb_platforms.map_to_uuid IS "If this entry is a duplicate of another platform, this will have the actual UUID of the best platform";
COMMENT ON COLUMN nodb_platforms.status IS "Whether the platform is still active";
COMMENT ON COLUMN nodb_platforms.embargo_data_days IS "If greater than zero, data will not be published for the given number of days";


CREATE INDEX IF NOT EXISTS idx_nodb_platforms_wmo_id ON nodb_platforms(wmo_id) WHERE wmo_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_platforms_wigos_id ON nodb_platforms(wigos_id) WHERE wigos_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_platforms_platform_name ON nodb_platforms(platform_name) WHERE platform_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_platforms_platform_id ON nodb_platforms(platform_id) WHERE platform_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_platforms_service_start_date ON nodb_platforms(service_start_date);
CREATE INDEX IF NOT EXISTS idx_nodb_platforms_service_end_date ON nodb_platforms(service_end_date);
CREATE INDEX IF NOT EXISTS idx_nodb_platforms_instrumentation ON nodb_platforms USING GIN(instrumentation);


-- QC batch items (batches are processed as a single item)
CREATE TABLE IF NOT EXISTS nodb_qc_batches (
    batch_uuid          UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    metadata            JSON,
    status              qc_status       NOT NULL
);
COMMENT ON TABLE nodb_qc_batches IS "Stores metadata for batches of QC items being processed together";
COMMENT ON COLUMN nodb_qc_batches.batch_uuid IS "A unique identifier for the batch";
COMMENT ON COLUMN nodb_qc_batches.db_created_date IS "The date the batch was created";
COMMENT ON COLUMN nodb_qc_batches.db_modified_date IS "The date the batch was last modified";
COMMENT ON COLUMN nodb_qc_batches.metadata IS "Additional metadata about the batch";
COMMENT ON COLUMN nodb_qc_batches.status IS "The status of the batch";

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

    platform_uuid       UUID                        REFERENCES nodb_platforms(platform_uuid),
    mission_uuid        UUID                        REFERENCES nodb_missions(mission_uuid),
    obs_time            TIMESTAMPTZ,
    min_depth           REAL,
    max_depth           REAL,
    location            GEOGRAPHY(GEOMETRY, 4326),

    observation_type    obs_type,

    surface_parameters  JSONB,
    profile_parameters  JSONB,
    processing_level    processing_level        NOT NULL    DEFAULT 'UNKNOWN',
    embargo_date        TIMESTAMPTZ,

    PRIMARY KEY (obs_uuid, received_date)
) PARTITION BY RANGE(received_date);
COMMENT ON TABLE nodb_obs IS "Stores data about observations that is commonly used in searches";
COMMENT ON COLUMN nodb_obs.obs_uuid IS "A unique identifier for this observation, in combination with the received date";
COMMENT ON COLUMN nodb_obs.received_date IS "The date this observation was received by MEDS";
COMMENT ON COLUMN nodb_obs.db_created_date IS "The date this observation was created in the database";
COMMENT ON COLUMN nodb_obs.db_modified_date IS "The date this observation was last modified";
COMMENT ON COLUMN nodb_obs.platform_uuid IS "The platform that took this observation";
COMMENT ON COLUMN nodb_obs.mission_uuid IS "The mission that collected this observation";
COMMENT ON COLUMN nodb_obs.obs_time IS "The time the observation was taken";
COMMENT ON COLUMN nodb_obs.min_depth IS "The shallowest depth (as a positive integer) that this observation was taken at";
COMMENT ON COLUMN nodb_obs.max_depth IS "The deepest depth (as a positive integer) that this observation was taken at";
COMMENT ON COLUMN nodb_obs.location IS "The area where this observation was taken at";
COMMENT ON COLUMN nodb_obs.observation_type IS "The type of observation (e.g. profile vs. at depth vs. surface)";
COMMENT ON COLUMN nodb_obs.surface_parameters IS "A JSON list of parameters observed at the surface";
COMMENT ON COLUMN nodb_obs.profile_parameters IS "A JSON list of parameters observed below the surface";
COMMENT ON COLUMN nodb_obs.processing_level IS "The level to which this observation was processed (replaced by data_mode)";
COMMENT ON COLUMN nodb_obs.embargo_date IS "The date until which this observation will be held";


CREATE INDEX IF NOT EXISTS nodb_obs_platform_uuid ON nodb_obs(platform_uuid);
CREATE INDEX IF NOT EXISTS nodb_obs_mission_uuid ON nodb_obs(mission_uuid);
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
CREATE TABLE IF NOT EXISTS nodb_obs_2000_2010 PARTITION OF nodb_obs FOR VALUES FROM ('2000-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2010_2020 PARTITION OF nodb_obs FOR VALUES FROM ('2010-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2020_2030 PARTITION OF nodb_obs FOR VALUES FROM ('2020-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_2030_2040 PARTITION OF nodb_obs FOR VALUES FROM ('2030-01-01') TO ('2040-01-01');



CREATE TABLE IF NOT EXISTS nodb_obs_data (
    obs_uuid            UUID            NOT NULL,
    received_date       DATE            NOT NULL,

    source_file_uuid    UUID            NOT NULL,
    message_idx         INTEGER         NOT NULL,
    record_idx          INTEGER         NOT NULL,
    processing_level    processing_level        NOT NULL    DEFAULT 'UNKNOWN',

    status              obs_status      NOT NULL DEFAULT 'VERIFIED',

    data_record         BYTEA,

    metadata            JSON,
    qc_tests            JSON,

    duplicate_uuid      UUID,
    duplicate_received_date DATE,

    batch_uuid          UUID,

    PRIMARY KEY (obs_uuid, received_date),
    FOREIGN KEY (obs_uuid, received_date) REFERENCES nodb_obs(obs_uuid, received_date),
    FOREIGN KEY (source_file_uuid, received_date) REFERENCES nodb_source_files(source_uuid, received_date),
    FOREIGN KEY (duplicate_uuid, duplicate_received_date) REFERENCES nodb_obs(obs_uuid, received_date)
) PARTITION BY RANGE(received_date);
COMMENT ON TABLE nodb_obs_data IS "Stores the full OCPROC2 record of an observation and any additional data not normally searched for";
COMMENT ON COLUMN nodb_obs_data.obs_uuid IS "A unique identifier for the observation (must match that of nodb_obs entry)";
COMMENT ON COLUMN nodb_obs_data.received_date IS "The date the observation was received by MEDS (must match that of nodb_obs entry)";
COMMENT ON COLUMN nodb_obs_data.source_file_uuid IS "The source file that the observation came from";
COMMENT ON COLUMN nodb_obs_data.message_idx IS "The index of the message within the source file";
COMMENT ON COLUMN nodb_obs_data.record_idx IS "The index of the record within the message";
COMMENT ON COLUMN nodb_obs_data.processing_level IS "The processing level of the record (replaced by data_mode)";
COMMENT ON COLUMN nodb_obs_data.status IS "The status of the observation";
COMMENT ON COLUMN nodb_obs_data.data_record IS "The OCPROC2-BIN encoded record";
COMMENT ON COLUMN nodb_obs_data.metadata IS "Additional metadata about the record";
COMMENT ON COLUMN nodb_obs_data.qc_tests IS "A list of QC tests that have been performed";
COMMENT ON COLUMN nodb_obs_data.duplicate_uuid IS "If this record is a duplicate of another, points to the better record";
COMMENT ON COLUMN nodb_obs_data.duplicate_received_date IS "If this record is a duplicate of another, points to the better record";
COMMENT ON COLUMN nodb_obs_data.batch_uuid IS "If this record is assigned to a batch, points to the batch";


CREATE INDEX IF NOT EXISTS ix_nodb_obs_data_status ON nodb_obs_data(status) WHERE status != 'VERIFIED';

-- Unique index on observations source info to ensure we don't duplicate records from the same file.
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_obs_data_source_info ON nodb_obs_data(received_date, source_file_uuid, message_idx, record_idx, processing_level);


-- Partition tables for 1980 to 2040
CREATE TABLE IF NOT EXISTS nodb_obs_data_1800_1970 PARTITION OF nodb_obs_data FOR VALUES FROM ('1800-01-01') TO ('1970-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_1970_1980 PARTITION OF nodb_obs_data FOR VALUES FROM ('1970-01-01') TO ('1980-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_1980_1990 PARTITION OF nodb_obs_data FOR VALUES FROM ('1980-01-01') TO ('1990-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_1990_2000 PARTITION OF nodb_obs_data FOR VALUES FROM ('1990-01-01') TO ('2000-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2000_2010 PARTITION OF nodb_obs_data FOR VALUES FROM ('2000-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2010_2020 PARTITION OF nodb_obs_data FOR VALUES FROM ('2010-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2020_2030 PARTITION OF nodb_obs_data FOR VALUES FROM ('2020-01-01') TO ('2030-01-01');
CREATE TABLE IF NOT EXISTS nodb_obs_data_2030_2040 PARTITION OF nodb_obs_data FOR VALUES FROM ('2030-01-01') TO ('2040-01-01');


-- Table for working data
CREATE TABLE IF NOT EXISTS nodb_working (
    working_uuid            UUID            NOT NULL    PRIMARY KEY,
    db_created_date         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date        TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    source_file_uuid        UUID            NOT NULL,
    received_date           DATE            NOT NULL,
    message_idx             INTEGER         NOT NULL,
    record_idx              INTEGER         NOT NULL,

    metadata                JSON,
    qc_batch_id             UUID                        REFERENCES nodb_qc_batches(batch_uuid),
    data_record             BYTEA,
    platform_uuid           UUID                        REFERENCES nodb_platforms(platform_uuid),
    obs_time                TIMESTAMPTZ,
    location                GEOGRAPHY(GEOMETRY, 4326),

    record_uuid             UUID,

    FOREIGN KEY (record_uuid, received_date) REFERENCES nodb_obs (obs_uuid, received_date),
    FOREIGN KEY (source_file_uuid, received_date) REFERENCES nodb_source_files (source_uuid, received_date)
);
COMMENT ON TABLE nodb_working IS "Stores a record that is currently being processed by PIPEMAN";
COMMENT ON COLUMN nodb_working.working_uuid IS "A unique identifier for the record";
COMMENT ON COLUMN nodb_working.db_created_date IS "The date the record was created in the database";
COMMENT ON COLUMN nodb_working.db_modified_date IS "The date the record was last modified";
COMMENT ON COLUMN nodb_working.source_file_uuid IS "The source file from which this record was created";
COMMENT ON COLUMN nodb_working.received_date IS "The date the source file was received";
COMMENT ON COLUMN nodb_working.message_idx IS "The index of the message from which this record was created";
COMMENT ON COLUMN nodb_working.record_idx IS "The index of the record within the message from which this record was created";
COMMENT ON COLUMN nodb_working.metadata IS "Additional metadata about the working record";
COMMENT ON COLUMN nodb_working.qc_batch_id IS "If the record is currently part of a batch, this points to the batch";
COMMENT ON COLUMN nodb_working.data_record IS "The OCPROC2-BIN encoded observation";
COMMENT ON COLUMN nodb_working.platform_uuid IS "The platform that collected the observation";
COMMENT ON COLUMN nodb_working.obs_time IS "The time the observation was collected";
COMMENT ON COLUMN nodb_working.location IS "Where the observation was collected";
COMMENT ON COLUMN nodb_working.record_uuid IS "(I don't remember what this is?)";


CREATE INDEX IF NOT EXISTS idx_nodb_working_batch_id ON nodb_working(qc_batch_id);
CREATE INDEX IF NOT EXISTS idx_nodb_working_platform_uuid ON nodb_working(platform_uuid);
CREATE INDEX IF NOT EXISTS idx_nodb_working_obs_time ON nodb_working(obs_time);
CREATE INDEX IF NOT EXISTS idx_nodb_working_location ON nodb_working USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_nodb_working_record ON nodb_working(record_uuid, received_date);
CREATE UNIQUE INDEX IF NOT EXISTS ix_nodb_working_source_info ON nodb_working(received_date, source_file_uuid, message_idx, record_idx);


-- Trigger for QC batch table modified date maintenance
CREATE OR REPLACE TRIGGER update_working_modified_date
    BEFORE UPDATE ON nodb_working
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


CREATE TABLE IF NOT EXISTS nodb_processes (
    system_id           VARCHAR(1024)   NOT NULL,
    process_id          VARCHAR(1024)   NOT NULL,

    process_name        VARCHAR(1024)   NOT NULL,
    process_version     VARCHAR(1024)   NOT NULL,

    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    exited              CHAR(1)         NOT NULL    DEFAULT 'N',

    info                JSON            NOT NULL
);
COMMENT ON TABLE nodb_processes IS "Stores information about active workers for PIPEMAN";
COMMENT ON COLUMN nodb_processes.system_id IS "The name of the computing environment running the worker";
COMMENT ON COLUMN nodb_processes.process_id IS "The ID of the worker";
COMMENT ON COLUMN nodb_processes.process_name IS "The name of the worker";
COMMENT ON COLUMN nodb_processes.process_version IS "The version of the worker";
COMMENT ON COLUMN nodb_processes.db_created_date IS "The date the row was created";
COMMENT ON COLUMN nodb_processes.db_modified_date IS "The date the row was last modified";
COMMENT ON COLUMN nodb_processes.exited IS "Set to Y when the process exits to cleanup";
COMMENT ON COLUMN nodb_processes.info IS "Key-value dictionary of information about the process";


CREATE UNIQUE INDEX IF NOT EXISTS idx_nodb_processes_id ON nodb_processes(system_id, process_id);

-- Trigger for QC batch table modified date maintenance
CREATE OR REPLACE TRIGGER update_processes_modified_date
    BEFORE UPDATE ON nodb_processes
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();


-- Table for managing the queue
CREATE TABLE IF NOT EXISTS nodb_queues (
    queue_uuid          UUID            NOT NULL    DEFAULT gen_random_uuid() PRIMARY KEY,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_closed_date      TIMESTAMPTZ                 DEFAULT NULL,
    db_locked_date      TIMESTAMPTZ                 DEFAULT NULL,

    status              queue_status    NOT NULL    DEFAULT 'UNLOCKED',
    locked_by           VARCHAR(126)                DEFAULT NULL,
    locked_since        TIMESTAMPTZ                 DEFAULT NULL,
    delay_release       TIMESTAMPTZ                 DEFAULT NULL,
    tag                 VARCHAR(126)                DEFAULT NULL,

    escalation_level    INTEGER         NOT NULL    DEFAULT 0,
    queue_name          VARCHAR(126)    NOT NULL,
    subqueue_name       VARCHAR(126)                DEFAULT NULL,
    unique_item_name    VARCHAR(126)                DEFAULT NULL,
    priority            INTEGER         NOT NULL    DEFAULT 0,
    correlation_id      UUID            NOT NULL    DEFAULT gen_random_uuid(),

    data                TEXT

);
COMMENT ON TABLE nodb_queues IS "Stores queue items that are being processed or recently finished";
COMMENT ON COLUMN nodb_queues.queue_uuid IS "A unique identifier for the queue item";
COMMENT ON COLUMN nodb_queues.db_created_date IS "The date that the queue item row was created";
COMMENT ON COLUMN nodb_queues.db_modified_date IS "The date that the queue item row was last modified";
COMMENT ON COLUMN nodb_queues.db_closed_date IS "The date that the queue item row was finished";
COMMENT ON COLUMN nodb_queues.db_locked_date IS "The date that the queue item row was locked (not cleared after lock released)";
COMMENT ON COLUMN nodb_queues.status IS "The status of the queue item";
COMMENT ON COLUMN nodb_queues.locked_by IS "The name of the worker that is currently processing the item";
COMMENT ON COLUMN nodb_queues.locked_since IS "The date that the queue item was locked (cleared after lock released)";
COMMENT ON COLUMN nodb_queues.delay_release IS "The date on which the queue item should be unlocked after a delayed release is requested";
COMMENT ON COLUMN nodb_queues.tag IS "The name of the workflow or other application that added the queue item";
COMMENT ON COLUMN nodb_queues.escalation_level IS "Set to 0 for most cases, 1 indicates elevated privileges are required";
COMMENT ON COLUMN nodb_queues.queue_name IS "The name of the queue that this item is in";
COMMENT ON COLUMN nodb_queues.subqueue_name IS "If relevant, the name of any subqueue that the item is in";
COMMENT ON COLUMN nodb_queues.unique_item_name IS "If provided, two items in the same queue and subqueue with the same unique item name will not be processed concurrently";
COMMENT ON COLUMN nodb_queues.priority IS "Lower numbers are processed before higher numbers";
COMMENT ON COLUMN nodb_queues.correlation_id IS "A correlation ID that is generated when the first queue item is generated and passed on to all subsequent queue items";
COMMENT ON COLUMN nodb_queues.data IS "Data stored for the queue item (as JSON object)";


CREATE INDEX IF NOT EXISTS idx_nodb_queues_status ON nodb_queues(status);
CREATE INDEX IF NOT EXISTS idx_nodb_queues_locked_since ON nodb_queues(locked_since) WHERE locked_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodb_queues_queue_name ON nodb_queues(queue_name, unique_item_name);



-- Trigger for queue table
CREATE OR REPLACE TRIGGER update_queues_modified_date
    BEFORE UPDATE ON nodb_queues
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();



-- Procedure to clean-up queue table and create partitions
CREATE OR REPLACE PROCEDURE run_nodb_maintenance(
    release_locks_older_than_seconds INTEGER DEFAULT 3600,
    delete_completed_older_than_seconds INTEGER DEFAULT 2592000,
    delete_errors_older_than_seconds INTEGER DEFAULT 2592000,
    exit_processes_older_than_seconds INTEGER DEFAULT 86400
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_date  TIMESTAMPTZ;
    end_date    TIMESTAMPTZ;
    source_table_name  TEXT;
    obs_table_name  TEXT;
    obs_data_table_name  TEXT;
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

    -- Flag processes as exited if they haven't been heard from recently
    UPDATE nodb_processes
    SET
        exited = 'Y'
    WHERE
        exited = 'N'
        AND db_modified_date < (CURRENT_TIMESTAMP(0) - (exit_processes_older_than_seconds * INTERVAL '1 SECOND'));

    -- Remove completed items
    DELETE FROM nodb_queues
    WHERE
        status = 'COMPLETE'
        AND db_modified_date < (CURRENT_TIMESTAMP(0) - (delete_completed_older_than_seconds * INTERVAL '1 second'));

    -- Remove errored items
    DELETE FROM nodb_queues
    WHERE
        status = 'ERROR'
        AND db_modified_date < (CURRENT_TIMESTAMP(0) - (delete_errors_older_than_seconds * INTERVAL '1 second'));

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
            locked_since = CURRENT_TIMESTAMP(0),
            db_locked_date = CURRENT_TIMESTAMP(0),
            db_closed_date = NULL
        WHERE
            queue_uuid = item_key
            AND status IN ('UNLOCKED', 'DELAYED_RELEASE')
        RETURNING queue_uuid INTO selected_key;
        RETURN selected_key;
    END IF;
    RETURN NULL;
END; $next_item$ LANGUAGE plpgsql;
