-- Temporary QC results storage
CREATE TABLE IF NOT EXISTS nodb_temporary_qc_results(
    batch_process_id    VARCHAR(1024)   NOT NULL,
    batch_identifier    VARCHAR(1024)   NOT NULL,
    outcome             INTEGER         NOT NULL,
    working_uuid        UUID            NOT NULL,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nodb_temp_qc_results_lookup ON nodb_temporary_qc_results(batch_process_id, batch_identifier, outcome);


-- Relationships between observations
CREATE TABLE IF NOT EXISTS nodb_observation_relationships(

    left_obs_uuid       UUID            NOT NULL,
    left_received_date  DATE            NOT NULL,

    -- see enum for values
    relationship_type   VARCHAR(32)     NOT NULL,

    right_obs_uuid      UUID            NOT NULL,
    right_received_date DATE            NOT NULL,

    PRIMARY KEY (left_obs_uuid, left_received_date, right_obs_uuid, right_received_date, relationship_type)
);

ALTER TABLE nodb_obs_data DROP CONSTRAINT nodb_obs_data_duplicate_uuid_duplicate_received_date_fkey;
ALTER TABLE nodb_obs_data DROP COLUMN duplicate_uuid;
ALTER TABLE nodb_obs_data DROP COLUMN duplicate_received_date;


-- "Replaces" relationship for source files
ALTER TABLE nodb_source_files ADD COLUMN replaces_uuid UUID DEFAULT NULL;
ALTER TABLE nodb_source_files ADD COLUMN replaces_received_date UUID DEFAULT NULL;
ALTER TABLE nodb_source_files ADD CONSTRAINT fk_replaces FOREIGN KEY (replaces_uuid, replaces_received_date) REFERENCES nodb_source_files(source_uuid, received_date);


-- Data Mode and QC flags to replace processing level
DROP INDEX IF EXISTS ix_nodb_working_source_info;
ALTER TABLE nodb_working ADD COLUMN data_mode CHAR(2) DEFAULT '??';
ALTER TABLE nodb_working ADD COLUMN quality_checks BIGINT DEFAULT 0;

DROP INDEX IF EXISTS ix_nodb_obs_data_source_info;
ALTER TABLE nodb_obs_data DROP COLUMN processing_level;
ALTER TABLE nodb_obs_data ADD COLUMN data_mode CHAR(2) DEFAULT '??';
ALTER TABLE nodb_obs_data ADD COLUMN quality_checks BIGINT DEFAULT 0;

ALTER TABLE nodb_obs DROP COLUMN processing_level;
ALTER TABLE nodb_obs ADD COLUMN data_mode CHAR(2) DEFAULT '??';
ALTER TABLE nodb_obs ADD COLUMN quality_checks BIGINT DEFAULT 0;


-- Finalizer result table
CREATE TABLE IF NOT EXISTS nodb_temporary_finalize_results(
    object_type     CHAR(1)         NOT NULL,
    object_uuid     VARCHAR(256)    NOT NULL,
    obs_uuid        UUID            NOT NULL,
    obs_date        DATE            NOT NULL,
    result          VARCHAR(125)    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_nodb_temp_finalize_results ON nodb_temporary_finalize_results(object_type, object_uuid);
