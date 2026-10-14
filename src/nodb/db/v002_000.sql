-- Temporary QC results storage
CREATE TABLE IF NOT EXISTS nodb_temporary_qc_results(
    batch_process_id    VARCHAR(1024)   NOT NULL,
    batch_identifier    VARCHAR(1024)   NOT NULL,
    outcome             INTEGER         NOT NULL,
    working_uuid        UUID            NOT NULL,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE nodb_temporary_qc_results IS "Stores temporary QC results while a batch is being processed";
COMMENT ON COLUMN nodb_temporary_qc_results.batch_process_id IS "The identifier of the worker that is running the QC";
COMMENT ON COLUMN nodb_temporary_qc_results.batch_identifier IS "The identifier of the current batch being processed";
COMMENT ON COLUMN nodb_temporary_qc_results.outcome IS "The outcome of the QC test (e.g. pass fail or review)";
COMMENT ON COLUMN nodb_temporary_qc_results.working_uuid IS "The identifier of the working record that was tested";
COMMENT ON COLUMN nodb_temporary_qc_results.db_created_date IS "The date the row was created";

CREATE INDEX IF NOT EXISTS idx_nodb_temp_qc_results_lookup ON nodb_temporary_qc_results(batch_process_id, batch_identifier, outcome);


-- Relationships between observations
CREATE TABLE IF NOT EXISTS nodb_observation_relationships(

    left_obs_uuid       UUID            NOT NULL,
    left_received_date  DATE            NOT NULL,

    relationship_type   VARCHAR(32)     NOT NULL,

    right_obs_uuid      UUID            NOT NULL,
    right_received_date DATE            NOT NULL,

    PRIMARY KEY (left_obs_uuid, left_received_date, right_obs_uuid, right_received_date, relationship_type)
);
COMMENT ON TABLE nodb_observation_relationships IS "Stores the relationships between two observations";
COMMENT ON COLUMN nodb_observation_relationships.left_obs_uuid IS "The identifier of the first (aka left) observation";
COMMENT ON COLUMN nodb_observation_relationships.left_received_date IS "The date the first (aka left) observation was received";
COMMENT ON COLUMN nodb_observation_relationships.relationship_type IS "The type of the relationship, typically a description of how LEFT relates to RIGHT";
COMMENT ON COLUMN nodb_observation_relationships.right_obs_uuid IS "The identifier of the second (aka right) observation";
COMMENT ON COLUMN nodb_observation_relationships.right_received_date IS "The date the second (aka right) observation was received";

ALTER TABLE nodb_obs_data DROP CONSTRAINT nodb_obs_data_duplicate_uuid_duplicate_received_date_fkey;
ALTER TABLE nodb_obs_data DROP COLUMN duplicate_uuid;
ALTER TABLE nodb_obs_data DROP COLUMN duplicate_received_date;


-- "Replaces" relationship for source files
ALTER TABLE nodb_source_files ADD COLUMN source_file_identifier VARCHAR(256) DEFAULT NULL;
COMMENT ON COLUMN nodb_source_files.source_file_identifier IS "A unique identifier for this data file that is the same when it is submitted";
ALTER TABLE nodb_source_files ADD COLUMN source_file_version INT DEFAULT NULL;
COMMENT ON COLUMN nodb_source_files.source_file_version IS "The version of this file (higher versions replace lower versions)";


-- Data Mode and QC flags to replace processing level
DROP INDEX IF EXISTS ix_nodb_working_source_info;
ALTER TABLE nodb_working ADD COLUMN data_mode CHAR(2) DEFAULT '??';
COMMENT ON COLUMN nodb_working.data_mode IS "The mode of the data (RT or DM)";
ALTER TABLE nodb_working ADD COLUMN quality_checks BIGINT DEFAULT 0;
COMMENT ON COLUMN nodb_working.quality_checks IS "Quality checks that have been applied to the data (as an int flag)";
ALTER TABLE nodb_working ADD COLUMN observation_identifier VARCHAR(255) DEFAULT NULL;
COMMENT ON COLUMN nodb_working.observation_identifier IS "A unique identifier for this observation that is the same (or not set) for the same observation";

DROP INDEX IF EXISTS ix_nodb_obs_data_source_info;
ALTER TABLE nodb_obs_data DROP COLUMN processing_level;
ALTER TABLE nodb_obs_data ADD COLUMN data_mode CHAR(2) DEFAULT '??';
COMMENT ON COLUMN nodb_obs_data.data_mode IS "The mode of the data (RT or DM)";
ALTER TABLE nodb_obs_data ADD COLUMN quality_checks BIGINT DEFAULT 0;
COMMENT ON COLUMN nodb_obs_data.quality_checks IS "Quality checks that have been applied to the data (as an int flag)";
ALTER TABLE nodb_obs_data ADD COLUMN observation_identifier VARCHAR(255) DEFAULT NULL;
COMMENT ON COLUMN nodb_obs_data.observation_identifier IS "A unique identifier for this observation that is the same (or not set) for the same observation";

ALTER TABLE nodb_obs DROP COLUMN processing_level;
ALTER TABLE nodb_obs ADD COLUMN data_mode CHAR(2) DEFAULT '??';
COMMENT ON COLUMN nodb_obs.data_mode IS "The mode of the data (RT or DM)";
ALTER TABLE nodb_obs ADD COLUMN quality_checks BIGINT DEFAULT 0;
COMMENT ON COLUMN nodb_obs.quality_checks IS "Quality checks that have been applied to the data (as an int flag)";
ALTER TABLE nodb_obs ADD COLUMN observation_identifier VARCHAR(255) DEFAULT NULL;
COMMENT ON COLUMN nodb_obs.observation_identifier IS "A unique identifier for this observation that is the same (or not set) for the same observation";

-- Min and max time ranges
ALTER TABLE nodb_obs ADD COLUMN min_time TIMESTAMPTZ DEFAULT NULL;
COMMENT ON COLUMN nodb_obs.min_time IS "The earliest time data was recorded for this observation";
ALTER TABLE nodb_obs ADD COLUMN max_time TIMESTAMPTZ DEFAULT NULL;
COMMENT ON COLUMN nodb_obs.max_time IS "The latest time data was recorded for this observation";
ALTER TABLE nodb_working ADD COLUMN min_time TIMESTAMPTZ DEFAULT NULL;
COMMENT ON COLUMN nodb_working.min_time IS "The earliest time data was recorded for this observation";
ALTER TABLE nodb_working ADD COLUMN max_time TIMESTAMPTZ DEFAULT NULL;
COMMENT ON COLUMN nodb_working.max_time IS "The latest time data was recorded for this observation";


-- Finalizer result table
CREATE TABLE IF NOT EXISTS nodb_temporary_finalize_results(
    object_type     CHAR(1)         NOT NULL,
    object_uuid     VARCHAR(256)    NOT NULL,
    obs_uuid        UUID            NOT NULL,
    obs_date        DATE            NOT NULL,
    result          VARCHAR(125)    NOT NULL
);
COMMENT ON TABLE nodb_temporary_finalize_results IS "Stores temporary results while finalizing a batch";
COMMENT ON COLUMN nodb_temporary_finalize_results.object_type IS "The type of object stored";
COMMENT ON COLUMN nodb_temporary_finalize_results.object_uuid IS "The unique identifier of the object";
COMMENT ON COLUMN nodb_temporary_finalize_results.obs_uuid IS "The unique identifier of the observation";
COMMENT ON COLUMN nodb_temporary_finalize_results.obs_date IS "The date the observation was received by MEDS";
COMMENT ON COLUMN nodb_temporary_finalize_results.result IS "The outcome of finalizing the object";


CREATE INDEX IF NOT EXISTS ix_nodb_temp_finalize_results ON nodb_temporary_finalize_results(object_type, object_uuid);


-- Add the ICES code
ALTER TABLE nodb_platforms ADD COLUMN ship_code VARCHAR(126) DEFAULT NULL;
COMMENT ON COLUMN nodb_platforms.ship_code IS "The ICES ship code for the platform, if one exists";

-- Add the MEDS ID (what will be used to generate cruise IDs)
ALTER TABLE nodb_platforms ADD COLUMN meds_id VARCHAR(126) DEFAULT NULL;
COMMENT ON COLUMN nodb_platforms.meds_id IS "The MEDS code (used to build the cruise ID), if one exists";

-- Add a list of instrument types
ALTER TABLE nodb_obs ADD COLUMN instrument_types JSONB DEFAULT NULL;
COMMENT ON COLUMN nodb_obs.instrument_types IS "A list of instrument types used for the observation (e.g. ctd, bottle)";

-- Product def table
CREATE TABLE IF NOT EXISTS nodb_product_definitions (
    product_name        VARCHAR(125)    NOT NULL    PRIMARY KEY,
    product_rule        JSON            DEFAULT NULL,
    metadata            JSON            DEFAULT NULL
);
COMMENT ON TABLE nodb_product_definitions IS "Stores product definitions";
COMMENT ON COLUMN nodb_product_definitions.product_name IS "A unique identifier for the product";
COMMENT ON COLUMN nodb_product_definitions.product_rule IS "A JSON dictionary or list describing the rules that must be met for the product";
COMMENT ON COLUMN nodb_product_definitions.metadata IS "Additional data about the product";

-- Product processed table
CREATE TABLE IF NOT EXISTS nodb_product_observation (
    product_name    VARCHAR(125)    NOT NULL    REFERENCES nodb_product_definitions(product_name),
    obs_uuid        UUID            NOT NULL,
    received_date   DATE            NOT NULL,
    processed       INT             NOT NULL    DEFAULT 0,

    PRIMARY KEY (product_name, obs_uuid, received_date),
    FOREIGN KEY (obs_uuid, received_date) REFERENCES nodb_obs(obs_uuid, received_date)
);
COMMENT ON TABLE nodb_product_observation IS "Stores the relationship that indicates an observation should be included in a product";
COMMENT ON COLUMN nodb_product_observation.product_name IS "The name of the product that the observation is included in";
COMMENT ON COLUMN nodb_product_observation.obs_uuid IS "The unique identifier of the observation that is being included";
COMMENT ON COLUMN nodb_product_observation.received_date IS "The date the observation was originally received by MEDS";
COMMENT ON COLUMN nodb_product_observation.processed IS "Set to 1 when the observation has been included in the product";


-- Additional identifiers and metadata for platforms
ALTER TABLE nodb_platforms ADD COLUMN imo_number VARCHAR(126) DEFAULT NULL;
COMMENT ON COLUMN nodb_platforms.imo_number IS "The platform's IMO number, if it has one";
ALTER TABLE nodb_platforms ADD COLUMN mmsi VARCHAR(126) DEFAULT NULL;
COMMENT ON COLUMN nodb_platforms.mmsi IS "The platform's MMSI number, if it has one";
ALTER TABLE nodb_platforms ADD COLUMN registry_ca VARCHAR(126) DEFAULT NULL;
COMMENT ON COLUMN nodb_platforms.registry_ca IS "The platform's Canadian registry number, if it has one";
ALTER TABLE nodb_platforms ADD COLUMN country CHAR(2) DEFAULT NULL;
COMMENT ON COLUMN nodb_platforms.country IS "The country that operates the vessel";
