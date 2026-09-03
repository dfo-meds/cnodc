

-- created_date (auditable)
-- modified_date (auditable
-- created_by (auditable)
-- short_name 255
-- display_names JSON


CREATE TABLE IF NOT EXISTS dmd_entity (
    entity_id           SERIAL          PRIMARY KEY,
    entity_uuid         VARCHAR(255)    DEFAULT NULL,
    entity_type         VARCHAR(255)    NOT NULL,

    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    created_by          INTEGER         DEFAULT NULL,

    short_name          VARCHAR(255)    NOT NULL,
    display_names       JSON            NOT NULL,

    organization_id     INTEGER         DEFAULT NULL,

    parent_id           INTEGER         DEFAULT NULL,
    parent_type         VARCHAR(128)    DEFAULT NULL,

    is_deprecated       INTEGER         DEFAULT 0,

);


CREATE TABLE IF NOT EXISTS dmd_entity_data (
    entity_id           INTEGER         NOT NULL,
    revision_no         INTEGER         NOT NULL,
    data                JSON            NOT NULL
);


CREATE TABLE IF NOT EXISTS dmd_dataset (
    dataset_id          SERIAL          PRIMARY KEY,
    dataset_uuid        VARCHAR(255)    NOT NULL,
    authority           VARCHAR(255)    NOT NULL,

    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    created_by          INTEGER         DEFAULT NULL,

    short_name          VARCHAR(255)    NOT NULL,
    display_names       JSON            NOT NULL,

    organization_id     INTEGER         DEFAULT NULL,

    profiles            JSON            NOT NULL,
    pub_workflow        VARCHAR(255)    NOT NULL,
    act_workflow        VARCHAR(255)    NOT NULL,
    security_level      VARCHAR(255)    NOT NULL,

    status              VARCHAR(255)    NOT NULL,

    is_deprecated       INTEGER         DEFAULT 0,

    activation_item_id  INTEGER         DEFAULT NULL,

);


CREATE TABLE IF NOT EXISTS dmd_dataset_version (

    dataset_id          INTEGER         NOT NULL,
    revision_no         INTEGER         NOT NULL,
    data                JSON            NOT NULL,

    published_date      TIMESTAMPTZ     DEFAULT NULL,
    approval_item_id    INTEGER         DEFAULT NULL
    
);