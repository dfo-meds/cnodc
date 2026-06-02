

ALTER TABLE nodb_users ADD COLUMN allow_api_access CHAR(1) NOT NULL DEFAULT 'N';
ALTER TABLE nodb_users ADD COLUMN metadata JSON DEFAULT NULL;


CREATE TABLE IF NOT EXISTS nodb_access_tokens (
    username        VARCHAR(126)        NOT NULL    PRIMARY KEY,
    identifier      VARCHAR(126)        NOT NULL    PRIMARY KEY,

    key_hash        BYTEA                           DEFAULT NULL,
    key_salt        BYTEA                           DEFAULT NULL,
    expiry          TIMESTAMPTZ                     DEFAULT NULL,

    old_key_hash    BYTEA                           DEFAULT NULL,
    old_key_salt    BYTEA                           DEFAULT NULL,
    old_expiry      TIMESTAMPTZ                     DEFAULT NULL,

    is_active       CHAR(1)             NOT NULL    DEFAULT 'Y'
);

DROP INDEX IF EXISTS ix_nodb_logins_login_time;
DROP TABLE IF EXISTS nodb_logins;

CREATE TABLE IF NOT EXISTS nodb_logins (
    username        VARCHAR(126),
    success         CHAR(1)             NOT NULL,
    from_api        CHAR(1)             NOT NULL,
    message         TEXT,
    remote_addr     VARCHAR(126),
    since_last      CHAR(1)             NOT NULL    DEFAULT 'N',
    db_created_date TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS nodb_organizations (
    organization_name   VARCHAR(126)    NOT NULL    PRIMARY KEY,
    display_name        JSON            NOT NULL,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
);


-- Trigger for source files table modified date maintenance
CREATE OR REPLACE TRIGGER update_organization_modified_date
    BEFORE UPDATE ON nodb_organizations
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();



CREATE TABLE IF NOT EXISTS nodb_organization_user (
    username            VARCHAR(126)    NOT NULL    PRIMARY KEY,
    organization_name   VARCHAR(126)    NOT NULL    PRIMARY KEY
);
