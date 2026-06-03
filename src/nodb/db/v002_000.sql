
-- Redo the primary key index
ALTER TABLE nodb_users DROP CONSTRAINT nodb_users_pkey CASCADE;
DROP INDEX IF EXISTS nodb_users_pkey;
CREATE UNIQUE INDEX IF NOT EXISTS ix_username ON nodb_users(username);

-- Change the user table as necessary
ALTER TABLE nodb_users ADD COLUMN identifier SERIAL PRIMARY KEY;
ALTER TABLE nodb_users ADD COLUMN allow_api_access CHAR(1) NOT NULL DEFAULT 'N';
ALTER TABLE nodb_users ADD COLUMN metadata JSON DEFAULT NULL;
ALTER TABLE nodb_users DROP COLUMN roles;

-- Update the session table
DROP INDEX IF EXISTS ix_nodb_sessions_username;
ALTER TABLE nodb_sessions DROP COLUMN username;
ALTER TABLE nodb_sessions ADD COLUMN user_id INTEGER DEFAULT NULL;

-- Access key tokens
CREATE TABLE IF NOT EXISTS nodb_access_tokens (
    user_id         VARCHAR(126)        NOT NULL,
    identifier      INTEGER             NOT NULL,

    key_hash        BYTEA                           DEFAULT NULL,
    key_salt        BYTEA                           DEFAULT NULL,
    expiry          TIMESTAMPTZ                     DEFAULT NULL,

    old_key_hash    BYTEA                           DEFAULT NULL,
    old_key_salt    BYTEA                           DEFAULT NULL,
    old_expiry      TIMESTAMPTZ                     DEFAULT NULL,

    is_active       CHAR(1)             NOT NULL    DEFAULT 'Y'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_nodb_access_tokens_user_identifier ON nodb_access_tokens(user_id, identifier);

-- Fix login table
DROP INDEX IF EXISTS ix_nodb_logins_login_time;
DROP TABLE IF EXISTS nodb_logins;

CREATE TABLE IF NOT EXISTS nodb_logins (
    user_id         INTEGER                         DEFAULT NULL,
    username        VARCHAR(126)                    DEFAULT NULL,
    success         CHAR(1)             NOT NULL,
    from_api        CHAR(1)             NOT NULL,
    message         TEXT,
    remote_addr     VARCHAR(126)                    DEFAULT NULL,
    since_last      CHAR(1)             NOT NULL    DEFAULT 'N',
    db_created_date TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_nodb_logins_search ON nodb_logins(username, success, from_api, since_last, db_created_date);

-- New organizations table
CREATE TABLE IF NOT EXISTS nodb_organizations (
    organization_id     SERIAL                      PRIMARY KEY,
    organization_name   VARCHAR(126)    NOT NULL,
    display_name        JSON            NOT NULL,
    db_created_date     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    db_modified_date    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_nodb_organization_org_name ON nodb_organizations(organization_name);

-- Trigger for source files table modified date maintenance
CREATE OR REPLACE TRIGGER update_organization_modified_date
    BEFORE UPDATE ON nodb_organizations
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_date();

-- New user-role relationship table
CREATE TABLE IF NOT EXISTS nodb_user_role (
    user_id         INTEGER             NOT NULL,
    role_name       VARCHAR(126)        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_nodb_user_roles_pkey ON nodb_user_role(user_id, role_name);

-- New user-organization relationship table
CREATE TABLE IF NOT EXISTS nodb_organization_user (
    user_id             INTEGER         NOT NULL,
    organization_id     INTEGER         NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_nodb_organization_user_pkey ON nodb_organization_user(user_id, organization_id);

-- Function to record a login
CREATE OR REPLACE FUNCTION record_login(
    username_in VARCHAR(126),
    remote_addr_in VARCHAR(126),
    success_in CHAR(1),
    from_api_in CHAR(1),
    message_in TEXT,
    max_failures INTEGER DEFAULT 0,
    max_failure_window_seconds INTEGER DEFAULT 300,
    user_lock_time_seconds INTEGER DEFAULT 3600
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    user_id INTEGER;
    login_count INTEGER;
BEGIN
    IF success_in = 'Y' AND username_in IS NOT NULL THEN
        UPDATE nodb_logins SET since_last = 'Y' WHERE "username" = username_in;
    END IF;
    SELECT "user_id" INTO user_id FROM nodb_users WHERE "username" = username_in;
    INSERT INTO nodb_logins ("username", "success", "from_api", "message", "remote_addr", "user_id") VALUES (username_in, success_in, from_api_in, message_in, remote_addr_in, user_id);
    IF user_id IS NOT NULL AND success_in = 'N' THEN
        SELECT COUNT(*) INTO login_count FROM nodb_logins WHERE "username" = username_in AND success = 'N' AND since_last = 'N' AND db_created_date >= (CURRENT_TIMESTAMP - (INTERVAL '1 second' * max_failure_window_seconds));
        IF login_count >= max_failures THEN
            UPDATE nodb_users SET locked_until = CURRENT_TIMESTAMP + (INTERVAL '1 second' * user_lock_time_seconds) WHERE "username" = username_in;
            RETURN 1;
        END IF;
    END IF;
    RETURN 0;
END; $$;