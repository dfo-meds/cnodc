


CREATE TABLE IF NOT EXISTS users (
    user_id             SERIAL          NOT NULL    PRIMARY KEY
    username            VARCHAR(126)    NOT NULL    UNIQUE,
    email               VARCHAR(1024)               UNIQUE  DEFAULT NULL,
    email_verified      CHAR(1)                     DEFAULT 'N'

    name                VARCHAR(126)                DEFAULT NULL,

    language_pref       CHAR(2)         NOT NULL    DEFAULT 'en',
    locked_until        TIMESTAMPTZ                 DEFAULT NULL,

    phash               BYTEA                       DEFAULT NULL,
    salt                BYTEA                       DEFAULT NULL,

    old_phash           BYTEA                       DEFAULT NULL,
    old_salt            BYTEA                       DEFAULT NULL,
    old_expiry          TIMESTAMPTZ                 DEFAULT NULL,

    hotp                VARCHAR(126)                DEFAULT NULL,
    hotp_count          INTEGER                     DEFAULT NULL,

    totp                VARCHAR(126)                DEFAULT NULL,

    active              CHAR(1)                     DEFAULT 'Y',

    last_success        TIMESTAMPTZ                 DEFAULT NULL,
    last_error          TIMESTAMPTZ                 DEFAULT NULL,
    last_success_ip     VARCHAR(256)                DEFAULT NULL,
    last_error_ip       VARCHAR(256)                DEFAULT NULL,
    total_errors        INTEGER                     DEFAULT 0,
);


CREATE TABLE IF NOT EXISTS applications (
    app_id              SERIAL          NOT NULL        PRIMARY KEY,
    app_str_id          VARCHAR(32)     NOT NULL        UNIQUE,
    name                VARCHAR(126)    NOT NULL        UNIQUE,
    display             JSON            DEFAULT NULL,
    oauth_config        JSON            DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS applications (
    app_id              SERIAL          NOT NULL        PRIMARY KEY,
    name                VARCHAR(126)    NOT NULL        UNIQUE,
    display             JSON            DEFAULT NULL,
);

CREATE TABLE IF NOT EXISTS permissions (
    permission_id       SERIAL          NOT NULL        PRIMARY KEY,
    app_id              INTEGER         NOT NULL        REFERENCES applications(app_id),
    name                VARCHAR(126)    NOT NULL,
    display             JSON            DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS application_roles (
    app_role_id         SERIAL          NOT NULL        PRIMARY KEY,
    app_id              INTEGER         NOT NULL        REFERENCES applications(app_id),
    name                VARCHAR(126)    NOT NULL        UNIQUE,
    display             JSON            DEFAULT NULL
)

CREATE TABLE IF NOT EXISTS permissions_application_roles (
    app_role_id         INTEGER         NOT NULL        REFERENCES application_roles(app_role_id),
    permission_id       INTEGER         NOT NULL        REFERENCES permissions(permission_id)
);


CREATE TABLE IF NOT EXISTS roles (
    role_id             SERIAL          NOT NULL        PRIMARY KEY,
    name                VARCHAR(126)    NOT NULL        UNIQUE,
    display             JSON            DEFAULT NULL
)

CREATE TABLE IF NOT EXISTS roles_permissions(
    role_id             INTEGER         NOT NULL        REFERENCES roles(role_id),
    permission_id       INTEGER         NOT NULL        REFERENCES permissions(permission_id)
);

CREATE TABLE IF NOT EXISTS roles_app_roles(
    role_id             INTEGER         NOT NULL        REFERENCES roles(role_id),
    app_role_id         INTEGER         NOT NULL        REFERENCES application_roles(app_role_id)
);

CREATE TABLE IF NOT EXISTS user_role (
    user_id             INTEGER         NOT NULL    REFERENCES users(user_id),
    role_id             INTEGER         NOT NULL    REFERENCES roles(role_id)

);





CREATE TABLE IF NOT EXISTS login_attempts (
    username            VARCHAR(126)    NOT NULL    REFERENCES users(username),
    login_time          TIMESTAMPTZ     NOT NULL,
    login_addr          VARCHAR(126)    NOT NULL,
    instance_name       VARCHAR(126)    NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id          VARCHAR(126)    NOT NULL    PRIMARY KEY,
    start_time          TIMESTAMPTZ     NOT NULL,
    expiry_time         TIMESTAMPTZ     NOT NULL,
    username            VARCHAR(126)    NOT NULL    REFERENCES users(username),
    session_data        JSON
);
