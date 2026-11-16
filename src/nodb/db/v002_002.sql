
CREATE TABLE IF NOT EXISTS wis_incoming_message (

    message_uuid    UUID            NOT NULL    PRIMARY KEY     DEFAULT gen_random_uuid(),

    wis_version     VARCHAR(64)     NOT NULL,
    message_id      VARCHAR(1024)   NOT NULL,
    path            TEXT            NOT NULL,
    hash            VARCHAR(1024)   NOT NULL,
    received_date   DATE            NOT NULL,
    bundled         INTEGER         NOT NULL    DEFAULT 0,
    source_file_uuid    UUID        DEFAULT NULL

);

CREATE INDEX IF NOT EXISTS idx_wis_incoming_message_hash ON wis_incoming_message(hash);

COMMENT ON TABLE wis_incoming_message IS "Stores incoming messages from the WIS";
COMMENT ON COLUMN wis_incoming_message.message_uuid IS "A unique message identifier";
COMMENT ON COLUMN wis_incoming_message.wis_version IS "The WIS version this message was received from";
COMMENT ON COLUMN wis_incoming_message.message_id IS "A unique identifier from the WIS assigned to this message";
COMMENT ON COLUMN wis_incoming_message.path IS "The path where this message's content is stored";
COMMENT ON COLUMN wis_incoming_message.hash IS "A hash of the message's content";
COMMENT ON COLUMN wis_incoming_message.received_date IS "The date the message was received";
COMMENT ON COLUMN wis_incoming_message.bundled IS "Whether the message has been bundled to the old system"
COMMENT ON COLUMN wis_incoming_message.source_file_uuid IS "The source file created from this file";
