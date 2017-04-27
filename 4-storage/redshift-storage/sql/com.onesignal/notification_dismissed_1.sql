-- AUTO-GENERATED BY schema-ddl DO NOT EDIT
-- Generator: schema-ddl 0.3.1
-- Generated: 2016-11-11 11:22

CREATE SCHEMA IF NOT EXISTS atomic;

CREATE TABLE IF NOT EXISTS atomic.com_onesignal_notification_dismissed_1 (
    "schema_vendor"    VARCHAR(128)  ENCODE RUNLENGTH NOT NULL,
    "schema_name"      VARCHAR(128)  ENCODE RUNLENGTH NOT NULL,
    "schema_format"    VARCHAR(128)  ENCODE RUNLENGTH NOT NULL,
    "schema_version"   VARCHAR(128)  ENCODE RUNLENGTH NOT NULL,
    "root_id"          CHAR(36)      ENCODE RAW       NOT NULL,
    "root_tstamp"      TIMESTAMP     ENCODE LZO       NOT NULL,
    "ref_root"         VARCHAR(255)  ENCODE RUNLENGTH NOT NULL,
    "ref_tree"         VARCHAR(1500) ENCODE RUNLENGTH NOT NULL,
    "ref_parent"       VARCHAR(255)  ENCODE RUNLENGTH NOT NULL,
    "event"            VARCHAR(4096) ENCODE LZO,
    "id"               VARCHAR(4096) ENCODE LZO,
    "userId"           VARCHAR(4096) ENCODE LZO,
    "url"              VARCHAR(4096) ENCODE LZO,
    "heading"          VARCHAR(4096) ENCODE LZO,
    "content"          VARCHAR(4096) ENCODE LZO,
    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
DISTKEY (root_id)
SORTKEY (root_tstamp);

COMMENT ON TABLE atomic.com_onesignal_notification_dismissed_1 IS 'iglu:com.onesignal/notification_dismissed/jsonschema/1-0-0';