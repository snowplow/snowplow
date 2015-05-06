-- Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
-- 
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
-- 
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
-- 
-- Authors:     Alex Dean, Joshua Beemster
-- Copyright:     Copyright (c) 2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0
-- 
-- Compatibility: iglu:com.mailchimp/subscribe/jsonschema/1-0-0

CREATE TABLE atomic.com_mailchimp_subscribe_1 (
    -- Schema of this type
    schema_vendor           varchar(128)   encode runlength not null,
    schema_name             varchar(128)   encode runlength not null,
    schema_format           varchar(128)   encode runlength not null,
    schema_version          varchar(128)   encode runlength not null,
    -- Parentage of this type
    root_id                 char(36)       encode raw not null,
    root_tstamp             timestamp      encode raw not null,
    ref_root                varchar(255)   encode runlength not null,
    ref_tree                varchar(1500)  encode runlength not null,
    ref_parent              varchar(255)   encode runlength not null,
    -- Properties of this type
    type                    varchar(255)   encode raw,
    fired_at                timestamp      encode raw,
    "data.email"            varchar(255)   encode raw,
    "data.email_type"       varchar(255)   encode raw,
    "data.id"               varchar(255)   encode raw,
    "data.web_id"           varchar(255)   encode raw,
    "data.ip_opt"           varchar(255)   encode raw,
    "data.ip_signup"        varchar(255)   encode raw,
    "data.list_id"          varchar(255)   encode raw,
    "data.merges.EMAIL"     varchar(255)   encode raw,
    "data.merges.FNAME"     varchar(255)   encode raw,
    "data.merges.LNAME"     varchar(255)   encode raw,
    "data.merges.INTERESTS" varchar(255)   encode raw,
    FOREIGN KEY(root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
