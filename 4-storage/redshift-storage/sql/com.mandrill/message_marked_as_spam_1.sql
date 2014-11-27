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
-- Authors:     Joshua Beemster
-- Copyright:   Copyright (c) 2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0
-- 
-- Compatibility: iglu:com.mandrill/message_marked_as_spam/jsonschema/1-0-0

CREATE TABLE atomic.com_mandrill_message_marked_as_spam_1 (
    -- Schema of this type
    schema_vendor  varchar(128)   encode runlength not null,
    schema_name    varchar(128)   encode runlength not null,
    schema_format  varchar(128)   encode runlength not null,
    schema_version varchar(128)   encode runlength not null,
    -- Parentage of this type
    root_id        char(36)       encode raw not null,
    root_tstamp    timestamp      encode raw not null,
    ref_root       varchar(255)   encode runlength not null,
    ref_tree       varchar(1500)  encode runlength not null,
    ref_parent     varchar(255)   encode runlength not null,
    -- Properties of this type
    _id                    varchar(255)   encode raw,
    ts                     timestamp      encode raw,
    "msg._id"              varchar(255)   encode raw,
    "msg._version"         varchar(255)   encode raw,
    "msg.clicks"           varchar(2048)  encode runlength, -- Holds a JSON array
    "msg.email"            varchar(255)   encode raw,
    "msg.metadata.user_id" varchar(255)   encode raw,
    "msg.opens"            varchar(2048)  encode runlength, -- Holds a JSON array
    "msg.sender"           varchar(255)   encode raw,
    "msg.state"            varchar(255)   encode raw,
    "msg.subject"          varchar(255)   encode raw,
    "msg.tags"             varchar(2048)  encode runlength, -- Holds a JSON array
    "msg.ts"               timestamp      encode raw
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
