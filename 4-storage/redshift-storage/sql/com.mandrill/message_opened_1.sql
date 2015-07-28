-- Copyright (c) 2014-2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Copyright:   Copyright (c) 2014-2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0
--
-- Compatibility: iglu:com.mandrill/message_opened/jsonschema/1-0-0

CREATE TABLE atomic.com_mandrill_message_opened_1 (
    -- Schema of this type
    schema_vendor  varchar(128)   encode runlength not null,
    schema_name    varchar(128)   encode runlength not null,
    schema_format  varchar(128)   encode runlength not null,
    schema_version varchar(128)   encode runlength not null,
    -- Parentage of this type
    root_id        char(36)       encode raw not null,
    root_tstamp    timestamp      encode lzo not null,
    ref_root       varchar(255)   encode runlength not null,
    ref_tree       varchar(1500)  encode runlength not null,
    ref_parent     varchar(255)   encode runlength not null,
    -- Properties of this type
    _id                                varchar(255)   encode lzo,
    ts                                 timestamp      encode lzo,
    ip                                 varchar(255)   encode lzo,
    "location.city"                    varchar(255)   encode lzo,
    "location.country_short"           varchar(255)   encode lzo,
    "location.country"                 varchar(255)   encode lzo,
    "location.latitude"                varchar(255)   encode lzo,
    "location.longitude"               varchar(255)   encode lzo,
    "location.postal_code"             varchar(255)   encode lzo,
    "location.region"                  varchar(255)   encode lzo,
    "location.timezone"                varchar(255)   encode lzo,
    "msg._id"                          varchar(255)   encode lzo,
    "msg._version"                     varchar(255)   encode lzo,
    "msg.clicks"                       varchar(5000)  encode lzo, -- Holds a JSON array
    "msg.email"                        varchar(255)   encode lzo,
    "msg.metadata.user_id"             varchar(255)   encode lzo,
    "msg.opens"                        varchar(5000)  encode lzo, -- Holds a JSON array
    "msg.sender"                       varchar(255)   encode lzo,
    "msg.state"                        varchar(255)   encode lzo,
    "msg.subject"                      varchar(255)   encode lzo,
    "msg.tags"                         varchar(5000)  encode lzo, -- Holds a JSON array
    "msg.ts"                           timestamp      encode lzo,
    "msg.resends"                      varchar(5000)  encode lzo, -- Holds a JSON array
    "msg.smtp_events"                  varchar(5000)  encode lzo, -- Holds a JSON array
    "msg.template"                     varchar(255)   encode lzo,
    "user_agent_parsed.mobile"         varchar(255)   encode lzo,
    "user_agent_parsed.os_company_url" varchar(255)   encode lzo,
    "user_agent_parsed.os_company"     varchar(255)   encode lzo,
    "user_agent_parsed.os_family"      varchar(255)   encode lzo,
    "user_agent_parsed.os_icon"        varchar(255)   encode lzo,
    "user_agent_parsed.os_name"        varchar(255)   encode lzo,
    "user_agent_parsed.os_url"         varchar(255)   encode lzo,
    "user_agent_parsed.type"           varchar(255)   encode lzo,
    "user_agent_parsed.ua_company_url" varchar(255)   encode lzo,
    "user_agent_parsed.ua_company"     varchar(255)   encode lzo,
    "user_agent_parsed.ua_family"      varchar(255)   encode lzo,
    "user_agent_parsed.ua_icon"        varchar(255)   encode lzo,
    "user_agent_parsed.ua_name"        varchar(255)   encode lzo,
    "user_agent_parsed.ua_url"         varchar(255)   encode lzo,
    "user_agent_parsed.ua_version"     varchar(255)   encode lzo,
    user_agent                         varchar(255)   encode lzo,
    FOREIGN KEY(root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
