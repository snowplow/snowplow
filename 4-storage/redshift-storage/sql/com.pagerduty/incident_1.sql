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
-- Compatibility: iglu:com.pagerduty/incident/jsonschema/1-0-0

CREATE TABLE atomic.com_pagerduty_incident_1 (
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
    id                                               varchar(255)   encode lzo,
    created_on                                       timestamp      encode lzo,
    type                                             varchar(255)   encode lzo,
    "data.incident.id"                               varchar(255)   encode lzo,
    "data.incident.incident_number"                  varchar(255)   encode lzo,
    "data.incident.created_on"                       timestamp      encode lzo,
    "data.incident.status"                           varchar(255)   encode lzo,
    "data.incident.html_url"                         varchar(255)   encode lzo,
    "data.incident.incident_key"                     varchar(255)   encode lzo,
    "data.incident.service.id"                       varchar(255)   encode lzo,
    "data.incident.service.name"                     varchar(255)   encode lzo,
    "data.incident.service.html_url"                 varchar(255)   encode lzo,
    "data.incident.service.deleted_at"               varchar(255)   encode lzo,
    "data.incident.escalation_policyid"              varchar(255)   encode lzo,
    "data.incident.escalation_policyname"            varchar(255)   encode lzo,
    "data.incident.escalation_policydeleted_at"      varchar(255)   encode lzo,
    "data.incident.assigned_to_user.id"              varchar(255)   encode lzo,
    "data.incident.assigned_to_user.name"            varchar(255)   encode lzo,
    "data.incident.assigned_to_user.email"           varchar(255)   encode lzo,
    "data.incident.assigned_to_user.html_url"        varchar(255)   encode lzo,
    "data.incident.trigger_summary_data.description" varchar(255)   encode lzo,
    "data.incident.trigger_summary_data.subject"     varchar(255)   encode lzo,
    "data.incident.trigger_summary_data.client"      varchar(255)   encode lzo,
    "data.incident.trigger_details_html_url"         varchar(255)   encode lzo,
    "data.incident.trigger_type"                     varchar(255)   encode lzo,
    "data.incident.last_status_change_on"            timestamp      encode lzo,
    "data.incident.last_status_change_by.id"         varchar(255)   encode lzo,
    "data.incident.last_status_change_by.name"       varchar(255)   encode lzo,
    "data.incident.last_status_change_by.email"      varchar(255)   encode lzo,
    "data.incident.last_status_change_by.html_url"   varchar(255)   encode lzo,
    "data.incident.number_of_escalations"            varchar(255)   encode lzo,
    "data.incident.assigned_to"                      varchar(2048)  encode lzo, -- Holds a JSON array
    "data.incident.resolved_by_user.id"              varchar(255)   encode lzo,
    "data.incident.resolved_by_user.name"            varchar(255)   encode lzo,
    "data.incident.resolved_by_user.email"           varchar(255)   encode lzo,
    "data.incident.resolved_by_user.html_url"        varchar(255)   encode lzo,
    FOREIGN KEY(root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
