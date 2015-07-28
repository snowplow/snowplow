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
-- Authors:       Alex Dean
-- Copyright:     Copyright (c) 2014-2015 Snowplow Analytics Ltd
-- License:       Apache License Version 2.0
--
-- Compatibility: iglu:org.w3/PerformanceTiming/jsonschema/1-0-0

CREATE TABLE atomic.org_w3_performance_timing_1 (
	-- Schema of this type
	schema_vendor                  varchar(128)  encode runlength not null,
	schema_name                    varchar(128)  encode runlength not null,
	schema_format                  varchar(128)  encode runlength not null,
	schema_version                 varchar(128)  encode runlength not null,
	-- Parentage of this type
	root_id                        char(36)      encode raw not null,
	root_tstamp                    timestamp     encode lzo not null,
	ref_root                       varchar(255)  encode runlength not null,
	ref_tree                       varchar(1500) encode runlength not null,
	ref_parent                     varchar(255)  encode runlength not null,
	-- Properties of this type
	navigation_start               bigint encode lzo,
	redirect_start                 bigint encode lzo,
	redirect_end                   bigint encode lzo,
	fetch_start                    bigint encode lzo,
	domain_lookup_start            bigint encode lzo,
	domain_lookup_end              bigint encode lzo,
	secure_connection_start        bigint encode lzo,
	connect_start                  bigint encode lzo,
	connect_end                    bigint encode lzo,
	request_start                  bigint encode lzo,
	response_start                 bigint encode lzo,
	response_end                   bigint encode lzo,
	unload_event_start             bigint encode lzo,
	unload_event_end               bigint encode lzo,
	dom_loading                    bigint encode lzo,
	dom_interactive                bigint encode lzo,
	dom_content_loaded_event_start bigint encode lzo,
	dom_content_loaded_event_end   bigint encode lzo,
	dom_complete                   bigint encode lzo,
	load_event_start               bigint encode lzo,
	load_event_end                 bigint encode lzo,
	ms_first_paint                 bigint encode lzo,
	chrome_first_paint             bigint encode lzo,
	FOREIGN KEY(root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
