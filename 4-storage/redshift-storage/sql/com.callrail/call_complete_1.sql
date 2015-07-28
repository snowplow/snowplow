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
-- Compatibility: iglu:com.callrail/call_complete/jsonschema/1-0-0

CREATE TABLE atomic.com_callrail_call_complete_1 (
    -- Schema of this type
    schema_vendor   varchar(128)   encode runlength not null,
    schema_name     varchar(128)   encode runlength not null,
    schema_format   varchar(128)   encode runlength not null,
    schema_version  varchar(128)   encode runlength not null,
	-- Parentage of this type
	root_id         char(36)       encode raw not null,
	root_tstamp     timestamp      encode lzo not null,
	ref_root        varchar(255)   encode runlength not null,
	ref_tree        varchar(1500)  encode runlength not null,
	ref_parent      varchar(255)   encode runlength not null,
	-- Properties of this type
	answered        boolean        encode runlength,
  callercity      varchar(255)   encode lzo,
	callercountry   varchar(255)   encode lzo,
	callername      varchar(255)   encode lzo,
	callernum       varchar(255)   encode lzo,
	callerstate     varchar(255)   encode lzo,
	callerzip       varchar(255)   encode lzo,
	callsource      varchar(255)   encode lzo,
	datetime        timestamp      encode lzo not null,
	destinationnum  varchar(255)   encode lzo,
	duration        integer        encode lzo,
	first_call      boolean        encode runlength,
	ga              varchar(255)   encode lzo,
	gclid           varchar(255)   encode lzo,
	id              varchar(255)   encode lzo not null,
	ip              varchar(45)    encode lzo,
	keywords        varchar(255)   encode lzo,
	kissmetrics_id  varchar(255)   encode lzo,
	landingpage     varchar(4096)  encode lzo,
	recording       varchar(4096)  encode lzo,
	referrer        varchar(255)   encode lzo,
	referrermedium  varchar(255)   encode lzo,
	trackingnum     varchar(255)   encode lzo,
	transcription   varchar(28000) encode lzo,
	utm_campaign    varchar(255)   encode lzo,
	utm_content     varchar(255)   encode lzo,
	utm_medium      varchar(255)   encode lzo,
	utm_source      varchar(255)   encode lzo,
	utm_term        varchar(255)   encode lzo,
	utma            varchar(255)   encode lzo,
	utmb            varchar(255)   encode lzo,
	utmc            varchar(255)   encode lzo,
	utmv            varchar(255)   encode lzo,
	utmx            varchar(255)   encode lzo,
	utmz            varchar(255)   encode lzo,
	FOREIGN KEY(root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
