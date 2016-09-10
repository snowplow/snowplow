-- Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     Ports 1-0-0 to 1-0-1
-- URL:         -
--
-- Authors:     @digitaltouch, Alex Dean
-- Copyright:   Copyright (c) 2016 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

BEGIN TRANSACTION;

ALTER TABLE atomic.com_callrail_call_complete_1 RENAME TO com_callrail_call_complete_1_old;

--Create new table with support for schema 1-0-1
CREATE TABLE atomic.com_callrail_call_complete_1 (
    -- Schema of this type
    schema_vendor   varchar(128)   encode runlength not null,
    schema_name     varchar(128)   encode runlength not null,
    schema_format   varchar(128)   encode runlength not null,
    schema_version  varchar(128)   encode runlength not null,
	-- Parentage of this type
	root_id         char(36)       encode raw not null,
	root_tstamp     timestamp      encode raw not null,
	ref_root        varchar(255)   encode runlength not null,
	ref_tree        varchar(1500)  encode runlength not null,
	ref_parent      varchar(255)   encode runlength not null,
	-- Properties of this type
	answered        boolean        encode runlength,
    callercity      varchar(255)   encode text32k,
	callercountry   varchar(255)   encode runlength,
	callername      varchar(255)   encode raw,
	callernum       varchar(255)   encode raw,
	callerstate     varchar(255)   encode text255,
	callerzip       varchar(255)   encode text32k, 
	callsource      varchar(255)   encode runlength,
	datetime        timestamp      encode raw not null,
	destinationnum  varchar(255)   encode raw,
	duration        integer        encode raw,
	first_call      boolean        encode runlength,
	device_type     varchar(255)   encode runlenght,
	ga              varchar(255)   encode runlength,
	gclid           varchar(255)   encode runlength, 
	id              varchar(255)   encode raw not null,
	ip              varchar(45)    encode raw,
	keywords        varchar(255)   encode runlength,
	kissmetrics_id  varchar(255)   encode runlength,
	landingpage     varchar(4096)  encode text32k,
	recording       varchar(4096)  encode raw,
	referrer        varchar(255)   encode runlength,
	referrermedium  varchar(255)   encode runlength,
	trackingnum     varchar(255)   encode raw,
	transcription   varchar(28000) encode raw,
	utm_campaign    varchar(255)   encode runlength,
	utm_content     varchar(255)   encode runlength,
	utm_medium      varchar(255)   encode runlength,
	utm_source      varchar(255)   encode runlength,
	utm_term        varchar(255)   encode runlength,
	utma            varchar(255)   encode runlength,
	utmb            varchar(255)   encode runlength,
	utmc            varchar(255)   encode runlength,
	utmv            varchar(255)   encode runlength,
	utmx            varchar(255)   encode runlength,
	utmz            varchar(255)   encode runlength,
	FOREIGN KEY(root_id) REFERENCES atomic.events(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);

-- Now copy into new from atomic.com_callrail_call_complete_1_old setting device_type to NULL
INSERT INTO atomic.com_callrail_call_complete_1 
	SELECT schema_vendor, schema_name, schema_format, schema_version, root_id, root_tstamp, ref_root, ref_tree, ref_parent, answered, callercity
       , callercountry, callername, callernum, callerstate, callerzip, callsource, datetime, destinationnum, duration, first_call, NULL, ga, gclid
       , id, ip, keywords, kissmetrics_id, landingpage, recording, referrer, referrermedium, trackingnum, transcription, utm_campaign, utm_content
       , utm_medium, utm_source, utm_term, utma, utmb, utmc, utmv, utmx, utmz
FROM atomic.com_callrail_call_complete_1_old;

-- OPTIONAL: Delete atomic.com_callrail_call_complete_1_old
--DROP TABLE atomic.com_callrail_call_complete_1_old;

END TRANSACTION;
