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
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Alex Dean
-- Copyright:   Copyright (c) 2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE TABLE atomic.com_snowplowanalytics_ad_impression (
    -- Nature of this type
    type_name     varchar(128)  encode runlength not null,
    type_vendor   varchar(128)  encode runlength not null,
    -- Parentage of this type
    root_id       char(36)      encode raw not null,
    root_tstamp   timestamp     encode raw not null,
    ref_root      varchar(255)  encode runlength not null,
    ref_tree      varchar(1500) encode runlength not null,
    ref_parent    varchar(255)  encode runlength not null,
    -- Properties of this type
    impression_id varchar(255)  encode raw,
    zone_id       varchar(255)  encode raw,
    banner_id     varchar(255)  encode raw,
    campaign_id   varchar(255)  encode runlength,
    advertiser_id varchar(255)  encode runlength,
    cost_model    char(3)       encode runlength,
    cost_if_cpm   decimal       encode runlength
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);

CREATE TABLE atomic.com_snowplowanalytics_ad_click (
    -- Nature of this type
    type_name     varchar(128)   encode runlength not null,
    type_vendor   varchar(128)   encode runlength not null,
    -- Parentage of this type
    root_id       char(36)       encode raw not null,
    root_tstamp   timestamp      encode raw not null,
    ref_root      varchar(255)   encode runlength not null,
    ref_tree      varchar(1500)  encode runlength not null,
    ref_parent    varchar(255)   encode runlength not null,
    -- Properties of this type
    click_id      varchar(255)   encode raw,
    impression_id varchar(255)   encode raw,
    zone_id       varchar(255)   encode raw,
    banner_id     varchar(255)   encode raw,
    campaign_id   varchar(255)   encode runlength,
    advertiser_id varchar(255)   encode runlength,
    target_url    varchar(4096)  encode runlength,
    cost_model    char(3)        encode runlength,
    cost_if_cpm   decimal(15,10) encode runlength
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);

CREATE TABLE atomic.com_snowplowanalytics_ad_conversion (
    -- Nature of this type
    type_name     varchar(128)   encode runlength not null,
    type_vendor   varchar(128)   encode runlength not null,
    -- Parentage of this type
    root_id       char(36)       encode raw not null,
    root_tstamp   timestamp      encode raw not null,
    ref_root      varchar(255)   encode runlength not null,
    ref_tree      varchar(1500)  encode runlength not null,
    ref_parent    varchar(255)   encode runlength not null,
    -- Properties of this type
    conversion_id varchar(255)   encode raw,
    banner_id     varchar(255)   encode raw,
    campaign_id   varchar(255)   encode runlength,
    category      varchar(255)   encode runlength,
    action        varchar(255)   encode runlength,
    property      varchar(255)   encode runlength,
    cost_model    char(3)        encode runlength,
    cost_if_cpa   decimal(15,10) encode runlength,
    initial_value decimal(15,10) encode runlength
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (root_id)
SORTKEY (root_tstamp);
