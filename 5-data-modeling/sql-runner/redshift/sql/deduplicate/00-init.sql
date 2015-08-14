-- Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Authors: Christophe Bogaert
-- Copyright: Copyright (c) 2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: deduplicate
-- Version: 0.2

DROP TABLE IF EXISTS duplicates.tmp_ids_1;
DROP TABLE IF EXISTS duplicates.tmp_ids_2;
DROP TABLE IF EXISTS duplicates.tmp_events;
DROP TABLE IF EXISTS duplicates.tmp_queries;

CREATE TABLE duplicates.tmp_queries ( -- create table to track performance
  component varchar(255) encode lzo,
  step varchar(255) encode lzo,
  tstamp timestamp
)
DISTSTYLE KEY
DISTKEY (component)
SORTKEY (tstamp);
