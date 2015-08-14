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

CREATE TABLE IF NOT EXISTS duplicates.queries (

  min_tstamp timestamp encode lzo,
  max_tstamp timestamp encode lzo,
  component varchar(255) encode lzo,
  step varchar(255) encode lzo,
  tstamp timestamp encode lzo,
  duration int encode lzo
)
DISTSTYLE KEY
DISTKEY (min_tstamp)
SORTKEY (min_tstamp, tstamp);
