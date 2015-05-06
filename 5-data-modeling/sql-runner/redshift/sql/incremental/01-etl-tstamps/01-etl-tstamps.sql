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
-- Authors: Yali Sassoon, Christophe Bogaert
-- Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0

-- It could happen that new data arrives in snowplow_landing after the run has started, but before the existing
-- data has been moved to atomic. New data will have a different etl_tstamp. The etl_tstamps at the start of the
-- run are therefore stored to restrict future queries. Note: DISTINCT etl_tstamp doesn't include NULL.

DROP TABLE IF EXISTS snowplow_intermediary.etl_tstamps;
CREATE TABLE snowplow_intermediary.etl_tstamps
  DISTKEY (etl_tstamp)
  SORTKEY (etl_tstamp)
AS (
  SELECT
    etl_tstamp,
    count(*)
  FROM snowplow_landing.events
  GROUP BY 1
  ORDER BY 1
);
