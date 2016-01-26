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
-- Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0

-- Maps domain_userid to user_id. In case a domain_userid has multiple user_ids, we use the last record

DROP TABLE IF EXISTS snowplow_intermediary.cookie_id_to_user_id_map;

CREATE TABLE snowplow_intermediary.cookie_id_to_user_id_map 
DISTKEY (domain_userid)
SORTKEY (domain_userid)
AS (
  SELECT domain_userid, user_id as inferred_user_id FROM
    (SELECT
      collector_tstamp,
      domain_userid,
      user_id,
      RANK() OVER (PARTITION BY domain_userid ORDER BY collector_tstamp DESC) AS login_sequence
    FROM atomic.events
    WHERE domain_userid IS NOT NULL
    GROUP BY 1,2,3)
  WHERE login_sequence = 1
  GROUP BY 1,2
);
