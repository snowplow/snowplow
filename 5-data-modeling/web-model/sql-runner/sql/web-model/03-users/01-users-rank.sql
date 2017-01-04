-- Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
--
-- Authors:     Christophe Bogaert
-- Copyright:   Copyright (c) 2016 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0


DROP TABLE IF EXISTS {{.scratch_schema}}.users_rank;
CREATE TABLE {{.scratch_schema}}.users_rank
  DISTKEY(user_snowplow_domain_id)
  SORTKEY(first_session_start)
AS (

WITH prep AS (

SELECT

  *,

  RANK (
)
OVER (PARTITION BY user_custom_id
ORDER BY first_session_start
) AS rank

FROM {{.output_schema}}.users_tmp

)
SELECT *

FROM prep

WHERE rank =1
);
