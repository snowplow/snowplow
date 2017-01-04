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

DROP TABLE IF EXISTS {{.scratch_schema}}.user_mapping;
CREATE TABLE {{.scratch_schema}}.user_mapping

AS (

	SELECT

  	a.domain_userid,

	  a.user_id,

    b.cnt

	FROM {{.input_schema}}.events as a

  LEFT JOIN {{.scratch_schema}}.user_mapping_cnt as b
    ON a.domain_userid = b.domain_userid

	WHERE a.domain_userid IS NOT NULL

	AND a.user_id IS NOT NULL

  AND b.cnt <= 1

	GROUP BY 1,2,3

);
