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

-- Create the snowplow_intermediary.cookie_id_to_user_id_map table (used for identity stitching):
CREATE TABLE IF NOT EXISTS snowplow_intermediary.cookie_id_to_user_id_map (
  domain_userid varchar(36),
  inferred_user_id varchar(255) encode runlength -- Most identitiy stitching models will infer the user_id
)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (domain_userid);
