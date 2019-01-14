-- Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     Ports version 0.7.0 to version 0.7.1
-- URL:         -
--
-- Authors:     Mike Robins
-- Copyright:   Copyright (c) 2018 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

BEGIN TRANSACTION;

ALTER TABLE "atomic"."events"
    ALTER COLUMN geo_region TYPE char(3);

COMMENT ON TABLE "atomic"."events" IS '0.7.1';

END TRANSACTION;
