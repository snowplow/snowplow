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

-- In the incremental model, new data arrives in the snowplow_landing schema and is moved to atomic after it
-- has been processed. Processed data is ready for use in a BI or pivot tool and stored in the snowplow_pivots
-- schema. The snowplow_intermediary schema is used to store data while processing.

-- Create the schemas:
CREATE SCHEMA IF NOT EXISTS snowplow_landing;
CREATE SCHEMA IF NOT EXISTS snowplow_intermediary;
CREATE SCHEMA IF NOT EXISTS snowplow_pivots;
