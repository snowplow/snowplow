-- Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
-- Version:     Ports version 0.0.7 to version 0.0.8
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean
-- Copyright:   Copyright (c) 2013 SnowPlow Analytics Ltd
-- License:     Apache License Version 2.0

-- Note: Infobright does not support MySQL-like ALTER TABLE statements
-- Approach below follows recommended best practice for ICE:
-- http://www.infobright.org/images/uploads/blogs/how-to/How_To_ALTER_TABLE_in_Infobright.pdf

USE snowplow ;

SELECT *
FROM events_007 INTO OUTFILE '/tmp/events_008'
FIELDS TERMINATED BY '|';

LOAD DATA INFILE '/tmp/events_008' INTO TABLE events_008
FIELDS TERMINATED BY '|';