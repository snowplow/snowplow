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

BEGIN;

  DROP TABLE IF EXISTS web_user_identity_stitching.page_views;
  ALTER TABLE web_user_identity_stitching.page_views_tmp RENAME TO page_views;

COMMIT;

BEGIN;

  DROP TABLE IF EXISTS web_user_identity_stitching.sessions;
  ALTER TABLE web_user_identity_stitching.sessions_tmp RENAME TO sessions;

COMMIT;

BEGIN;

  DROP TABLE IF EXISTS web_user_identity_stitching.users;
  ALTER TABLE web_user_identity_stitching.users_tmp RENAME TO users;
  ALTER TABLE web_user_identity_stitching.users_stich_tmp RENAME TO users_stich;

COMMIT;

DROP TABLE IF EXISTS scratch_user_identity_stitching.web_page_context;
DROP TABLE IF EXISTS scratch_user_identity_stitching.web_events;
DROP TABLE IF EXISTS scratch_user_identity_stitching.web_events_time;
DROP TABLE IF EXISTS scratch_user_identity_stitching.web_events_scroll_depth;
DROP TABLE IF EXISTS scratch_user_identity_stitching.web_ua_parser_context;
DROP TABLE IF EXISTS scratch_user_identity_stitching.web_timing_context;
DROP TABLE IF EXISTS scratch_user_identity_stitching.user_mapping_cnt;
DROP TABLE IF EXISTS scratch_user_identity_stitching.user_mapping;
DROP TABLE IF EXISTS scratch_user_identity_stitching.users_rank;
