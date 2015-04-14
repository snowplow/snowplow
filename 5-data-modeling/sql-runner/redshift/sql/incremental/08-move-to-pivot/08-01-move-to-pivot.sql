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

-- The standard model identifies sessions using only first party cookies and session domain indexes,
-- but contains placeholders for identity stitching.

-- Events belonging to the same session can arrive at different times and could end up in different batches.
-- Rows in the sessions_new table therefore have to be merged with those in the pivot table.

-- Move the consolidated sessions to the pivot table (but first delete its contents).

BEGIN;
  DELETE FROM snowplow_pivots.sessions;

  INSERT INTO snowplow_pivots.sessions (
    SELECT
      f.blended_user_id,
      f.inferred_user_id,
      a.domain_userid,
      a.domain_sessionidx,
      a.session_start_tstamp,
      a.session_end_tstamp,
      a.dvce_min_tstamp,
      a.dvce_max_tstamp,
      a.max_etl_tstamp,
      a.event_count,
      a.time_engaged_with_minutes,
      i.geo_country,
      i.geo_country_code_2_characters,
      i.geo_country_code_3_characters,
      i.geo_region,
      i.geo_city,
      i.geo_zipcode,
      i.geo_latitude,
      i.geo_longitude,
      i.landing_page_host,
      i.landing_page_path,
      f.exit_page_host,
      f.exit_page_path,
      i.mkt_source,
      i.mkt_medium,
      i.mkt_term,
      i.mkt_content,
      i.mkt_campaign,
      i.refr_source,
      i.refr_medium,
      i.refr_term,
      i.refr_urlhost,
      i.refr_urlpath,
      i.br_name,
      i.br_family,
      i.br_version,
      i.br_type,
      i.br_renderengine,
      i.br_lang,
      i.br_features_director,
      i.br_features_flash,
      i.br_features_gears,
      i.br_features_java,
      i.br_features_pdf,
      i.br_features_quicktime,
      i.br_features_realplayer,
      i.br_features_silverlight,
      i.br_features_windowsmedia,
      i.br_cookies,
      i.os_name,
      i.os_family,
      i.os_manufacturer,
      i.os_timezone,
      i.dvce_type,
      i.dvce_ismobile,
      i.dvce_screenwidth,
      i.dvce_screenheight
    FROM snowplow_intermediary.sessions_aggregate_frame AS a
    LEFT JOIN snowplow_intermediary.sessions_initial_frame AS i
      ON  a.domain_userid = i.domain_userid
      AND a.domain_sessionidx = i.domain_sessionidx
    LEFT JOIN snowplow_intermediary.sessions_final_frame AS f
      ON  a.domain_userid = f.domain_userid
      AND a.domain_sessionidx = f.domain_sessionidx
  );
COMMIT;

-- Events belonging to the same visitor can arrive at different times and could end up in different batches.
-- Rows in the visitors_new table therefore have to be merged with those in the pivot table.

-- Move the consolidated visitors to the pivot table (but first delete its contents).

BEGIN;
  DELETE FROM snowplow_pivots.visitors;

  INSERT INTO snowplow_pivots.visitors (
    SELECT
      a.blended_user_id,
      a.first_touch_tstamp,
      a.last_touch_tstamp,
      a.dvce_min_tstamp,
      a.dvce_max_tstamp,
      a.max_etl_tstamp,
      a.event_count,
      a.session_count,
      a.page_view_count,
      a.time_engaged_with_minutes,
      i.landing_page_host,
      i.landing_page_path,
      i.mkt_source,
      i.mkt_medium,
      i.mkt_term,
      i.mkt_content,
      i.mkt_campaign,
      i.refr_source,
      i.refr_medium,
      i.refr_term,
      i.refr_urlhost,
      i.refr_urlpath
    FROM snowplow_intermediary.visitors_aggregate_frame AS a
    LEFT JOIN snowplow_intermediary.visitors_initial_frame AS i
      ON a.blended_user_id = i.blended_user_id
  );
COMMIT;

-- Events belonging to the same page view can arrive at different times and could end up in different batches.
-- Rows in the page_views_new table therefore have to be merged with those in the pivot table.

-- Move the consolidated page views to the pivot table (but first delete its contents).

BEGIN;
  DELETE FROM snowplow_pivots.page_views;

  INSERT INTO snowplow_pivots.page_views (
    SELECT
      f.blended_user_id,
      f.inferred_user_id,
      a.domain_userid,
      a.domain_sessionidx,
      a.page_urlhost,
      a.page_urlpath,
      a.first_touch_tstamp,
      a.last_touch_tstamp,
      a.dvce_min_tstamp, -- Used to replace SQL window functions
      a.dvce_max_tstamp, -- Used to replace SQL window functions
      a.max_etl_tstamp, -- Used for debugging
      a.event_count,
      a.page_view_count,
      a.page_ping_count,
      a.time_engaged_with_minutes
    FROM snowplow_intermediary.page_views_aggregate_frame AS a
    LEFT JOIN snowplow_intermediary.page_views_final_frame AS f
      ON  a.domain_userid = f.domain_userid
      AND a.domain_sessionidx = f.domain_sessionidx
      AND a.page_urlhost = f.page_urlhost
      AND a.page_urlpath = f.page_urlpath
  );
COMMIT;