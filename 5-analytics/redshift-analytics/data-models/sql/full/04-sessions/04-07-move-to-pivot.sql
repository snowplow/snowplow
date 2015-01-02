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

-- Move the consolidated sessions to the pivot table.

DROP TABLE IF EXISTS snowplow_pivots.sessions;
CREATE TABLE snowplow_pivots.sessions
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx)
AS (
  SELECT
    b.blended_user_id, -- Placeholder (equals domain_userid)
    b.inferred_user_id, -- Placeholder (equals NULL)
    b.domain_userid,
    b.domain_sessionidx,
    b.session_start_tstamp,
    b.session_end_tstamp,
    b.dvce_min_tstamp, -- Used to join with older sessions
    b.dvce_max_tstamp, -- Used to join with older sessions
    b.max_etl_tstamp, -- Used for debugging
    b.event_count,
    b.time_engaged_with_minutes,
    g.geo_country,
    g.geo_country_code_2_characters,
    g.geo_country_code_3_characters,
    g.geo_region,
    g.geo_city,
    g.geo_zipcode,
    g.geo_latitude,
    g.geo_longitude,
    l.page_urlhost AS landing_page_host,
    l.page_urlpath AS landing_page_path,
    e.page_urlhost AS exit_page_host,
    e.page_urlpath AS exit_page_path,
    s.mkt_source,
    s.mkt_medium,
    s.mkt_term,
    s.mkt_content,
    s.mkt_campaign,
    s.refr_source,
    s.refr_medium,
    s.refr_term,
    s.refr_urlhost,
    s.refr_urlpath,
    t.br_name,
    t.br_family,
    t.br_version,
    t.br_type,
    t.br_renderengine,
    t.br_lang,
    t.br_features_director,
    t.br_features_flash,
    t.br_features_gears,
    t.br_features_java,
    t.br_features_pdf,
    t.br_features_quicktime,
    t.br_features_realplayer,
    t.br_features_silverlight,
    t.br_features_windowsmedia,
    t.br_cookies,
    t.os_name,
    t.os_family,
    t.os_manufacturer,
    t.os_timezone,
    t.dvce_type,
    t.dvce_ismobile,
    t.dvce_screenwidth,
    t.dvce_screenheight
  FROM      snowplow_intermediary.sessions_basic           AS b
  LEFT JOIN snowplow_intermediary.sessions_geo             AS g ON b.domain_userid = g.domain_userid AND b.domain_sessionidx = g.domain_sessionidx
  LEFT JOIN snowplow_intermediary.sessions_landing_page    AS l ON b.domain_userid = l.domain_userid AND b.domain_sessionidx = l.domain_sessionidx
  LEFT JOIN snowplow_intermediary.sessions_exit_page       AS e ON b.domain_userid = e.domain_userid AND b.domain_sessionidx = e.domain_sessionidx
  LEFT JOIN snowplow_intermediary.sessions_source          AS s ON b.domain_userid = s.domain_userid AND b.domain_sessionidx = s.domain_sessionidx
  LEFT JOIN snowplow_intermediary.sessions_technology      AS t ON b.domain_userid = t.domain_userid AND b.domain_sessionidx = t.domain_sessionidx
);

