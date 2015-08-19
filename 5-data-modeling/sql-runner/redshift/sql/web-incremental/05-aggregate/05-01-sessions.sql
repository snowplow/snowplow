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
--
-- Data Model: web-incremental
-- Version: 2.0
--
-- Aggregate new and old rows:
-- (a) calculate aggregate frame (i.e. a GROUP BY)
-- (b) calculate initial frame (i.e. first value)
-- (c) calculate final frame (i.e. last value)
-- (d) combine

CREATE TABLE snplw_temp.sessions_aggregated
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, min_dvce_tstamp)
AS (

WITH aggregate_frame AS (

  -- (a) calculate aggregate frame (i.e. a GROUP BY)

  SELECT

    domain_userid,
    domain_sessionidx,

    MIN(session_start_tstamp) AS session_start_tstamp,
    MAX(session_end_tstamp) AS session_end_tstamp,
    MIN(min_dvce_tstamp) AS min_dvce_tstamp,
    MAX(max_dvce_tstamp) AS max_dvce_tstamp,
    MAX(max_etl_tstamp) AS max_etl_tstamp,
    SUM(event_count) AS event_count,
    SUM(time_engaged_with_minutes) AS time_engaged_with_minutes

  FROM snplw_temp.sessions

  GROUP BY 1,2
  ORDER BY 1,2

), initial_frame AS (

  -- (b) calculate initial frame (i.e. first value)

  SELECT * FROM (
    SELECT

      a.domain_userid,
      a.domain_sessionidx,

      a.geo_country,
      a.geo_country_code_2_characters,
      a.geo_country_code_3_characters,
      a.geo_region,
      a.geo_city,
      a.geo_zipcode,
      a.geo_latitude,
      a.geo_longitude,

      a.landing_page_host,
      a.landing_page_path,

      a.mkt_source,
      a.mkt_medium,
      a.mkt_term,
      a.mkt_content,
      a.mkt_campaign,

      a.refr_source,
      a.refr_medium,
      a.refr_term,
      a.refr_urlhost,
      a.refr_urlpath,

      a.br_name,
      a.br_family,
      a.br_version,
      a.br_type,
      a.br_renderengine,
      a.br_lang,
      a.br_features_director,
      a.br_features_flash,
      a.br_features_gears,
      a.br_features_java,
      a.br_features_pdf,
      a.br_features_quicktime,
      a.br_features_realplayer,
      a.br_features_silverlight,
      a.br_features_windowsmedia,
      a.br_cookies,
      a.os_name,
      a.os_family,
      a.os_manufacturer,
      a.os_timezone,
      a.dvce_type,
      a.dvce_ismobile,
      a.dvce_screenwidth,
      a.dvce_screenheight,

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx) AS row_number

    FROM snplw_temp.sessions AS a

    INNER JOIN aggregate_frame AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.min_dvce_tstamp = b.min_dvce_tstamp

    ORDER BY 1,2

  )
  WHERE row_number = 1 -- deduplicate

), final_frame AS (

  -- (c) calculate final frame (i.e. last value)

  SELECT * FROM (
    SELECT

      a.domain_userid,
      a.domain_sessionidx,

      a.blended_user_id, -- edge case: one session with multiple logins and events in several batches
      a.inferred_user_id, -- edge case: one session with multiple logins and events in several batches

      a.exit_page_host,
      a.exit_page_path,

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx) AS row_number

    FROM snplw_temp.sessions AS a

    INNER JOIN aggregate_frame AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.max_dvce_tstamp = b.max_dvce_tstamp

    ORDER BY 1,2

  )
  WHERE row_number = 1 -- deduplicate

)

-- (d) combine and insert into derived

SELECT

  f.blended_user_id,
  f.inferred_user_id,

  a.domain_userid,
  a.domain_sessionidx,

  a.session_start_tstamp,
  a.session_end_tstamp,
  a.min_dvce_tstamp,
  a.max_dvce_tstamp,
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

FROM aggregate_frame AS a

LEFT JOIN initial_frame AS i
  ON  a.domain_userid = i.domain_userid
  AND a.domain_sessionidx = i.domain_sessionidx
LEFT JOIN final_frame AS f
  ON  a.domain_userid = f.domain_userid
  AND a.domain_sessionidx = f.domain_sessionidx

);

INSERT INTO snplw_temp.queries (SELECT 'sessions', 'aggregate', GETDATE()); -- track time
