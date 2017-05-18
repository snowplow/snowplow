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
-- Sessions:
-- (a) aggregate events into sessions
-- (b) select geo
-- (c) select landing page
-- (d) select exit page
-- (e) select source
-- (f) select tech
-- (g) combine in a single table

CREATE TABLE snplw_temp.sessions
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, min_dvce_created_tstamp)
AS (

WITH basic AS (

  -- (a) aggregate events into sessions

  SELECT

    blended_user_id,
    inferred_user_id,
    domain_userid,
    domain_sessionidx,

    MIN(collector_tstamp) AS session_start_tstamp,
    MAX(collector_tstamp) AS session_end_tstamp,
    MIN(dvce_created_tstamp) AS min_dvce_created_tstamp, -- used to replace SQL window functions
    MAX(dvce_created_tstamp) AS max_dvce_created_tstamp, -- used to replace SQL window functions
    MAX(etl_tstamp) AS max_etl_tstamp, -- for debugging
    COUNT(*) AS event_count,
    COUNT(DISTINCT(FLOOR(EXTRACT(EPOCH FROM dvce_created_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes

  FROM snplw_temp.enriched_events

  GROUP BY 1,2,3,4
  ORDER BY 1,2,3,4

), geo AS (

  -- (b) select geo

  SELECT -- enrich the data

    c.domain_userid,
    c.domain_sessionidx,

    d.name AS geo_country,
    c.geo_country AS geo_country_code_2_characters,
    d.three_letter_iso_code AS geo_country_code_3_characters,
    c.geo_region,
    c.geo_city,
    c.geo_zipcode,
    c.geo_latitude,
    c.geo_longitude

  FROM (
    SELECT -- select the first value for each column

      a.domain_userid,
      a.domain_sessionidx,

      a.geo_country,
      a.geo_region,
      a.geo_city,
      a.geo_zipcode,
      a.geo_latitude,
      a.geo_longitude,

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx) AS row_number

    FROM snplw_temp.enriched_events AS a

    INNER JOIN basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_created_tstamp = b.min_dvce_created_tstamp -- replaces the FIRST VALUE window function in SQL

    ORDER BY 1,2

  ) AS c

  LEFT JOIN reference_data.country_codes AS d
    ON c.geo_country = d.two_letter_iso_code

  WHERE c.row_number = 1 -- deduplicate

), landing_page AS (

  -- (c) select landing page

  SELECT * FROM (
    SELECT

      a.domain_userid,
      a.domain_sessionidx,

      a.page_urlhost,
      a.page_urlpath,

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx) AS row_number

    FROM snplw_temp.enriched_events AS a

    INNER JOIN basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_created_tstamp = b.min_dvce_created_tstamp

    ORDER BY 1,2
  )
  WHERE row_number = 1 -- deduplicate

), exit_page AS (

  -- (d) select exit page

  SELECT * FROM (
    SELECT
      a.domain_userid,
      a.domain_sessionidx,

      a.page_urlhost,
      a.page_urlpath,

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx) AS row_number

    FROM snplw_temp.enriched_events AS a

    INNER JOIN basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_created_tstamp = b.max_dvce_created_tstamp

    ORDER BY 1,2
  )
  WHERE row_number = 1 -- deduplicate

), source AS (

  -- (e) select source

  SELECT * FROM (
    SELECT
      a.domain_userid,
      a.domain_sessionidx,

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

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx) AS row_number

    FROM snplw_temp.enriched_events AS a

    INNER JOIN basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_created_tstamp = b.min_dvce_created_tstamp

    ORDER BY 1,2
  )
  WHERE row_number = 1 -- deduplicate

), tech AS (

  -- (f) select tech

  SELECT * FROM (
    SELECT

      a.domain_userid,
      a.domain_sessionidx,

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

    FROM snplw_temp.enriched_events AS a

    INNER JOIN basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_created_tstamp = b.min_dvce_created_tstamp

    ORDER BY 1,2
  )
  WHERE row_number = 1 -- deduplicate

)

-- (g) combine in a single table

SELECT

  b.blended_user_id,
  b.inferred_user_id,
  b.domain_userid,
  b.domain_sessionidx,

  b.session_start_tstamp,
  b.session_end_tstamp,
  b.min_dvce_created_tstamp,
  b.max_dvce_created_tstamp,
  b.max_etl_tstamp, -- for debugging
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

FROM basic AS b

LEFT JOIN geo AS g ON b.domain_userid = g.domain_userid AND b.domain_sessionidx = g.domain_sessionidx
LEFT JOIN landing_page AS l ON b.domain_userid = l.domain_userid AND b.domain_sessionidx = l.domain_sessionidx
LEFT JOIN exit_page AS e ON b.domain_userid = e.domain_userid AND b.domain_sessionidx = e.domain_sessionidx
LEFT JOIN source AS s ON b.domain_userid = s.domain_userid AND b.domain_sessionidx = s.domain_sessionidx
LEFT JOIN tech AS t ON b.domain_userid = t.domain_userid AND b.domain_sessionidx = t.domain_sessionidx

);

INSERT INTO snplw_temp.queries (SELECT 'sessions', 'new', GETDATE()); -- track time
