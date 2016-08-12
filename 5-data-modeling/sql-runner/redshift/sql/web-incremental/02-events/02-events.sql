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
-- Enrich events:
-- (a) select the columns we need and deduplicate events
-- (b) deduplicate an example unstructured event
-- (c) deduplicate an example context
-- (d) combine all tables

CREATE TABLE snplw_temp.enriched_events
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, dvce_created_tstamp)
AS (

WITH events AS (

  -- (a) select the columns we need and deduplicate events

  SELECT * FROM (
    SELECT

      COALESCE(u.inferred_user_id, e.domain_userid) AS blended_user_id,
      u.inferred_user_id,

      e.domain_userid,
      e.domain_sessionidx,

      e.event_id, -- for deduplication
      e.event, -- for filtering

      e.collector_tstamp,
      e.dvce_created_tstamp,
      e.etl_tstamp, -- for debugging

      e.geo_country,
      e.geo_region,
      e.geo_city,
      e.geo_zipcode,
      e.geo_latitude,
      e.geo_longitude,

      e.page_urlhost,
      e.page_urlpath,

      e.mkt_source,
      e.mkt_medium,
      e.mkt_term,
      e.mkt_content,
      e.mkt_campaign,
      e.refr_source,
      e.refr_medium,
      e.refr_term,
      e.refr_urlhost,
      e.refr_urlpath,

      e.br_name,
      e.br_family,
      e.br_version,
      e.br_type,
      e.br_renderengine,
      e.br_lang,
      e.br_features_director,
      e.br_features_flash,
      e.br_features_gears,
      e.br_features_java,
      e.br_features_pdf,
      e.br_features_quicktime,
      e.br_features_realplayer,
      e.br_features_silverlight,
      e.br_features_windowsmedia,
      e.br_cookies,
      e.os_name,
      e.os_family,
      e.os_manufacturer,
      e.os_timezone,
      e.dvce_type,
      e.dvce_ismobile,
      e.dvce_screenwidth,
      e.dvce_screenheight,

      ROW_NUMBER() OVER (PARTITION BY event_id) as event_number -- select one event at random if the ID is duplicated

    FROM landing.events AS e

    LEFT JOIN derived.id_map AS u ON u.domain_userid = e.domain_userid

    WHERE e.domain_userid != ''           -- do not aggregate missing values
      AND e.domain_userid IS NOT NULL     -- do not aggregate NULL
      AND e.domain_sessionidx IS NOT NULL -- do not aggregate NULL
      AND e.collector_tstamp IS NOT NULL  -- not required
      AND e.dvce_created_tstamp IS NOT NULL       -- not required

      AND e.dvce_created_tstamp < DATEADD(year, +1, e.collector_tstamp) -- remove outliers (can cause errors)
      AND e.dvce_created_tstamp > DATEADD(year, -1, e.collector_tstamp) -- remove outliers (can cause errors)

    --AND e.app_id = 'production'
    --AND e.platform = ''
    --AND e.page_urlhost = ''
    --AND e.page_urlpath IS NOT NULL

  ) WHERE event_number = 1

), example_unstructured_event AS (

  -- (b) deduplicate an example unstructured event

  -- OPTION 1: weak deduplication

  SELECT * FROM landing.com_example_unstructured_event_1 GROUP BY 1,2,3,4,5 -- include ALL columns in GROUP BY

  -- OPTION 2: strong deduplication

  SELECT * FROM (
    SELECT

      root_id, -- add all relevant columns

      ROW_NUMBER() OVER (PARTITION BY root_id) as event_number -- select one event at random if the ID is duplicated

    FROM landing.com_example_unstructured_event_1
  ) WHERE event_number = 1

), example_context AS (

  -- (c) deduplicate an example context

  SELECT * FROM landing.com_example_context_1 GROUP BY 1,2,3,4,5 -- include ALL columns in GROUP BY

)

-- (d) combine all tables

SELECT

  a.*,

  CASE WHEN b.root_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_example_unstructured_event,

  b.*, -- select all relevant columns

  CASE WHEN c.root_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_example_context,

  c.* -- select all relevant columns

FROM events AS a

LEFT JOIN example_unstructured_event AS b ON a.event_id = b.root_id
LEFT JOIN example_context AS c ON a.event_id = c.root_id

);

INSERT INTO snplw_temp.queries (SELECT 'enriched-events', 'enriched-events', GETDATE()); -- track time
