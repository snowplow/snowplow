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

DROP TABLE IF EXISTS {{.output_schema}}.users_stitch_tmp;
CREATE TABLE {{.output_schema}}.users_stitch_tmp
  DISTKEY(user_id_combined)
  SORTKEY(first_session_start)

AS (

WITH Prep AS (

      SELECT

        -- user

        user_id_combined,
        CASE WHEN user_custom_id IS NULL
          THEN 'snowplow_domain_user_id'
        ELSE 'snowplow_custom_user_id' END         AS user_id_type,
        COUNT(*)                                   AS numer_of_devices,

        -- first session: time

        MIN(first_session_start)                   AS first_session_start,

        -- example derived dimensions

        MIN(first_session_minute)                  AS first_session_minute,
        MIN(first_session_hour)                    AS first_session_hour,
        MIN(first_session_date)                    AS first_session_date,
        MIN(first_session_week)                    AS first_session_week,
        MIN(first_session_month)                   AS first_session_month,
        MIN(first_session_quarter)                 AS first_session_quarter,
        MIN(first_session_year)                    AS first_session_year,

        -- first session: time in the user's local timezone

        MIN(first_session_start_local)             AS first_session_start_local,

        -- example derived dimensions

        MIN(first_session_local_time)              AS first_session_local_time,
        MIN(first_session_local_time_of_day)       AS first_session_local_time_of_day,
        MIN(first_session_local_hour_of_day)       AS first_session_local_hour_of_day,
        MIN(first_session_local_day_of_week)       AS first_session_local_day_of_week,
        MIN(first_session_local_day_of_week_index) AS first_session_local_day_of_week_index,

        -- last session: time

        MAX(last_session_end)                      AS last_session_end,

        -- engagement

        SUM(page_views)                            AS page_views,
        SUM(sessions)                              AS sessions,
        SUM(time_engaged_in_s)                     AS time_engaged_in_s

      -- first page


      FROM {{.output_schema}}.users_tmp
      GROUP BY 1, 2

  )

  SELECT
  a.user_id_combined,
  a.user_id_type,
  a.numer_of_devices,
  a.first_session_start,
  a.first_session_minute,
  a.first_session_hour,
  a.first_session_date,
  a.first_session_month,
  a.first_session_quarter,
  a.first_session_year,
  a.first_session_start_local,
  a.first_session_local_time,
  a.first_session_local_time_of_day,
  a.first_session_local_hour_of_day,
  a.first_session_local_day_of_week,
  a.first_session_local_day_of_week_index,
  a.last_session_end,
  a.page_views,
  a.sessions,
  a.time_engaged_in_s,

  -- first page

  b.first_page_url,

  b.first_page_url_scheme,
  b.first_page_url_host,
  b.first_page_url_port,
  b.first_page_url_path,
  b.first_page_url_query,
  b.first_page_url_fragment,

  b.first_page_title,

  -- referer

  b.referer_url,
  b.referer_url_scheme,
  b.referer_url_host,
  b.referer_url_port,
  b.referer_url_path,
  b.referer_url_query,
  b.referer_url_fragment,

  b.referer_medium,
  b.referer_source,
  b.referer_term,

  -- marketing

  b.marketing_medium,
  b.marketing_source,
  b.marketing_term,
  b.marketing_content,
  b.marketing_campaign,
  b.marketing_click_id,
  b.marketing_network,

  -- application

  b.app_id

  FROM prep AS a

  LEFT JOIN {{.scratch_schema}}.users_rank AS b
  ON a.user_id_combined = b.user_id_combined
  );
