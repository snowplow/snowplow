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

DROP TABLE IF EXISTS web.users_tmp;
CREATE TABLE web.users_tmp
  DISTKEY(user_snowplow_domain_id)
  SORTKEY(first_session_start)
AS (

  WITH prep AS (

    SELECT

      user_snowplow_domain_id,

      -- time

      MIN(session_start) AS first_session_start,
      MIN(session_start_local) AS first_session_start_local,

      MAX(session_end) AS last_session_end,

      -- engagement

      SUM(page_views) AS page_views,
      COUNT(*) AS sessions,

      SUM(time_engaged_in_s) AS time_engaged_in_s

    FROM web.sessions_tmp

    GROUP BY 1
    ORDER BY 1

  )

  SELECT

    -- user

    a.user_custom_id,
    a.user_snowplow_domain_id,
    a.user_snowplow_crossdomain_id,

    -- first sesssion: time

    b.first_session_start,

      -- example derived dimensions

      TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24:MI:SS') AS first_session_time,
      TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24:MI') AS first_session_minute,
      TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24') AS first_session_hour,
      TO_CHAR(b.first_session_start, 'YYYY-MM-DD') AS first_session_date,
      TO_CHAR(DATE_TRUNC('week', b.first_session_start), 'YYYY-MM-DD') AS first_session_week,
      TO_CHAR(b.first_session_start, 'YYYY-MM') AS first_session_month,
      TO_CHAR(DATE_TRUNC('quarter', b.first_session_start), 'YYYY-MM') AS first_session_quarter,
      DATE_PART(Y, b.first_session_start)::INTEGER AS first_session_year,

    -- first session: time in the user's local timezone

    b.first_session_start_local,

      -- example derived dimensions

      TO_CHAR(b.first_session_start_local, 'YYYY-MM-DD HH24:MI:SS') AS first_session_local_time,
      TO_CHAR(b.first_session_start_local, 'HH24:MI') AS first_session_local_time_of_day,
      DATE_PART(hour, b.first_session_start_local)::INTEGER AS first_session_local_hour_of_day,
      TRIM(TO_CHAR(b.first_session_start_local, 'd')) AS first_session_local_day_of_week,
      MOD(EXTRACT(DOW FROM b.first_session_start_local)::INTEGER - 1 + 7, 7) AS first_session_local_day_of_week_index,

    -- last session: time

    b.last_session_end,

    -- engagement

    b.page_views,
    b.sessions,

    b.time_engaged_in_s,

    -- first page

    a.first_page_url,

    a.first_page_url_scheme,
    a.first_page_url_host,
    a.first_page_url_port,
    a.first_page_url_path,
    a.first_page_url_query,
    a.first_page_url_fragment,

    a.first_page_title,

    -- referer

    a.referer_url,

    a.referer_url_scheme,
    a.referer_url_host,
    a.referer_url_port,
    a.referer_url_path,
    a.referer_url_query,
    a.referer_url_fragment,

    a.referer_medium,
    a.referer_source,
    a.referer_term,

    -- marketing

    a.marketing_medium,
    a.marketing_source,
    a.marketing_term,
    a.marketing_content,
    a.marketing_campaign,
    a.marketing_click_id,
    a.marketing_network,

    -- application

    a.app_id

  FROM web.sessions_tmp AS a

  INNER JOIN prep AS b
    ON a.user_snowplow_domain_id = b.user_snowplow_domain_id

  WHERE a.session_index = 1

);
