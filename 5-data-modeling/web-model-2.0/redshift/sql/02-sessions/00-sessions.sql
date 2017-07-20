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

DROP TABLE IF EXISTS web.sessions_tmp;
CREATE TABLE web.sessions_tmp
  DISTKEY(user_snowplow_domain_id)
  SORTKEY(session_start)
AS (

  WITH prep AS (

    SELECT

      session_id,

      -- time

      MIN(page_view_start) AS session_start,
      MAX(page_view_end) AS session_end,

      MIN(page_view_start_local) AS session_start_local,
      MAX(page_view_end_local) AS session_end_local,

      -- engagement

      COUNT(*) AS page_views,

      SUM(CASE WHEN user_bounced THEN 1 ELSE 0 END) AS bounced_page_views,
      SUM(CASE WHEN user_engaged THEN 1 ELSE 0 END) AS engaged_page_views,

      SUM(time_engaged_in_s) AS time_engaged_in_s

    FROM web.page_views_tmp

    GROUP BY 1
    ORDER BY 1

  )

  SELECT

    -- user

    a.user_custom_id,
    a.user_snowplow_domain_id,
    a.user_snowplow_crossdomain_id,

    -- sesssion

    a.session_id,
    a.session_index,

    -- session: time

    b.session_start,
    b.session_end,

      -- example derived dimensions

      TO_CHAR(b.session_start, 'YYYY-MM-DD HH24:MI:SS') AS session_time,
      TO_CHAR(b.session_start, 'YYYY-MM-DD HH24:MI') AS session_minute,
      TO_CHAR(b.session_start, 'YYYY-MM-DD HH24') AS session_hour,
      TO_CHAR(b.session_start, 'YYYY-MM-DD') AS session_date,
      TO_CHAR(DATE_TRUNC('week', b.session_start), 'YYYY-MM-DD') AS session_week,
      TO_CHAR(b.session_start, 'YYYY-MM') AS session_month,
      TO_CHAR(DATE_TRUNC('quarter', b.session_start), 'YYYY-MM') AS session_quarter,
      DATE_PART(Y, b.session_start)::INTEGER AS session_year,

    -- session: time in the user's local timezone

    b.session_start_local,
    b.session_end_local,

      -- example derived dimensions

      TO_CHAR(b.session_start_local, 'YYYY-MM-DD HH24:MI:SS') AS session_local_time,
      TO_CHAR(b.session_start_local, 'HH24:MI') AS session_local_time_of_day,
      DATE_PART(hour, b.session_start_local)::INTEGER AS session_local_hour_of_day,
      TRIM(TO_CHAR(b.session_start_local, 'd')) AS session_local_day_of_week,
      MOD(EXTRACT(DOW FROM b.session_start_local)::INTEGER - 1 + 7, 7) AS session_local_day_of_week_index,

    -- engagement

    b.page_views,

    b.bounced_page_views,
    b.engaged_page_views,

    b.time_engaged_in_s,

      CASE
        WHEN b.time_engaged_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
        WHEN b.time_engaged_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
        WHEN b.time_engaged_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
        WHEN b.time_engaged_in_s BETWEEN 60 AND 119 THEN '60s to 119s'
        WHEN b.time_engaged_in_s BETWEEN 120 AND 239 THEN '120s to 239s'
        WHEN b.time_engaged_in_s > 239 THEN '240s or more'
        ELSE NULL
      END AS time_engaged_in_s_tier,

      CASE WHEN (b.page_views = 1 AND b.bounced_page_views = 1) THEN TRUE ELSE FALSE END AS user_bounced,
      CASE WHEN (b.page_views > 2 AND b.time_engaged_in_s > 59) OR b.engaged_page_views > 0 THEN TRUE ELSE FALSE END AS user_engaged,

    -- first page

    a.page_url AS first_page_url,

    a.page_url_scheme AS first_page_url_scheme,
    a.page_url_host AS first_page_url_host,
    a.page_url_port AS first_page_url_port,
    a.page_url_path AS first_page_url_path,
    a.page_url_query AS first_page_url_query,
    a.page_url_fragment AS first_page_url_fragment,

    a.page_title AS first_page_title,

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

    -- location

    a.geo_country,
    a.geo_region,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone, -- can be NULL

    -- IP

    a.ip_address,

    a.ip_isp,
    a.ip_organization,
    a.ip_domain,
    a.ip_net_speed,

    -- application

    a.app_id,

    -- browser

    a.browser,
    a.browser_name,
    a.browser_major_version,
    a.browser_minor_version,
    a.browser_build_version,
    a.browser_engine,

    a.browser_language,

    -- OS

    a.os,
    a.os_name,
    a.os_major_version,
    a.os_minor_version,
    a.os_build_version,
    a.os_manufacturer,
    a.os_timezone,

    -- device

    a.device,
    a.device_type,
    a.device_is_mobile

  FROM web.page_views_tmp AS a

  INNER JOIN prep AS b
    ON a.session_id = b.session_id

  WHERE a.page_view_in_session_index = 1

);
