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

-- The sessions_tech table has one row per session (in this batch) and assigns browser details to each session.

-- The standard model identifies sessions using only first party cookies and session domain indexes,
-- but contains placeholders for identity stitching.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_technology;
CREATE TABLE snowplow_intermediary.sessions_technology
  DISTKEY (domain_userid) -- Optimized to join on other snowplow_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other snowplow_intermediary.session_X tables
AS (
  SELECT
    *
  FROM (
    SELECT -- Select the last page (using dvce_tstamp)
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
      RANK() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx
        ORDER BY a.br_name, a.br_family, a.br_version, a.br_type, a.br_renderengine, a.br_lang, a.br_features_director, a.br_features_flash, 
          a.br_features_gears, a.br_features_java, a.br_features_pdf, a.br_features_quicktime, a.br_features_realplayer, a.br_features_silverlight,
          a.br_features_windowsmedia, a.br_cookies, a.os_name, a.os_family, a.os_manufacturer, a.os_timezone, a.dvce_type, a.dvce_ismobile, a.dvce_screenwidth,
          a.dvce_screenheight) AS rank
    FROM snowplow_intermediary.events_enriched_final AS a
    INNER JOIN snowplow_intermediary.sessions_basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_tstamp = b.dvce_min_tstamp -- Replaces the FIRST VALUE window function in SQL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
  )
  WHERE rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
);
