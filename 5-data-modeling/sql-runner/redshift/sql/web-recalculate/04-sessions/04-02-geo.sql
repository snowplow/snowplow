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

-- The sessions_geo table has one row per session and assigns a geography to each session.

-- The standard model identifies sessions using only first party cookies and session domain indexes,
-- but contains placeholders for identity stitching.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_geo;
CREATE TABLE snowplow_intermediary.sessions_geo
  DISTKEY (domain_userid) -- Optimized to join on other snowplow_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other snowplow_intermediary.session_X tables
AS (
  SELECT -- Join with reference_data.country_codes
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
    SELECT -- Select the geographical information associated with the earliest dvce_tstamp
      a.domain_userid,
      a.domain_sessionidx,
      a.geo_country,
      a.geo_region,
      a.geo_city,
      a.geo_zipcode,
      a.geo_latitude,
      a.geo_longitude,
      RANK() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx
        ORDER BY a.geo_country, a.geo_region, a.geo_city, a.geo_zipcode, a.geo_latitude, a.geo_longitude) AS rank
    FROM snowplow_intermediary.events_enriched_final AS a
    INNER JOIN snowplow_intermediary.sessions_basic AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_tstamp = b.dvce_min_tstamp -- Replaces the FIRST VALUE window function in SQL
    GROUP BY 1,2,3,4,5,6,7,8 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
  ) AS c
  LEFT JOIN reference_data.country_codes AS d
    ON c.geo_country = d.two_letter_iso_code
  WHERE c.rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
);
