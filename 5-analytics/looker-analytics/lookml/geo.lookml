# Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
#
# Version:     0.1.0
#
# Author(s):   Yali Sassoon
# Copyright:   Copyright (c) 2013-2014 Snowplow Analytics Ltd
# License:     Apache License Version 2.0

- view: geo
  derived_table:
    sql: |
      SELECT 
        domain_userid,
        domain_sessionidx,
        domain_userid || '-' || domain_sessionidx AS visit_id,
        geo_country,
        geo_region,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          geo_country,
          geo_region,
          geo_city,
          geo_zipcode,
          geo_latitude,
          geo_longitude,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank" -- Oddly, we have users who show multiple locations per session. Maybe they're working behind proxy IPs?
        FROM atomic.events
        WHERE geo_country IS NOT NULL
        AND geo_country != '' ) AS t
      WHERE "rank" = 1;
    
    persist_for: 3 hours
    
  fields:
  
  # DIMENSIONS #
  
  # Basic dimensions #
  
  - dimension: visit_id
    primary_key: true
    sql: ${TABLE}.visit_id
  
  - dimension: domain_userid
    sql: ${TABLE}.domain_userid
    hidden: true
    
  - dimension: domain_sessionidx
    type: int
    sql: ${TABLE}.domain_sessionidx
    hidden: true

  # Geo fields #
  
  - dimension: country
    sql: ${TABLE}.geo_country
  
  - dimension: region
    sql: ${TABLE}.geo_region
    
  - dimension: city
    sql: ${TABLE}.geo_city
    
  - dimension: zipcode
    sql: ${TABLE}.zipcode
    
  - dimension: latitude
    sql: ${TABLE}.geo_latitude
  
  - dimension: longitude
    sql: ${TABLE}.geo_longitude
    
  # MEASURES #
  # Add measures for count of countries and average longitude / latitude
  # Add detail for drill down
  
  - measure: country_count
    type: count_distinct
    sql: ${country}
    detail: 
      - country
      - region_count
      - detail*
    
  - measure: region_count
    type: count_distinct
    sql: ${region}
    detail: 
      - region 
      - city_count
      - detail*
    
  - measure: city_count
    type: count_distinct
    sql: ${city}
    detail: 
      - city
      - detail*
  
  sets:
    detail:
      - visits.visits_count
      - visits.bounce_rate
      - events.approx_usage_per_visitor_in_minutes
      - events.events_per_visit
  