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
# Version: 2-0-0
#
# Author(s): Yali Sassoon
# Copyright: Copyright (c) 2013-2014 Snowplow Analytics Ltd
# License: Apache License Version 2.0

- view: sessions_geo
  derived_table:
    sql: |
      SELECT
        v.domain_userid,
        v.domain_sessionidx,
        g.name AS geo_country,
        v.geo_country AS geo_country_code_2_characters,
        g.three_letter_iso_code AS geo_country_code_3_characters,
        v.geo_region,
        v.geo_city,
        v.geo_zipcode,
        v.geo_latitude,
        v.geo_longitude
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
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
            FIRST_VALUE(geo_country) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_country,
            FIRST_VALUE(geo_region) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_region,
            FIRST_VALUE(geo_city) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_city,
            FIRST_VALUE(geo_zipcode) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_zipcode,
            FIRST_VALUE(geo_latitude) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_latitude,
            FIRST_VALUE(geo_longitude) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_longitude
          FROM atomic.events) AS a
        GROUP BY 1,2,3,4,5,6,7,8
        ) AS v
        LEFT JOIN reference_data.country_codes AS g
        ON v.geo_country = g.two_letter_iso_code
    
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_basic.SQL_TABLE_NAME} # Generate this table *after* the sessions_basic table is generated
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx]
    
  fields:
  
  # DIMENSIONS #
  
  # Basic dimensions #
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: session_index
    type: int
    sql: ${TABLE}.domain_sessionidx

  # Geo fields #
  
  - dimension: geography_country
    sql: ${TABLE}.geo_country
    
  - dimension: geography_country_three_letter_iso_code
    sql: ${TABLE}.geo_country_code_3_characters
    
  - dimension: geography_country_two_letter_iso_code
    sql: ${TABLE}.geo_country_code_2_characters
  
  - dimension: geography_region
    sql: ${TABLE}.geo_region
    
  - dimension: geography_city
    sql: ${TABLE}.geo_city
    
  - dimension: geography_zipcode
    sql: ${TABLE}.geo_zipcode
    
  - dimension: geography_latitude
    sql: ${TABLE}.geo_latitude
  
  - dimension: geography_longitude
    sql: ${TABLE}.geo_longitude
    
  # MEASURES #
  
  # Geo measures
  - measure: country_count
    type: count_distinct
    sql: ${geography_country}
    detail: detail*
    
  - measure: region_count
    type: count_distinct
    sql: ${geography_region}
    detail: detail*
    
  - measure: city_count
    type: count_distinct
    sql: ${geography_city}
    detail: detail*
      
  - measure: zip_code_count
    type: count_distinct
    sql: ${geography_zipcode}
    detail: detail*
    
  - measure: count
    type: count
  
  