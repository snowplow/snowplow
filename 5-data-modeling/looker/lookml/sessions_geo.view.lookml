# Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
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
# Version: 3-0-0
#
# Authors: Yali Sassoon, Christophe Bogaert
# Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
# License: Apache License Version 2.0

- view: sessions_geo
  derived_table:
    sql: |
      SELECT
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
        FROM atomic.events AS a
        INNER JOIN ${sessions_basic.SQL_TABLE_NAME} AS b
          ON  a.domain_userid = b.domain_userid
          AND a.domain_sessionidx = b.domain_sessionidx
          AND a.dvce_tstamp = b.dvce_min_tstamp
        GROUP BY 1,2,3,4,5,6,7,8 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
      ) AS c
      LEFT JOIN reference_data.country_codes AS d
        ON c.geo_country = d.two_letter_iso_code
      WHERE c.rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
    
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_basic.SQL_TABLE_NAME} # Generate this table after the sessions_basic table
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
    drill_fields: detail*
    
  - measure: region_count
    type: count_distinct
    sql: ${geography_region}
    drill_fields: detail*
    
  - measure: city_count
    type: count_distinct
    sql: ${geography_city}
    drill_fields: detail*
      
  - measure: zip_code_count
    type: count_distinct
    sql: ${geography_zipcode}
    drill_fields: detail*
    
  - measure: count
    type: count
  
  