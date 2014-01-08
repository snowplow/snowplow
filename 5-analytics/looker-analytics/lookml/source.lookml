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

- view: source
  derived_table:
    sql: |
      SELECT *
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          domain_userid || '-' || domain_sessionidx AS visit_id,
          mkt_source,
          mkt_medium,
          mkt_campaign,
          mkt_term,
          refr_source,
          refr_medium,
          refr_term,
          refr_urlhost,
          refr_urlpath,
          dvce_tstamp,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank"
        FROM
          atomic.events
        WHERE
          refr_medium != 'internal' -- Not an internal referer
          AND (
            NOT(refr_medium IS NULL OR refr_medium = '') OR
            NOT ((mkt_campaign IS NULL AND mkt_content IS NULL AND mkt_medium IS NULL AND mkt_source IS NULL AND mkt_term IS NULL)
                    OR (mkt_campaign = '' AND mkt_content = '' AND mkt_medium = '' AND mkt_source = '' AND mkt_term = '')
            )
          ) -- Either the refr or mkt fields are set (not blank)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13) AS t
      WHERE "rank" = 1 -- Only pull the first referer for each visit
    
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
    
  
  # REFR fields (all acquisition channels) #
    
  - dimension: refr_medium
    sql_case:
      email: ${TABLE}.refr_medium = 'email'
      search: ${TABLE}.refr_medium = 'search'
      social: ${TABLE}.refr_medium = 'social'
      other_website: ${TABLE}.refr_medium = 'unknown'
      else: direct
    
  - dimension: refr_source
    sql: ${TABLE}.refr_source
    
  - dimension: refr_term
    sql: ${TABLE}.refr_term
    
  - dimension: refr_urlhost
    sql: ${TABLE}.refr_urlhost
  
  - dimension: refr_urlpath
    sql: ${TABLE}.refr_urlpath
    
  # MKT fields (paid acquisition channels)
    
  - dimension: mkt_medium
    sql: ${TABLE}.mkt_medium
  
  - dimension: mkt_source
    sql: ${TABLE}.mkt_source
  
  - dimension: mkt_term
    sql: ${TABLE}.mkt_term
  
  - dimension: mkt_campaign
    sql: ${TABLE}.mkt_campaign

  # MEASURES #

  - measure: mkt_medium_count
    type: count_distinct
    sql: ${mkt_medium}
    detail: 
      - mkt_medium
      - mkt_source_count
      - detail*
    
  - measure: mkt_source_count
    type: count_distinct
    sql: ${mkt_source}
    detail: 
      - mkt_source
      - mkt_term_count
      - detail*
      
  - measure: mkt_term_count
    type: count_distinct
    sql: ${mkt_term}
    detail:
      - mkt_term
      - detail*
      
  - measure: refr_medium_count
    type: count_distinct
    sql: ${refr_medium}
    detail: 
      - refr_medium
      - refr_source_count
      - detail*
  
  - measure: refr_source_count
    type: count_distinct
    sql: ${refr_source}
    detail: 
      - refr_source
      - refr_term_count
      - detail*
    
  - measure: refr_term_count
    type: count_distinct
    sql: ${refr_term}
    detail: 
      - refr_term
      - detail*
    
  sets:
    detail:
      - landing_page.landing_page_count
      - visits.visits_count
      - visits.bounce_rate
      - events.approx_usage_per_visit_in_minutes
      - events.events_per_visit
      - visits.bounce_rate
      - events.transactions_count
      
    