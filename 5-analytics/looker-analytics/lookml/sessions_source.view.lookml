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

- view: sessions_source
  derived_table:
    sql: |
      SELECT *
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
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
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx 
            ORDER BY dvce_tstamp, mkt_source, mkt_medium, mkt_campaign, mkt_term, refr_source, refr_medium, refr_term, refr_urlhost, refr_urlpath) AS "rank"
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
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) AS t
      WHERE "rank" = 1 -- Only pull the first referer for each visit
    
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_last_page.SQL_TABLE_NAME}
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
    
  
  # Referer fields (all acquisition channels) #
    
  - dimension: referer_medium
    sql_case:
      email: ${TABLE}.refr_medium = 'email'
      search: ${TABLE}.refr_medium = 'search'
      social: ${TABLE}.refr_medium = 'social'
      other_website: ${TABLE}.refr_medium = 'unknown'
      else: direct
    
  - dimension: referer_source
    sql: ${TABLE}.refr_source
    
  - dimension: referer_term
    sql: ${TABLE}.refr_term
    
  - dimension: referer_url_host
    sql: ${TABLE}.refr_urlhost
  
  - dimension: referer_url_path
    sql: ${TABLE}.refr_urlpath
    
  # MKT fields (paid acquisition channels)
    
  - dimension: campaign_medium
    sql: ${TABLE}.mkt_medium
  
  - dimension: campaign_source
    sql: ${TABLE}.mkt_source
  
  - dimension: campaign_term
    sql: ${TABLE}.mkt_term
  
  - dimension: campaign_name
    sql: ${TABLE}.mkt_campaign

  # MEASURES #

  - measure: campaign_medium_count
    type: count_distinct
    sql: ${campaign_medium}
    detail: detail*
    
  - measure: campaign_source_count
    type: count_distinct
    sql: ${campaign_source}
    detail: detail*
    
  - measure: campaign_term_count
    type: count_distinct
    sql: ${campaign_term}
    detail: detail*
      
  - measure: campaign_count
    type: count_distinct
    sql: ${campaign_name}
    detail: detail*
    
  - measure: referer_medium_count
    type: count_distinct
    sql: ${referer_medium}
    detail: detail*
    
  - measure: referer_source_count
    type: count_distinct
    sql: ${referer_source}
    detail: detail*
    
  - measure: referer_term_count
    type: count_distinct
    sql: ${referer_term}
    detail: detail*
    
  - measure: count
    type: count
      
    