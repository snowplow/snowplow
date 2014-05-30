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

- view: visitors
  derived_table: 
    sql: |
      SELECT
        v.domain_userid,
        v.first_touch,
        v.last_touch,
        v.number_of_events,
        v.distinct_pages_viewed,
        v.number_of_sessions,
        l.page_urlhost,
        l.page_urlpath,
        s.mkt_source,
        s.mkt_medium,
        s.mkt_campaign,
        s.mkt_term,
        s.refr_source,
        s.refr_medium,
        s.refr_term,
        s.refr_urlhost,
        s.refr_urlpath
      FROM
        ${visitors_basic.SQL_TABLE_NAME} AS v
        LEFT JOIN ${sessions_landing_page.SQL_TABLE_NAME} AS l
        ON v.domain_userid = l.domain_userid
        AND l.domain_sessionidx = 1
        LEFT JOIN ${sessions_source.SQL_TABLE_NAME} AS s
        ON v.domain_userid = s.domain_userid
        AND s.domain_sessionidx = 1
        
    
    sql_trigger_value: SELECT COUNT(*) FROM ${visitors_basic.SQL_TABLE_NAME}
    distkey: domain_userid
    sortkeys: [domain_userid, first_touch]
  
  
  fields:
  
  # DIMENSIONS # 
  
  # Basic dimensions #
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: first_touch
    sql: ${TABLE}.first_touch
    
  - dimension_group: first_touch
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.first_touch
    
  - dimension: last_touch
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.last_touch
    
  - dimension: events_during_lifetime
    type: int
    sql: ${TABLE}.number_of_events
    
  - dimension: events_during_lifetime_tiered
    type: tier
    tiers: [1,5,10,25,50,100,1000,10000,100000]
    sql: ${TABLE}.number_of_events
    
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.number_of_events = 1
    
  - dimension: distinct_pages_viewed
    type: int
    sql: ${TABLE}.distinct_pages_viewed
    
  - dimension: distinct_pages_viewed_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000]
    sql: ${distinct_pages_viewed}
    
  - dimension: number_of_sessions
    type: int
    sql: ${TABLE}.number_of_sessions
    
  - dimension: number_of_sessions_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000]
    sql: ${number_of_sessions}
    
  - dimension: session_stream
    sql: ${user_id}
    html: |
      <a href=sessions?fields=sessions.individual_detail*&f[sessions.user_id]=<%=value%>>Session Stream</a>
      
  - dimension: event_stream
    sql: ${user_id}
    html: |
      <a href=events?fields=events.event_detail*&f[events.user_id]=<%=value%>>Event stream</a>
      
  # Landing page dimensions #
  
  - dimension: landing_page_url_host
    sql: ${TABLE}.page_urlhost
    
  - dimension: landing_page_url_path
    sql: ${TABLE}.page_urlpath
    
  - dimension: landing_page_url
    sql: ${TABLE}.page_urlhost || ${TABLE}.page_urlpath  
    
  # Referer source dimensions #
  
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
      
  # Measures #
      
  - measure: count
    type: count_distinct
    sql: ${user_id}
    detail: individual_detail*
    
  - measure: bounced_visitor_count
    type: count_distinct
    sql: ${user_id}
    filter:
      bounce: yes
    detail: detail*
    
  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_visitor_count}/NULLIF(${count},0)::REAL
    
  - measure: events_count
    type: sum
    sql: ${TABLE}.number_of_events
    
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${events_count}/NULLIF(${count},0)::REAL
    
  - measure: sessions_count
    type: sum
    sql: ${TABLE}.number_of_sessions
    detail: details*
    
  - measure: sessions_per_visitor
    type: number
    decimals: 2
    sql: ${sessions_count}/NULLIF(${count},0)::REAL
    
  # Landing page measures #
    
  - measure: landing_page_count
    type: count_distinct
    sql: ${landing_page_url}
    detail:
    - landing_page_url
    - detail*
    
  # Traffic source measures #
  
  - measure: campaign_medium_count
    type: count_distinct
    sql: ${campaign_medium}
    detail: 
    - campaign_medium
    - detail*
    
  - measure: campaign_source_count
    type: count_distinct
    sql: ${campaign_source}
    detail: 
    - campaign_medium
    - campaign_source
    - detail*
    
  - measure: campaign_term_count
    type: count_distinct
    sql: ${campaign_term}
    detail: 
    - campaign_medium
    - campaign_source
    - campaign_term
    - detail*
      
  - measure: campaign_count
    type: count_distinct
    sql: ${campaign_name}
    detail: 
    - campaign_medium
    - campaign_source
    - campaign_term
    - detail*
    
  - measure: referer_medium_count
    type: count_distinct
    sql: ${referer_medium}
    detail: 
    - referer_medium
    - detail*
    
  - measure: referer_source_count
    type: count_distinct
    sql: ${referer_source}
    detail: 
    - referer_medium
    - referer_source
    - detail*
    
  - measure: referer_term_count
    type: count_distinct
    sql: ${referer_term}
    detail: 
    - referer_medium
    - referer_source
    - referer_term
    - detail*

  sets:    
    detail:
      - count
      - bounce_rate
      - sessions_per_visitor
      - events_per_visitor
      - campaign_medium_count
      - campaign_source_count
      - campaign_term_count
      - campaign_count
      - referer_medium_count
      - referer_source_count
      - referer_term_count
      
    individual_detail:
      - user_id
      - first_touch
      - last_touch
      - referer_medium
      - referer_source
      - referer_host
      - referer_url_host
      - referer_url_path
      - campaign_medium
      - campaign_source
      - campaign_name
      - landing_page_url
      - number_of_sessions
      - number_of_events
      - session_stream
      - event_stream
    