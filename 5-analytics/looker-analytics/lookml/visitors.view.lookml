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

- view: visitors
  derived_table:
    sql: |
      SELECT
        b.domain_userid,
        b.first_touch_tstamp,
        b.last_touch_tstamp,
        b.dvce_min_tstamp,
        b.dvce_max_tstamp,
        b.event_count,
        b.session_count,
        b.page_view_count,
        b.time_engaged_with_minutes,
        s.landing_page_host,
        s.landing_page_path,
        s.mkt_source,
        s.mkt_medium,
        s.mkt_term,
        s.mkt_content,
        s.mkt_campaign,
        s.refr_source,
        s.refr_medium,
        s.refr_term,
        s.refr_urlhost,
        s.refr_urlpath
      FROM ${visitors_basic.SQL_TABLE_NAME} b
      LEFT JOIN ${sessions.SQL_TABLE_NAME} AS s
        ON b.domain_userid = s.domain_userid
        AND s.domain_sessionidx = 1
    
    sql_trigger_value: SELECT COUNT(*) FROM ${visitors_basic.SQL_TABLE_NAME} # Generate this table after visitors_source
    distkey: domain_userid
    sortkeys: [domain_userid]
  
  fields:
  
  # DIMENSIONS # 
  
  # Basic dimensions
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: first_touch
    sql: ${TABLE}.first_touch_tstamp
    
  - dimension_group: first_touch
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.first_touch_tstamp
    
  - dimension: last_touch
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.last_touch_tstamp
    
  - dimension: events_during_lifetime
    type: int
    sql: ${TABLE}.event_count
    
  - dimension: events_during_lifetime_tiered
    type: tier
    tiers: [1,5,10,25,50,100,1000,10000,100000]
    sql: ${TABLE}.event_count
    
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.event_count = 1
    
  - dimension: number_of_page_views
    type: int
    sql: ${TABLE}.page_view_count
    
  - dimension: number_of_page_views_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000]
    sql: ${number_of_page_views}
    
  - dimension: number_of_sessions
    type: int
    sql: ${TABLE}.session_count
    
  - dimension: number_of_sessions_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000]
    sql: ${number_of_sessions}

  - dimension: time_engaged_with_minutes
    sql: ${TABLE}.time_engaged_with_minutes
  
  - dimension: time_engaged_with_minutes_tiered
    type: tier
    tiers: [0,1,5,10,30,60,300,900]
    sql: ${time_engaged_with_minutes}

  - dimension: session_stream
    sql: ${user_id}
    html: |
      <a href=sessions?fields=sessions.individual_detail*&f[sessions.user_id]={{value}}>Session Stream</a>
      
  - dimension: event_stream
    sql: ${user_id}
    html: |
      <a href=events?fields=events.event_detail*&f[events.user_id]={{value}}>Event stream</a>
      
  # Landing page dimensions
  
  - dimension: landing_page_url_host
    sql: ${TABLE}.page_urlhost
    
  - dimension: landing_page_url_path
    sql: ${TABLE}.page_urlpath
    
  - dimension: landing_page_url
    sql: ${TABLE}.page_urlhost || ${TABLE}.page_urlpath

  # Referer fields (all acquisition channels)
  
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
    
  # Marketing fields (paid acquisition channels)
    
  - dimension: campaign_medium
    sql: ${TABLE}.mkt_medium
  
  - dimension: campaign_source
    sql: ${TABLE}.mkt_source
  
  - dimension: campaign_term
    sql: ${TABLE}.mkt_term
  
  - dimension: campaign_name
    sql: ${TABLE}.mkt_campaign

  - dimension: campaign_content
    sql: ${TABLE}.mkt_content
  
  # MEASURES #

  # Basic measures
  
  - measure: count
    type: count_distinct
    sql: ${user_id}
    drill_fields: individual_detail*
  
  - measure: bounced_visitor_count
    type: count_distinct
    sql: ${user_id}
    filter:
      bounce: yes
    drill_fields: detail*
    
  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_visitor_count}/NULLIF(${count},0)::REAL
    
  - measure: event_count
    type: sum
    sql: ${TABLE}.event_count
    
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${event_count}/NULLIF(${count},0)::REAL
    
  - measure: session_count
    type: sum
    sql: ${TABLE}.session_count
    drill_fields: details*
    
  - measure: sessions_per_visitor
    type: number
    decimals: 2
    sql: ${session_count}/NULLIF(${count},0)::REAL
    
  # Landing page measures
    
  - measure: landing_page_count
    type: count_distinct
    sql: ${landing_page_url}
    drill_fields:
    - landing_page_url
    - detail*
    
  # Marketing measures (paid acquisition channels)
  
  - measure: campaign_medium_count
    type: count_distinct
    sql: ${campaign_medium}
    drill_fields:
    - campaign_medium
    - detail*
    
  - measure: campaign_source_count
    type: count_distinct
    sql: ${campaign_source}
    drill_fields:
    - campaign_medium
    - campaign_source
    - detail*
    
  - measure: campaign_term_count
    type: count_distinct
    sql: ${campaign_term}
    drill_fields:
    - campaign_medium
    - campaign_source
    - campaign_term
    - detail*
      
  - measure: campaign_count
    type: count_distinct
    sql: ${campaign_name}
    drill_fields:
    - campaign_medium
    - campaign_source
    - campaign_term
    - detail*
  
  # Referer measures (all acquisition channels)

  - measure: referer_medium_count
    type: count_distinct
    sql: ${referer_medium}
    drill_fields:
    - referer_medium
    - detail*
    
  - measure: referer_source_count
    type: count_distinct
    sql: ${referer_source}
    drill_fields:
    - referer_medium
    - referer_source
    - detail*
    
  - measure: referer_term_count
    type: count_distinct
    sql: ${referer_term}
    drill_fields:
    - referer_medium
    - referer_source
    - referer_term
    - detail*

  # DRILL FIELDS #

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
      - evenets_during_lifetime
      - session_stream
      - event_stream
    