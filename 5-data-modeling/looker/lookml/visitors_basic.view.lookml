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

- view: visitors_basic
  derived_table: 
    sql: |
      SELECT
        domain_userid,
        MIN(collector_tstamp) AS first_touch_tstamp,
        MAX(collector_tstamp) AS last_touch_tstamp,
        MIN(dvce_tstamp) AS dvce_min_tstamp,
        MAX(dvce_tstamp) AS dvce_max_tstamp,
        COUNT(*) AS event_count,
        MAX(domain_sessionidx) AS session_count,
        SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
        COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM dvce_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
      FROM
        atomic.events
      WHERE domain_sessionidx IS NOT NULL
        AND domain_userid IS NOT NULL
        AND domain_userid != ''
        AND dvce_tstamp IS NOT NULL
        AND dvce_tstamp > '2000-01-01' -- Prevent SQL errors
        AND dvce_tstamp < '2030-01-01' -- Prevent SQL errors
        -- if dev -- AND collector_tstamp > '2015-03-20'
      GROUP BY 1
    
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions.SQL_TABLE_NAME} # Generate this table after sessions
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
    timeframes: [time, date, week, month]
    sql: ${TABLE}.first_touch_tstamp
    
  - dimension: last_touch
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.last_touch_tstamp
    
  - dimension: number_of_events
    type: int
    sql: ${TABLE}.event_count
    
  - dimension: number_of_events_tiered
    type: tier
    tiers: [1,5,10,25,50,100,1000,10000,100000]
    sql: ${number_of_events}
    
  - dimension: bounce
    type: yesno
    sql: ${number_of_events} = 1
    
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
      <a href=events?fields=events.individual_detail*&f[events.user_id]={{value}}>Event stream</a>
  
  # MEASURES #

  - measure: count
    type: count_distinct
    sql: ${user_id}
    detail: detail*
