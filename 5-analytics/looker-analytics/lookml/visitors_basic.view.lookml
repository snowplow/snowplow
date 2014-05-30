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

- view: visitors_basic
  derived_table: 
    sql: |
      SELECT
        domain_userid,
        MIN(collector_tstamp) AS first_touch,
        MAX(collector_tstamp) AS last_touch,
        COUNT(*) AS number_of_events,
        COUNT(DISTINCT page_urlpath) AS distinct_pages_viewed,
        MAX(domain_sessionidx) AS number_of_sessions
      FROM atomic.events
      WHERE domain_userid IS NOT NULL
        AND domain_userid <> ''
      GROUP BY 1
      
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions.SQL_TABLE_NAME}
    distkey: domain_userid
    sortkeys: [domain_userid]
    
  fields:
  
  # DIMENSIONS #
  
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
    
  - dimension: number_of_events
    type: int
    sql: ${TABLE}.number_of_events
    
  - dimension: number_of_events_tiered
    type: tier
    tiers: [1,5,10,25,50,100,1000,10000,100000]
    sql: ${number_of_events}
    
  - dimension: bounce
    type: yesno
    sql: ${number_of_events} = 1
    
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
      <a href=events?fields=events.individual_detail*&f[events.user_id]=<%=value%>>Event stream</a>
      
  - measure: count
    type: count_distinct
    sql: ${user_id}
    detail: detail*
    