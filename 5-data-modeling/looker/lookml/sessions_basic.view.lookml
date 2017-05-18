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

- view: sessions_basic
  derived_table:
    sql: |
      SELECT
        domain_userid,
        domain_sessionidx,
        MIN(collector_tstamp) AS session_start_tstamp,
        MAX(collector_tstamp) AS session_end_tstamp,
        MIN(dvce_tstamp) AS dvce_min_tstamp,
        MAX(dvce_tstamp) AS dvce_max_tstamp,
        COUNT(*) AS event_count,
        COUNT(DISTINCT(FLOOR(EXTRACT(EPOCH FROM dvce_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
      FROM
        atomic.events
      WHERE domain_sessionidx IS NOT NULL
        AND domain_userid IS NOT NULL
        AND domain_userid != ''
        AND dvce_tstamp IS NOT NULL
        AND dvce_tstamp > '2000-01-01' -- Prevent SQL errors
        AND dvce_tstamp < '2030-01-01' -- Prevent SQL errors
        -- if dev -- AND collector_tstamp > DATEADD (day, -2, GETDATE())
      GROUP BY 1,2

    sql_trigger_value: SELECT MAX(collector_tstamp) FROM ${events.SQL_TABLE_NAME}  # Trigger table generation when new data loaded into atomic.events
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx]

  fields:
  
  # DIMENSIONS #
  
  # Basic dimensions
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: session_index
    type: int
    sql: ${TABLE}.domain_sessionidx
  
  - dimension: session_id
    sql: ${TABLE}.domain_userid || '-' || ${TABLE}.domain_sessionidx
  
  - dimension: session_index_tier
    type: tier
    tiers: [1,2,3,4,5,10,25,100,1000]
    sql: ${session_index}
  
  - dimension: start
    sql: ${TABLE}.session_start_tstamp
  
  - dimension_group: start
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.session_start_tstamp
    
  - dimension: end
    sql: ${TABLE}.session_end_tstamp
    
  # Session duration

  - dimension: session_duration_seconds
    type: int
    sql: EXTRACT(EPOCH FROM (${TABLE}.session_end_tstamp - ${TABLE}.session_start_tstamp))

  - dimension: session_duration_seconds_tiered
    type: tier
    tiers: [0,1,5,10,30,60,300,900]
    sql: ${session_duration_seconds}

  - dimension: time_engaged_with_minutes
    sql: ${TABLE}.time_engaged_with_minutes
  
  - dimension: time_engaged_with_minutes_tiered
    type: tier
    tiers: [0,1,5,10,30,60,300,900]
    sql: ${time_engaged_with_minutes}

  # Events per visit and bounces (infered)

  - dimension: events_during_session
    type: int
    sql: ${TABLE}.event_count
    
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.event_count = 1
  
  # New vs returning visitor

  - dimension: new_vs_returning_visitor
    sql_case:
      new: ${TABLE}.domain_sessionidx = 1
      returning: ${TABLE}.domain_sessionidx > 1
      else: unknown
  
  # Event history

  - dimension: history
    sql: ${session_id}
    html: |
      <a href=events?fields=events.event_detail*&f[events.session_id]={{value}}>Event Stream</a>
  
  # MEASURES #

  - measure: count
    type: count_distinct
    sql: ${session_id}
    drill_fields: detail*

  - measure: visitors_count
    type: count_distinct
    sql: ${user_id}
    drill_fields: detail*
    hidden: true
    
  - measure: bounced_sessions_count
    type: count_distinct
    sql: ${session_id}
    filters:
      bounce: yes
    drill_fields: detail*

  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_sessions_count}/NULLIF(${count},0)::REAL
  
  - measure: sessions_from_new_visitors_count
    type: count_distinct
    sql: ${session_id}
    filters:
      session_index: 1
    drill_fields: detail*
  
  - measure: sessions_from_returning_visitor_count
    type: number
    sql: ${count} - ${sessions_from_new_visitors_count}
    drill_fields: detail*
  
  - measure: new_visitors_count_over_total_visitors_count
    type: number
    decimals: 2
    sql: ${sessions_from_new_visitors_count}/NULLIF(${count},0)::REAL
    drill_fields: detail*

  - measure: returning_visitors_count_over_total_visitors_count
    type: number
    decimals: 2
    sql: ${sessions_from_returning_visitor_count}/NULLIF(${count},0)::REAL
    drill_fields: detail*
  
  - measure: average_session_duration_seconds
    type: average
    sql: EXTRACT(EPOCH FROM (${end}-${start}))
      
  - measure: average_time_engaged_minutes
    type: average
    sql: ${time_engaged_with_minutes}
  