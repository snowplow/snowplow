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

- view: visitors
  derived_table: 
    sql: |
      SELECT
        domain_userid,
        MIN(collector_tstamp) AS first_touch_ts,
        MAX(collector_tstamp) AS last_touch_ts,
        COUNT(*) AS number_of_events,
        MAX(domain_sessionidx) AS number_of_visits
      FROM atomic.events
      WHERE domain_userid IS NOT NULL
      AND domain_userid <> ''
      GROUP BY 1
      
    persist_for: 3 hours
  
  fields:
  
  # DIMENSIONS # 
  
  - dimension: domain_userid
    primary_key: true
    sql: ${TABLE}.domain_userid
  
  - dimension: events_for_visitor
    type: int
    sql: ${TABLE}.number_of_events
    
  - dimension: events_tiered
    type: tier
    tiers: [1,2,3,4,5,10,25,100,1000,10000]
    sql: ${TABLE}.number_of_events
    
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.number_of_events = 1

  - dimension_group: first_touch
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.first_touch_ts
    
  - dimension_group: last_touch
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.last_touch_ts

  - dimension: lifetime_visits
    type: int
    sql: ${TABLE}.number_of_visits
    
  - dimension: lifetime_visits_tiered
    type: tier
    tiers: [1,2,3,4,5,10,25]
    sql: ${TABLE}.number_of_visits
  
  - dimension: visit_history
    sql: ${domain_userid}
    html: |
      <a href=visits?fields=visits.detail*&f[visits.domain_userid]=<%= value%>>Visit Stream</a>
    
  - dimension: event_history
    sql: ${domain_userid}
    html: |
      <a href=events?fields=events.event_detail*&f[events.domain_userid]=<%= value%>>Event Stream</a>

  # MEASURES #
  
  - measure: visitors_count
    type: count
    detail: visitors_detail*

  - measure: events_count
    type: sum
    sql: ${TABLE}.number_of_events
    detail: event_detail*
  
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${events_count} / NULLIF(${visitors_count}, 0)::REAL
    
  - measure: visits_count  
    type: sum
    sql: ${lifetime_visits}
    detail: visits_detail
    
  - measure: events_per_visit
    type: number
    decimals: 2
    sql: ${events_count} / NULLIF(${visits_count}, 0)::REAL

  - measure: bounced_visitors_count
    type: count
    filters:
      bounce: yes
    detail: visitors_detail*

  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_visitors_count}/${visitors_count}::REAL

  # ----- Detail ------
  sets:
    visitors_detail:
      - domain_userid
      - first_touch
      - landing_page_original.path
      - source_original.refr_medium
      - source_original.refr_source
      - last_touch
      - visits_count
      - visit_history
      - events_count
      - event_history
    
    visits_detail:
      - domain_userid
      - visit_history
      - event_history
      
    event_detail:
      - domain_userid
      - event_history
      