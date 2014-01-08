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

- view: visits
  derived_table:
    sql: |
      SELECT
        domain_userid,
        domain_sessionidx,
        domain_userid || '-' || domain_sessionidx AS visit_id,
        MIN(collector_tstamp) AS visit_start_ts,
        MIN(dvce_tstamp) AS dvce_visit_start_ts,
        MAX(dvce_tstamp) AS dvce_visit_finish_ts,
        COUNT(*) AS number_of_events
      FROM
        atomic.events
      WHERE domain_userid IS NOT NULL
        AND domain_userid <> ''
      GROUP BY 1,2,3
    
    persist_for: 3 hours
    

  fields:
  
  # DIMENSIONS #
  
  # Basic dimensions #
  
  - dimension: visit_id
    primary_key: true
    sql: ${TABLE}.visit_id
  
  - dimension: domain_userid
    sql: ${TABLE}.domain_userid
    
  - dimension: domain_sessionidx
    type: int
    sql: ${TABLE}.domain_sessionidx
  
  - dimension: session_index_tier
    type: tier
    tiers: [1,2,3,4,5,10,25,100,1000]
    sql: ${domain_sessionidx}
  
  - dimension_group: occurred
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.visit_start_ts
    
  # Visit duration #

  - dimension: time_on_site
    type: int
    sql: EXTRACT(EPOCH FROM (${TABLE}.dvce_visit_finish_ts - ${TABLE}.dvce_visit_start_ts))

  - dimension: time_on_site_tiered
    type: tier
    tiers: [0,1,5,10,30,60,300,900]
    sql: ${time_on_site}

  # Events per visit and bounces (infered) #

  - dimension: events_on_visit
    type: int
    sql: ${TABLE}.number_of_events
    
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.number_of_events = 1
  
  # New vs returning #
  - dimension: new_vs_returning_visitor
    sql_case:
      new: ${TABLE}.domain_sessionidx = 1
      returning: ${TABLE}.domain_sessionidx > 1
      else: unknown
  
  - dimension: history
    sql: ${visit_id}
    html: |
      <a href=events?fields=events.event_detail*&f[events.visit_id]=<%= value%>>Event Stream</a>
  
  # MEASURES #

  - measure: visits_count
    type: count_distinct
    sql: ${visit_id}
    detail: detail*

  - measure: visitors_count
    type: count_distinct
    sql: ${TABLE}.domain_userid
    detail: detail*
    
  - measure: bounced_visits_count
    type: count_distinct
    sql: ${visit_id}
    filters:
      bounce: yes
    detail: detail*

  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_visits_count}/NULLIF(${visits_count},0)::REAL
  
  - measure: visits_from_new_visitor_count
    type: count_distinct
    sql: ${visit_id}
    filters:
      domain_sessionidx: 1
    detail: detail*
  
  - measure: visits_from_returning_visitor_count
    type: number
    sql: ${visits_count} - ${visits_from_new_visitor_count}
    detail: detail*
  
  - measure: new_visitors_count_over_total_visitors_count
    type: number
    decimals: 2
    sql: ${visits_from_new_visitor_count}/NULLIF(${visits_count},0)::REAL

  - measure: returning_visitors_count_over_total_visitors_count
    type: number
    decimals: 2
    sql: ${visits_from_returning_visitor_count}/NULLIF(${visits_count},0)::REAL
  
  - measure: average_time_on_site
    type: average
    sql: EXTRACT(EPOCH FROM (${TABLE}.dvce_visit_finish_ts-${TABLE}.dvce_visit_start_ts))
    
  - measure: events_count
    type: sum
    sql: ${TABLE}.number_of_events
    
  - measure: events_per_visit
    type: int
    sql: ${events_count}/NULLIF(${visits_count},0)::REAL
    
  - measure: events_per_visitor
    type: int
    sql: ${events_count}/NULLIF(${visitors_count},0)::REAL

    
  # Detail #
  sets:
    detail:
      - domain_userid
      - domain_sessionidx
      - occurred_time
      - time_on_site
      - events_count
      - history