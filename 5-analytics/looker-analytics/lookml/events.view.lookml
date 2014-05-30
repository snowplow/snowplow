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

- view: events
  derived_table:
    sql: |
      SELECT
      domain_userid, 
      domain_sessionidx,
      event_id,
      event,
      collector_tstamp,
      dvce_tstamp,
      page_title,
      page_urlscheme,
      page_urlhost,
      page_urlpath,
      page_urlport,
      page_urlquery,
      page_urlfragment,
      refr_medium,
      refr_source,
      refr_term,
      refr_urlhost,
      refr_urlpath,
      pp_xoffset_max,
      pp_xoffset_min,
      pp_yoffset_max,
      pp_yoffset_min,
      se_category,
      se_action,
      se_label,
      se_property,
      se_value,
      doc_height,
      doc_width,
      tr_orderid,
      tr_affiliation,
      tr_total,
      tr_tax,
      tr_city,
      tr_state,
      tr_country,
      ti_orderid,
      ti_category,
      ti_sku,
      ti_name,
      ti_price,
      ti_quantity
      FROM atomic.events
      WHERE domain_userid IS NOT NULL
    
    sql_trigger_value: SELECT MAX(collector_tstamp) FROM atomic.events
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx, collector_tstamp]
    
  fields:
  # DIMENSIONS #
  
  - dimension: event_id
    primary_key: true
    sql: ${TABLE}.event_id
  
  - dimension: event_type
    sql: ${TABLE}.event
    
  - dimension: timestamp
    sql: ${TABLE}.collector_tstamp

  - dimension_group: timestamp
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.collector_tstamp

  - dimension: session_index
    type: number
    sql: ${TABLE}.domain_sessionidx

  - dimension: user_id
    sql: ${TABLE}.domain_userid

  - dimension: session_id
    sql: ${TABLE}.domain_userid || '-' || ${TABLE}.domain_sessionidx

  - dimension_group: device_timestamp
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.dvce_tstamp
    hidden: true

  - dimension: page_title
    sql: ${TABLE}.page_title

  - dimension: page_url_scheme
    sql: ${TABLE}.page_urlscheme

  - dimension: page_url_host
    sql: ${TABLE}.page_urlhost
      
  - dimension: page_url_path
    sql: ${TABLE}.page_urlpath

  - dimension: page_url_port
    type: int
    sql: ${TABLE}.page_urlport
    hidden: true

  - dimension: page_url_query
    sql: ${TABLE}.page_urlquery
    hidden: true

  - dimension: page_url_fragment
    sql: ${TABLE}.page_urlfragment
    hidden: true
    
  - dimension: refr_medium
    sql: ${TABLE}.refr_medium
    
  - dimension: first_event_in_session
    type: yesno
    sql: ${TABLE}.refr_medium != 'internal'
    
  - dimension: first_event_for_visitor
    type: yesno
    sql: ${TABLE}.refr_medium != 'internal' AND ${TABLE}.domain_sessionidx = 1
    
  - dimension: refr_source
    sql: ${TABLE}.refr_source
    
  - dimension: refr_term
    sql: ${TABLE}.refr_term
    
  - dimension: refr_url_host
    sql: ${TABLE}.refr_urlhost
    
  - dimension: refr_url_path
    sql: ${TABLE}.refr_urlpath

  - dimension: x_offset
    type: int
    sql: ${TABLE}.pp_xoffset_max

  - dimension: pp_xoffset_min
    type: int
    sql: ${TABLE}.pp_xoffset_min
    hidden: true

  - dimension: y_offset
    type: int
    sql: ${TABLE}.pp_yoffset_max

  - dimension: pp_yoffset_min
    type: int
    sql: ${TABLE}.pp_yoffset_min
    hidden: true

  - dimension: structured_event_action
    sql: ${TABLE}.se_action

  - dimension: structured_event_category
    sql: ${TABLE}.se_category

  - dimension: structured_event_label
    sql: ${TABLE}.se_label

  - dimension: structured_event_property
    sql: ${TABLE}.se_property

  - dimension: structured_event_value
    type: number
    sql: ${TABLE}.se_value
    
  - dimension: user_agent
    sql: ${TABLE}.useragent
    hidden: true
    
  - dimension: page_height
    type: int
    sql: ${TABLE}.doc_height
    
  - dimension: page_width
    type: int
    sql: ${TABLE}.doc_width
    
  # Transaction fields #
  
  - dimension: transaction_order_id
    sql: ${TABLE}.tr_orderid
    
  - dimension: transaction_affiliation
    sql: ${TABLE}.tr_affiliation
    
  - dimension: transaction_value
    type: number
    decimals: 2
    sql: ${TABLE}.tr_total
    
  - dimension: transaction_tax
    type: number
    decimals: 2
    sql: ${TABLE}.tr_tax
    
  - dimension: transaction_address_city
    sql: ${TABLE}.tr_city
    
  - dimension: transaction_address_state
    sql: ${TABLE}.tr_state
    
  - dimension: transaction_address_country
    sql: ${TABLE}.tr_country
  
  - dimension: transaction_item_order_id
    sql: ${TABLE}.ti_orderid
    
  - dimension: transaction_item_category
    sql: ${TABLE}.ti_category
    
  - dimension: transaction_item_sku
    sql: ${TABLE}.ti_sku
    
  - dimension: transaction_item_name
    sql: ${TABLE}.ti_name
    
  - dimension: transaction_item_price
    type: number
    decimals: 2
    sql: ${TABLE}.ti_price
    
  - dimension: transaction_item_quantity
    type: int
    sql: ${TABLE}.ti_quantity
    
  - dimension: transaction_items_list
    sql: ${transaction_order_id}
    html: |
      <a href=events?fields=events.transaction_items_detail*&f[events.transaction_item_order_id]=<%= value%>>Transaction Items</a>

# MEASURES #

  - measure: count
    type: count
    detail: event_detail*

  - measure: page_pings_count
    type: count
    detail: event_detail*
    filters:
      event_type: page_ping
      
  - measure: page_views_count
    type: count
    detail: page_views_detail*
    filters:
      event_type: page_view
      
  - measure: transactions_count
    type: count_distinct
    sql: ${transaction_order_id}
    filters:
      event_type: transaction
    detail: transaction_detail*

  - measure: distinct_pages_viewed_count
    type: count_distinct
    detail: page_views_detail*
    sql: ${page_url_path}
    
  - measure: sessions_count
    type: count_distinct
    sql: ${session_id}
    detail: detail*
    
  - measure: page_pings_per_session
    type: number
    decimals: 2
    sql: NULLIF(${page_pings_count},0)/NULLIF(${sessions_count},0)::REAL

  - measure: page_pings_per_visitor
    type: number
    decimals: 2
    sql: NULLIF(${page_pings_count},0)/NULLIF(${visitors_count},0)::REAL
      
  - measure: transactions_per_session
    type: number
    decimals: 3
    sql: NULLIF(${transactions_count},0)/NULLIF(${sessions_count},0)::REAL
    
  - measure: transactions_per_visitor
    type: number
    decimals: 2
    sql: NULLIF(${transactions_count},0)/NULLIF(${visitors_count},0)::REAL

  - measure: visitors_count
    type: count_distinct
    sql: ${user_id}
    detail: visitors_detail
    hidden: true  # Not to be shown in the UI (in UI only show visitors count for visitors table)
    
  - measure: events_per_session
    type: number
    decimals: 2
    sql: ${count}/NULLIF(${sessions_count},0)::REAL
    
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${count}/NULLIF(${visitors_count},0)::REAL

  - measure: page_views_per_sessions
    type: number
    decimals: 2
    sql: ${page_views_count}/NULLIF(${sessions_count},0)::REAL
    
  - measure: page_views_per_visitor
    type: number
    decimals: 2
    sql: ${page_views_count}/NULLIF(${visitors_count},0)::REAL
  
  - measure: landing_page_views_count
    type: count
    filters:
      event_type: page_view
      first_event_in_session: yes
    
  - measure: internal_page_views_count
    type: count
    filters: 
      event_type: page_view
      first_event_in_session: no
      
  - measure: approx_user_usage_in_minutes
    type: number
    decimals: 2
    sql: APPROXIMATE COUNT( DISTINCT CONCAT(FLOOR(EXTRACT (EPOCH FROM ${TABLE}.collector_tstamp)/10), ${TABLE}.domain_userid) ) /6
    
  - measure: approx_usage_per_visitor_in_seconds
    type: number
    decimals: 2
    sql: ${approx_user_usage_in_minutes}/NULLIF(${visitors_count}, 0)::REAL
    
  - measure: approx_usage_per_visit_in_seconds
    type: number
    decimals: 2
    sql: ${approx_user_usage_in_minutes}/NULLIF(${sessions_count}, 0)::REAL

  - measure: transactions_value
    type: sum
    sql: ${transaction_value}
    filters:
      event_type: transaction
  
  
  # ----- Detail ------
  sets:
    event_detail:
      - session_index
      - event_type
      - timestamp_time
      - device_timestamp
      - page_url_host
      - page_url_path
    
    page_views_detail:
      - page_url_host
      - page_url_path
      - timestamp_time
      - device_timestamp
    
    visit_detail:
      - user_id
      - domain_session_index
      - sessions.timestamp_time
      - source.refr_url_host
      - source.refr_url_path
      - landing_page.landing_page
      - sessions.visit_duration_seconds
      - count
      - sessions.distinct_pages_viewed
      - transactions_count
      - sessions.history 
      
    visitors_detail:
      - user_id
      - visitors.first_touch_time
      - source_first_visit.refr_url_host
      - source_first_visit.refr_url_path
      - landing_page_first_visit.page_url_path
      - visitors.last_touch_time
      - visitors.lifetime_sessions
      - visitors.visit_history
      - visitors.lifetime_distinct_pages_viewed
      - visitors.lifetime_events
      - visitors.event_history
      - transactions_count
      - visitors.history

    transaction_detail:
      - transaction_order_id
      - timestamp_time
      - user_id
      - domain_session_index
      - transaction_value
      - transaction_address_city
      - transaction_address_state
      - transaction_address_country
      - transaction_items_list
    
    transaction_items_detail:
      - transaction_item_order_id
      - timestamp_time
      - transaction_item_sku
      - transaction_item_name
      - transaction_item_price
      - transaction_item_quantity