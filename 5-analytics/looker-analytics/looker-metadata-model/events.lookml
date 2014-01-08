- view: events
  sql_table_name: atomic.events
  fields:

# DIMENSIONS # 

  - dimension: event_id
    primary_key: true
    sql: ${TABLE}.event_id
  
  - dimension: event_type
    sql: ${TABLE}.event

  - dimension_group: occurred
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.collector_tstamp

  - dimension: domain_sessionidx
    type: number
    sql: ${TABLE}.domain_sessionidx

  - dimension: domain_userid
    sql: ${TABLE}.domain_userid

  - dimension: visit_id
    sql: ${TABLE}.domain_userid || '-' || ${TABLE}.domain_sessionidx

  - dimension_group: dvce_tstamp
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.dvce_tstamp
    hidden: true

  - dimension: page_title
    sql: ${TABLE}.page_title

  - dimension: page_urlscheme
    sql: ${TABLE}.page_urlscheme

  - dimension: page_urlhost
    sql: ${TABLE}.page_urlhost
      
  - dimension: page_urlpath
    sql: ${TABLE}.page_urlpath

  - dimension: page_urlport
    type: int
    sql: ${TABLE}.page_urlport
    hidden: true

  - dimension: page_urlquery
    sql: ${TABLE}.page_urlquery
    hidden: true

  - dimension: page_urlfragment
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
    
  - dimension: refr_urlhost
    sql: ${TABLE}.refr_urlhost
    
  - dimension: refr_urlpath
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

  - dimension: se_action
    sql: ${TABLE}.se_action

  - dimension: se_category
    sql: ${TABLE}.se_category

  - dimension: se_label
    sql: ${TABLE}.se_label

  - dimension: se_property
    sql: ${TABLE}.se_property

  - dimension: se_value
    type: number
    sql: ${TABLE}.se_value
    
  - dimension: tr_orderid
    sql: ${TABLE}.tr_orderid

  - dimension: useragent
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

  - measure: events_count
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
    sql: ${page_urlpath}

  - measure: average_page_pings_per_visit
    type: number
    decimals: 2
    sql: NULLIF(${page_pings_count},0)/NULLIF(${visits_count},0)::REAL

  - measure: average_page_pings_per_visitor
    type: number
    decimals: 2
    sql: NULLIF(${page_pings_count},0)/NULLIF(${visitors_count},0)::REAL
      
  - measure: transactions_per_visit
    type: number
    decimals: 3
    sql: NULLIF(${transactions_count},0)/NULLIF(${visits_count},0)::REAL
    
  - measure: transactions_per_visitor
    type: number
    decimals: 2
    sql: NULLIF(${transactions_count},0)/NULLIF(${visitors_count},0)::REAL
  - measure: visitors_count
    type: count_distinct
    sql: ${domain_userid}
    detail: visitors_detail*

  - measure: visits_count
    type: count_distinct
    sql: ${visit_id}
    detail: visit_detail*
    
  - measure: visits_per_visitor
    type: number
    decimals: 2
    sql: ${visits_count}/NULLIF(${visitors_count},0)::REAL
    
  - measure: events_per_visit
    type: number
    decimals: 2
    sql: ${events_count}/NULLIF(${visits_count},0)::REAL
    
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${events_count}/NULLIF(${visitors_count},0)::REAL
    
  - measure: page_pings_per_visit
    type: number
    decimals: 2
    sql: ${page_pings_count}/NULLIF(${visits_count},0)::REAL

  - measure: page_views_per_visit
    type: number
    decimals: 2
    sql: ${page_views_count}/NULLIF(${visits_count},0)::REAL
    
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
    type: count_distinct
    sql : CONCAT(FLOOR(EXTRACT (EPOCH FROM ${TABLE}.collector_tstamp)/(60*2))*2 , ${TABLE}.domain_userid)
    
  - measure: approx_usage_per_visitor_in_minutes
    type: number
    decimals: 2
    sql: ${approx_user_usage_in_minutes}/NULLIF(${visitors_count}, 0)::REAL
    
  - measure: approx_usage_per_visit_in_minutes
    type: number
    decimals: 2
    sql: ${approx_user_usage_in_minutes}/NULLIF(${visits_count}, 0)::REAL

  - measure: transactions_value
    type: sum
    sql: ${transaction_value}
    filters:
      event_type: transaction

  # ----- Detail ------
  sets:
    event_detail:
      - event_type
      - occurred_time
      - dvce_tstamp
      - page_urlhost
      - page_urlpath
    
    page_views_detail:
      - page_urlhost
      - page_urlpath
      - occurred_time
      - dvce_tstamp
    
    visit_detail:
      - domain_userid
      - domain_sessionidx
      - visits.occurred_time
      - visits.time_on_site
      - approx_user_usage_in_minutes
      - events_count
      - visits.distinct_pages_viewed_count
      - transactions_count
      - visits.history    
    
    visitors_detail:
      - domain_userid
      - visitors.first_touch_time
      - visitors.last_touch_time
      - visits_count
      - events_count
      - page_views_count
      - distinct_pages_count
      - transactions_count
      - visitors.history
    
    transaction_detail:
      - transaction_order_id
      - occurred_time
      - domain_userid
      - domain_sessionidx
      - transaction_value
      - transaction_address_city
      - transaction_address_state
      - transaction_address_country
      - transaction_items_list
    
    transaction_items_detail:
      - transaction_item_order_id
      - occurred_time
      - transaction_item_sku
      - transaction_item_name
      - transaction_item_price
      - transaction_item_quantity