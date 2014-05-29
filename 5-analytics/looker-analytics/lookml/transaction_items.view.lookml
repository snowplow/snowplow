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

- view: transaction_items
  derived_table: 
    sql: |
      SELECT 
        ti_orderid,
        ti_category,
        ti_sku,
        ti_name,
        ti_price,
        ti_quantity
      FROM atomic.events 
      WHERE event = 'transaction_item'
      GROUP BY 1,2,3,4,5,5,6
      ORDER BY ti_orderid
    
    sql_trigger_value: SELECT MAX(collector_tstamp) FROM atomic.events
    distkey: ti_orderid
    sortkeys: [ti_orderid]
       
    distkey: ti_orderid
    sortkeys: ti_orderid
    
  fields: 
    
  # DIMENSIONS #
    
  - dimension: order_id
    sql: ${TABLE}.ti_orderid
    
  - dimension: product_category
    sql: ${TABLE}.ti_category
    
  - dimension: sku
    sql: ${TABLE}.ti_sku
    
  - dimension: name
    sql: ${TABLE}.ti_name
    
  - dimension: price
    type: number
    decimals: 2
    sql: ${TABLE}.ti_price
    
  - dimension: quantity
    type: int
    sql: ${TABLE}.ti_quantity

  - dimension: order_items
    sql: ${order_id}
    html: |
      <a href=transaction_items?fields=transaction_items.transaction_items_detail*&f[order_id]=<%= value%>>Transaction Items</a>
    
    
  # MEASURES #
  
  - measure: distinct_skus_bought
    type: count_distinct
    sql: ${sku}
    
  - measure: items_bought_count
    type: sum
    sql: ${quantity}
    
  - measure: sales_value
    type: sum
    sql: ${price}
    
  - measure: orders_count
    type: count_distinct
    sql: ${order_id}
    detail: transaction_detail*
    
  - measure: items_per_order
    type: number
    decimals: 2
    sql: ${items_bought_count} / NULLIF(${orders_count},0)::REAL
    
  - measure: average_order_value
    type: number
    decimals: 2
    sql: ${sales_value} / NULLIF(${orders_count},0)::REAL
    
  - measure: count
    type: count
    
  # ----- Detail ------
  sets:
    transaction_detail:
      - transactions.order_id
      - transactions.occurred_time
      - transactions.domain_user_id
      - transactions.domain_session_index
      - transactions.transaction_value
      - transactions.city
      - transactions.state
      - transactions.country
      - order_items
    
    transaction_items_detail:
      - order_id
      - transactions.occurred_time
      - sku
      - name
      - price
      - quantity
    
    
    