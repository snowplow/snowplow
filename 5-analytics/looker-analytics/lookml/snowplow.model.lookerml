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

- connection: snowplow

- scoping: true           # for backward compatibility
- include: "*.lookml"     # include all the lookml files in the same directory as the model

- base_view: events  
  joins:
  - join: visits
    sql_on: |
      events.domain_userid = visits.domain_userid AND
      events.domain_sessionidx = visits.domain_sessionidx
  - join: source
    sql_on: | 
      events.domain_userid = source.domain_userid AND
      events.domain_sessionidx = source.domain_sessionidx
  - join: geo
    sql_on: | 
      events.domain_userid = geo.domain_userid AND
      events.domain_sessionidx = geo.domain_sessionidx
  - join: landing_page
    sql_on: | 
      events.domain_userid = landing_page.domain_userid AND
      events.domain_sessionidx = landing_page.domain_sessionidx
  - join: last_page
    sql_on: | 
      events.domain_userid = last_page.domain_userid AND
      events.domain_sessionidx = last_page.domain_sessionidx
  - join: visitors
    sql_foreign_key: events.domain_userid
  - join: source_original
    sql_foreign_key: events.domain_userid
  - join: landing_page_original
    sql_foreign_key: events.domain_userid
    
- base_view: visits
  joins:
  - join: visitors
    sql_foreign_key: visits.domain_userid
  - join: source
    sql_on: | 
      visits.domain_userid = source.domain_userid AND
      visits.domain_sessionidx = source.domain_sessionidx
  - join: geo
    sql_on: | 
      visits.domain_userid = geo.domain_userid AND
      visits.domain_sessionidx = geo.domain_sessionidx
  - join: landing_page
    sql_on: | 
      visits.domain_userid = landing_page.domain_userid AND
      visits.domain_sessionidx = landing_page.domain_sessionidx
  - join: last_page
    sql_on: | 
      visits.domain_userid = last_page.domain_userid AND
      visits.domain_sessionidx = last_page.domain_sessionidx
  - join: source_original
    sql_foreign_key: visits.domain_userid
  - join: landing_page_original
    sql_foreign_key: visits.domain_userid
  - join: transactions
    sql_on: |
      visits.domain_userid = transactions.domain_userid AND
      visits.domain_sessionidx = transactions.domain_sessionidx

- base_view: visitors
  joins:
  - join: source_original
    sql_foreign_key: visitors.domain_userid
  - join: landing_page_original
    sql_foreign_key: visitors.domain_userid
  - join: transactions
    sql_on: visitors.domain_userid = transactions.domain_userid
    
- base_view: transactions

- base_view: transaction_items
  joins:
  - join: transactions
    sql_foreign_key: transaction_items.ti_orderid
