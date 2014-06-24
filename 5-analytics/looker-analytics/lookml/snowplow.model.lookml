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

- connection: snowplow

- scoping: true                  # for backward compatibility
- include: "*.view.lookml"       # include all the views
- include: "*.dashboard.lookml"  # include all the dashboards

- base_view: events  
  joins:
  - join: ad_clicks
    foreign_key: events.event_id
    join_type: one_to_one
  - join: ad_conversions
    foreign_key: events.event_id
    join_type: events.event_id
  - join: ad_impressions
    foreign_key: event_id
    join_type: one_to_one
  - join: link_click
    foreign_key: events.event_id
    join_type: one_to_one
  - join: screen_view
    foreign_key: events.event_id
    join_type: one_to_one
  - join: sessions
    sql_on: |
      events.domain_userid = sessions.domain_userid AND
      events.domain_sessionidx = sessions.domain_sessionidx
  - join: visitors
    sql_on: |
      events.domain_userid = visitors.domain_userid

- base_view: sessions
  joins: 
  - join: visitors
    sql_on: |
      sessions.domain_userid = visitors.domain_userid

- base_view: visitors

- base_view: transactions

- base_view: transaction_items
  joins:
  - join: transactions
    sql_foreign_key: transaction_items.ti_orderid
