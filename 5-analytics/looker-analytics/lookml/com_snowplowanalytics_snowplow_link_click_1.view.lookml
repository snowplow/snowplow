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
# Author(s): Yali Sassoon
# Copyright: Copyright (c) 2013-2014 Snowplow Analytics Ltd
# License: Apache License Version 2.0
#
# Compatibility: iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0

- view: link_clicks
  sql_table_name: atomic.com_snowplowanalytics_snowplow_link_click_1
  fields:

# DIMENSIONS #

  - dimension: event_id
    primary_key: true
    sql: ${TABLE}.root_id

  - dimension: timestamp
    sql: ${TABLE}.root_tstamp

  - dimension_group: timestamp
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.root_tstamp

  - dimension: link_id
    sql: ${TABLE}.element_id

  - dimension: link_classes
    sql: ${TABLE}.element_classes

  - dimension: link_target
    sql: ${TABLE}.element_target

  - dimension: target_url
    sql: ${TABLE}.target_url


# MEASURES #
  - measure: count
    type: count_distinct
    sql: ${event_id}