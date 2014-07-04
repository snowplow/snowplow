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
# Compatibility: iglu:com.snowplowanalytics.snowplow/screen_view/jsonschema/1-0-0

- view: web_page
  sql_table_name: atomic.org_schema_web_page_1
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

  - dimension: bread_crumb
    sql: ${TABLE}.breadcrumb

  - dimension: genre
    sql: ${TABLE}.genre

  - dimension: author 
    sql: ${TABLE}.author

  - dimension: date_created
    sql: ${TABLE}.date_created

  - dimension_group: date_created
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.date_created

  - dimension: date_modified
    sql: ${TABLE}.date_modified

  - dimension_group: date_modified
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.date_modified

  - dimension: date_published
    sql: ${TABLE}.date_published

  - dimension_group: date_published
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.date_published

  - dimension: in_language
    sql: ${TABLE}.in_language

# MEASURES #
  
  - measure: author_count
    type: count_distinct
    sql: ${author}

  - measure: language_count
    type: count_distinct
    sql: ${in_language}
