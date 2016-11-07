# Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
# Authors:     Christophe Bogaert, Keane Robinson
# Copyright:   Copyright (c) 2016 Snowplow Analytics Ltd
# License:     Apache License Version 2.0

view: scratch_pv_00 {
  derived_table: {
    sql: WITH prep AS (

        -- deduplicate the web page context in 2 steps

        SELECT

          root_id,
          id AS page_view_id

        FROM atomic.com_snowplowanalytics_snowplow_web_page_1

        GROUP BY 1,2

      )

      SELECT * FROM prep WHERE root_id NOT IN (SELECT root_id FROM prep GROUP BY 1 HAVING COUNT(*) > 1) -- exclude all root ID with more than one page view ID
       ;;
    sql_trigger_value: SELECT MAX(collector_tstamp) FROM atomic.events ;;
    distribution: "page_view_id"
    sortkeys: ["page_view_id"]
  }

  # DIMENSIONS #

  dimension: root_id {
    type: string
    sql: ${TABLE}.root_id ;;
  }

  dimension: page_view_id {
    type: string
    sql: ${TABLE}.page_view_id ;;
  }

  # MEASURES #

  measure: count {
    type: count
  }
}
