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

view: scratch_pv_02 {
  derived_table: {
    sql: SELECT

        wp.page_view_id,

        MIN(ev.derived_tstamp) AS min_tstamp, -- requires the derived timestamp (JS tracker 2.6.0+ and Snowplow 71+)
        MAX(ev.derived_tstamp) AS max_tstamp, -- requires the derived timestamp (JS tracker 2.6.0+ and Snowplow 71+)

        SUM(CASE WHEN ev.event_name = 'page_view' THEN 1 ELSE 0 END) AS pv_count, -- for debugging
        SUM(CASE WHEN ev.event_name = 'page_ping' THEN 1 ELSE 0 END) AS pp_count, -- for debugging

        10 * COUNT(DISTINCT(FLOOR(EXTRACT(EPOCH FROM ev.derived_tstamp)/10))) - 10 AS time_engaged_in_s -- assumes 10 seconds between subsequent page pings

      FROM atomic.events AS ev

      INNER JOIN ${scratch_pv_00.SQL_TABLE_NAME} AS wp
        ON ev.event_id = wp.root_id

      WHERE ev.event_name IN ('page_view', 'page_ping')

      GROUP BY 1
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${scratch_pv_01.SQL_TABLE_NAME} ;;
    distribution: "page_view_id"
    sortkeys: ["page_view_id"]
  }

  # DIMENSIONS #

  dimension: page_view_id {
    type: string
    sql: ${TABLE}.page_view_id ;;
  }

  # MEASURES #

  measure: count {
    type: count
  }
}
