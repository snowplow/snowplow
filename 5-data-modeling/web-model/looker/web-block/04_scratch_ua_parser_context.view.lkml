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

view: scratch_pv_04 {
  derived_table: {
    sql: WITH prep AS (

        -- deduplicate the UA parser context in 2 steps

        SELECT

          wp.page_view_id,

          ua.useragent_family,
          ua.useragent_major,
          ua.useragent_minor,
          ua.useragent_patch,
          ua.useragent_version,
          ua.os_family,
          ua.os_major,
          ua.os_minor,
          ua.os_patch,
          ua.os_patch_minor,
          ua.os_version,
          ua.device_family

        FROM atomic.com_snowplowanalytics_snowplow_ua_parser_context_1 AS ua

        INNER JOIN ${scratch_pv_00.SQL_TABLE_NAME} AS wp
          ON ua.root_id = wp.root_id

        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

      )

      SELECT * FROM prep WHERE page_view_id NOT IN (SELECT page_view_id FROM prep GROUP BY 1 HAVING COUNT(*) > 1) -- exclude all root ID with more than one page view ID
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${scratch_pv_03.SQL_TABLE_NAME} ;;
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
