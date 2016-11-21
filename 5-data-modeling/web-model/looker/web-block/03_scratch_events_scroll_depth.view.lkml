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

view: scratch_pv_03 {
  derived_table: {
    sql: WITH prep AS (

        -- calculate how far a user has scrolled

        SELECT

          wp.page_view_id,

          MAX(ev.doc_width) AS doc_width,
          MAX(ev.doc_height) AS doc_height,

          MAX(ev.br_viewwidth) AS br_viewwidth,
          MAX(ev.br_viewheight) AS br_viewheight,

          -- NVL replaces NULL with 0 (because the page view event does send an offset)
          -- GREATEST prevents outliers (negative offsets)
          -- LEAST also prevents outliers (offsets greater than the docwidth or docheight)

          LEAST(GREATEST(MIN(NVL(ev.pp_xoffset_min, 0)), 0), MAX(ev.doc_width)) AS hmin, -- should be zero
          LEAST(GREATEST(MAX(NVL(ev.pp_xoffset_max, 0)), 0), MAX(ev.doc_width)) AS hmax,

          LEAST(GREATEST(MIN(NVL(ev.pp_yoffset_min, 0)), 0), MAX(ev.doc_height)) AS vmin, -- should be zero (edge case: not zero because the pv event is missing - but these are not in scratch.dev_pv_01 so not an issue)
          LEAST(GREATEST(MAX(NVL(ev.pp_yoffset_max, 0)), 0), MAX(ev.doc_height)) AS vmax

        FROM atomic.events AS ev

        INNER JOIN ${scratch_pv_00.SQL_TABLE_NAME} AS wp
          ON  ev.event_id = wp.root_id

        WHERE ev.event_name IN ('page_view', 'page_ping')
          AND ev.doc_height > 0 -- exclude problematic (but rare) edge case
          AND ev.doc_width > 0 -- exclude problematic (but rare) edge case

        GROUP BY 1

      )

      SELECT

        page_view_id,

        doc_width,
        doc_height,

        br_viewwidth,
        br_viewheight,

        hmin,
        hmax,
        vmin,
        vmax, -- zero when a user hasn't scrolled

        ROUND(100*(GREATEST(hmin, 0)/doc_width::FLOAT)) AS relative_hmin, -- brackets matter: because hmin is of type INT, we need to divide before we multiply by 100 or we risk an overflow
        ROUND(100*(LEAST(hmax + br_viewwidth, doc_width)/doc_width::FLOAT)) AS relative_hmax,
        ROUND(100*(GREATEST(vmin, 0)/doc_height::FLOAT)) AS relative_vmin,
        ROUND(100*(LEAST(vmax + br_viewheight, doc_height)/doc_height::FLOAT)) AS relative_vmax -- not zero when a user hasn't scrolled because it includes the non-zero viewheight

      FROM prep
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${scratch_pv_02.SQL_TABLE_NAME} ;;
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
