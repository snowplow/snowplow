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

view: scratch_pv_05 {
  derived_table: {
    sql: WITH prep AS (

        SELECT

          wp.page_view_id,

          pt.navigation_start,
          pt.redirect_start,
          pt.redirect_end,
          pt.fetch_start,
          pt.domain_lookup_start,
          pt.domain_lookup_end,
          pt.secure_connection_start,
          pt.connect_start,
          pt.connect_end,
          pt.request_start,
          pt.response_start,
          pt.response_end,
          pt.unload_event_start,
          pt.unload_event_end,
          pt.dom_loading,
          pt.dom_interactive,
          pt.dom_content_loaded_event_start,
          pt.dom_content_loaded_event_end,
          pt.dom_complete,
          pt.load_event_start,
          pt.load_event_end

        FROM atomic.org_w3_performance_timing_1 AS pt

        INNER JOIN ${scratch_pv_00.SQL_TABLE_NAME} AS wp
          ON pt.root_id = wp.root_id

        -- all values should be set and some have to be greater than 0 (not the case in about 1% of events)

        WHERE pt.navigation_start IS NOT NULL AND pt.navigation_start > 0
          AND pt.redirect_start IS NOT NULL -- zero is acceptable
          AND pt.redirect_end IS NOT NULL -- zero is acceptable
          AND pt.fetch_start IS NOT NULL AND pt.fetch_start > 0
          AND pt.domain_lookup_start IS NOT NULL AND pt.domain_lookup_start > 0
          AND pt.domain_lookup_end IS NOT NULL AND pt.domain_lookup_end > 0
          AND pt.secure_connection_start IS NOT NULL AND pt.secure_connection_start > 0
          -- connect_start is either 0 or NULL
          AND pt.connect_end IS NOT NULL AND pt.connect_end > 0
          AND pt.request_start IS NOT NULL AND pt.request_start > 0
          AND pt.response_start IS NOT NULL AND pt.response_start > 0
          AND pt.response_end IS NOT NULL AND pt.response_end > 0
          AND pt.unload_event_start IS NOT NULL -- zero is acceptable
          AND pt.unload_event_end IS NOT NULL -- zero is acceptable
          AND pt.dom_loading IS NOT NULL AND pt.dom_loading > 0
          AND pt.dom_interactive IS NOT NULL AND pt.dom_interactive > 0
          AND pt.dom_content_loaded_event_start IS NOT NULL AND pt.dom_content_loaded_event_start > 0
          AND pt.dom_content_loaded_event_end IS NOT NULL AND pt.dom_content_loaded_event_end > 0
          AND pt.dom_complete IS NOT NULL -- zero is acceptable
          AND pt.load_event_start IS NOT NULL -- zero is acceptable
          AND pt.load_event_end IS NOT NULL -- zero is acceptable

          -- remove rare outliers (Unix timestamp is more than twice what it should be)

          AND DATEDIFF(d, pt.root_tstamp, (TIMESTAMP 'epoch' + pt.response_end/1000 * INTERVAL '1 second ')) < 365
          AND DATEDIFF(d, pt.root_tstamp, (TIMESTAMP 'epoch' + pt.unload_event_start/1000 * INTERVAL '1 second ')) < 365
          AND DATEDIFF(d, pt.root_tstamp, (TIMESTAMP 'epoch' + pt.unload_event_end/1000 * INTERVAL '1 second ')) < 365

      ), rolledup AS (

        SELECT

          page_view_id,

          -- select the first non-zero value

          MIN(NULLIF(navigation_start, 0)) AS navigation_start,
          MIN(NULLIF(redirect_start, 0)) AS redirect_start,
          MIN(NULLIF(redirect_end, 0)) AS redirect_end,
          MIN(NULLIF(fetch_start, 0)) AS fetch_start,
          MIN(NULLIF(domain_lookup_start, 0)) AS domain_lookup_start,
          MIN(NULLIF(domain_lookup_end, 0)) AS domain_lookup_end,
          MIN(NULLIF(secure_connection_start, 0)) AS secure_connection_start,
          MIN(NULLIF(connect_start, 0)) AS connect_start,
          MIN(NULLIF(connect_end, 0)) AS connect_end,
          MIN(NULLIF(request_start, 0)) AS request_start,
          MIN(NULLIF(response_start, 0)) AS response_start,
          MIN(NULLIF(response_end, 0)) AS response_end,
          MIN(NULLIF(unload_event_start, 0)) AS unload_event_start,
          MIN(NULLIF(unload_event_end, 0)) AS unload_event_end,
          MIN(NULLIF(dom_loading, 0)) AS dom_loading,
          MIN(NULLIF(dom_interactive, 0)) AS dom_interactive,
          MIN(NULLIF(dom_content_loaded_event_start, 0)) AS dom_content_loaded_event_start,
          MIN(NULLIF(dom_content_loaded_event_end, 0)) AS dom_content_loaded_event_end,
          MIN(NULLIF(dom_complete, 0)) AS dom_complete,
          MIN(NULLIF(load_event_start, 0)) AS load_event_start,
          MIN(NULLIF(load_event_end, 0)) AS load_event_end

        FROM prep

        GROUP BY 1

      )

      SELECT

        page_view_id,

        CASE
          WHEN ((redirect_start IS NOT NULL) AND (redirect_end IS NOT NULL) AND (redirect_end >= redirect_start)) THEN (redirect_end - redirect_start)
          ELSE NULL
        END AS redirect_time_in_ms,

        CASE
          WHEN ((unload_event_start IS NOT NULL) AND (unload_event_end IS NOT NULL) AND (unload_event_end >= unload_event_start)) THEN (unload_event_end - unload_event_start)
          ELSE NULL
        END AS unload_time_in_ms,

        CASE
          WHEN ((fetch_start IS NOT NULL) AND (domain_lookup_start IS NOT NULL) AND (domain_lookup_start >= fetch_start)) THEN (domain_lookup_start - fetch_start)
          ELSE NULL
        END AS app_cache_time_in_ms,

        CASE
          WHEN ((domain_lookup_start IS NOT NULL) AND (domain_lookup_end IS NOT NULL) AND (domain_lookup_end >= domain_lookup_start)) THEN (domain_lookup_end - domain_lookup_start)
          ELSE NULL
        END AS dns_time_in_ms,

        CASE
          WHEN ((connect_start IS NOT NULL) AND (connect_end IS NOT NULL) AND (connect_end >= connect_start)) THEN (connect_end - connect_start)
          ELSE NULL
        END AS tcp_time_in_ms,

        CASE
          WHEN ((request_start IS NOT NULL) AND (response_start IS NOT NULL) AND (response_start >= request_start)) THEN (response_start - request_start)
          ELSE NULL
        END AS request_time_in_ms,

        CASE
          WHEN ((response_start IS NOT NULL) AND (response_end IS NOT NULL) AND (response_end >= response_start)) THEN (response_end - response_start)
          ELSE NULL
        END AS response_time_in_ms,

        CASE
          WHEN ((dom_loading IS NOT NULL) AND (dom_complete IS NOT NULL) AND (dom_complete >= dom_loading)) THEN (dom_complete - dom_loading)
          ELSE NULL
        END AS processing_time_in_ms,

        CASE
          WHEN ((dom_loading IS NOT NULL) AND (dom_interactive IS NOT NULL) AND (dom_interactive >= dom_loading)) THEN (dom_interactive - dom_loading)
          ELSE NULL
        END AS dom_loading_to_interactive_time_in_ms,

        CASE
          WHEN ((dom_interactive IS NOT NULL) AND (dom_complete IS NOT NULL) AND (dom_complete >= dom_interactive)) THEN (dom_complete - dom_interactive)
          ELSE NULL
        END AS dom_interactive_to_complete_time_in_ms,

        CASE
          WHEN ((load_event_start IS NOT NULL) AND (load_event_end IS NOT NULL) AND (load_event_end >= load_event_start)) THEN (load_event_end - load_event_start)
          ELSE NULL
        END AS onload_time_in_ms,

        CASE
          WHEN ((navigation_start IS NOT NULL) AND (load_event_end IS NOT NULL) AND (load_event_end >= navigation_start)) THEN (load_event_end - navigation_start)
          ELSE NULL
        END AS total_time_in_ms

      FROM rolledup
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${scratch_pv_04.SQL_TABLE_NAME} ;;
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
