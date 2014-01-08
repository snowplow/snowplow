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

- view: landing_page_original
  derived_table:
    sql: |
       SELECT
        domain_userid,
        MAX(page_urlhost) AS page_urlhost, -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
        MAX(page_urlpath) AS page_urlpath  -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
        FROM (
          SELECT
          domain_userid,
          page_urlhost,
          page_urlpath,
          RANK() OVER (PARTITION BY domain_userid ORDER BY dvce_tstamp) AS "rank_asc"
          FROM atomic.events
          WHERE event = 'page_view') AS a
        WHERE rank_asc = 1                -- Filter so only return landing pages
        GROUP BY 1                      -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
      

    persist_for: 3 hours

  fields:
    
  # DIMENSIONS #
  
  - dimension: domain_userid
    sql: ${TABLE}.domain_userid
    primary_key: true
    hidden: true
    
  - dimension: host
    sql: ${TABLE}.page_urlhost
    
  - dimension: path
    sql: ${TABLE}.page_urlpath
    
  - dimension: landing_page
    sql: ${TABLE}.page_urlhost || ${TABLE}.page_urlpath

  # MEASURES # 
  
  - measure: landing_page_count
    type: count_distinct
    sql: ${landing_page}
    detail: landing_page_detail*    
    
  # Detail #
  sets:
    landing_page_detail:
      - landing_page
      - visitors.visitors_count
      - visitors.average_events_per_visitor
      - visitors.bounce_rate