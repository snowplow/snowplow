# Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
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
# Version: 3-0-0
#
# Authors: Yali Sassoon, Christophe Bogaert
# Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
# License: Apache License Version 2.0

- view: sessions_technology
  derived_table:
    sql: |
      SELECT
        *
      FROM (
        SELECT -- Select the last page (using dvce_tstamp)
          a.domain_userid,
          a.domain_sessionidx,
          a.br_name,
          a.br_family,
          a.br_version,
          a.br_type,
          a.br_renderengine,
          a.br_lang,
          a.br_features_director,
          a.br_features_flash,
          a.br_features_gears,
          a.br_features_java,
          a.br_features_pdf,
          a.br_features_quicktime,
          a.br_features_realplayer,
          a.br_features_silverlight,
          a.br_features_windowsmedia,
          a.br_cookies,
          a.os_name,
          a.os_family,
          a.os_manufacturer,
          a.os_timezone,
          a.dvce_type,
          a.dvce_ismobile,
          a.dvce_screenwidth,
          a.dvce_screenheight,
          RANK() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx
            ORDER BY a.br_name, a.br_family, a.br_version, a.br_type, a.br_renderengine, a.br_lang,
              a.br_features_director, a.br_features_flash, a.br_features_gears, a.br_features_java,
              a.br_features_pdf, a.br_features_quicktime, a.br_features_realplayer, a.br_features_silverlight,
              a.br_features_windowsmedia, a.br_cookies, a.os_name, a.os_family, a.os_manufacturer, a.os_timezone,
              a.dvce_type, a.dvce_ismobile, a.dvce_screenwidth, a.dvce_screenheight) AS rank
        FROM atomic.events AS a
        INNER JOIN ${sessions_basic.SQL_TABLE_NAME} AS b
          ON  a.domain_userid = b.domain_userid
          AND a.domain_sessionidx = b.domain_sessionidx
          AND a.dvce_tstamp = b.dvce_min_tstamp
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26 -- Aggregate identital rows
      )
      WHERE rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
    
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_source.SQL_TABLE_NAME} # Generate this table after sessions_source
    
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx]
    
  fields:
  
  # DIMENSIONS #
  
  # Basic dimensions #
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: session_index
    type: int
    sql: ${TABLE}.domain_sessionidx
    
  # Device fields #
    
  - dimension: device_type
    sql: ${TABLE}.dvce_type
    
  - dimension: device_is_mobile
    type: yesno
    sql: ${TABLE}.dvce_ismobile
    
  - dimension: device_screen_width
    sql: ${TABLE}.dvce_screenwidth
    
  - dimension: device_screen_height
    sql: ${TABLE}.dvce_screenheight
    
  # OS fields #
    
  - dimension: operating_system
    sql: ${TABLE}.os_name
    
  - dimension: operating_system_family
    sql: ${TABLE}.os_family
    
  - dimension: operating_system_manufacturer
    sql: ${TABLE}.os_manufacturer
    
  # Browser fields #
  
  - dimension: browser
    sql: ${TABLE}.br_name
  
  - dimension: browser_family
    sql: ${TABLE}.br_family

  - dimension: browser_version
    sql: ${TABLE}.br_version
    
  - dimension: browser_type
    sql: ${TABLE}.br_type
    
  - dimension: browser_renderengine
    sql: ${TABLE}.br_renderengine
    
  - dimension: browser_language
    sql: ${TABLE}.br_lang
    
  - dimension: browser_has_director_plugin
    type: yesno
    sql: ${TABLE}.br_features_director
    
  - dimension: browser_has_flash_plugin
    type: yesno
    sql: ${TABLE}.br_features_flash
    
  - dimension: browser_has_gears_plugin
    type: yesno
    sql: ${TABLE}.br_features_gears
    
  - dimension: browser_has_java_plugin
    type: yesno
    sql: ${TABLE}.br_features_java
    
  - dimension: browser_has_pdf_plugin
    type: yesno
    sql: ${TABLE}.br_features_pdf
    
  - dimension: browser_has_quicktime_plugin
    type: yesno
    sql: ${TABLE}.br_features_quicktime
    
  - dimension: browser_has_realplayer_plugin
    type: yesno
    sql: ${TABLE}.br_features_realplayer
    
  - dimension: browser_has_silverlight_plugin
    type: yesno
    sql: ${TABLE}.br_features_silverlight
    
  - dimension: browser_has_windowsmedia_plugin
    type: yesno
    sql: ${TABLE}.br_features_windowsmedia
    
  - dimension: browser_supports_cookies
    type: yesno
    sql: ${TABLE}.br_cookies
    
  # MEASURES #
  
  - measure: device_count
    type: count_distinct
    sql: ${device_type}
    drill_fields: detail*
  
  - measure: operating_system_count
    type: count_distinct
    sql: ${operating_system}
    drill_fields: detail*
  
  - measure: browser_count
    type: count_distinct
    sql: ${browser}
    drill_fields: detail*
    
  - measure: count
    type: count
