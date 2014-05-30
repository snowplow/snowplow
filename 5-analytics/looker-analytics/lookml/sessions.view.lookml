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

- view: sessions
  derived_table:
    sql: |
      SELECT
        s.domain_userid,
        s.domain_sessionidx,
        s.session_start_ts,
        s.session_end_ts,
        s.number_of_events,
        s.distinct_pages_viewed,
        g.geo_country,
        g.geo_country_code_2_characters,
        g.geo_country_code_3_characters,
        g.geo_region,
        g.geo_city,
        g.geo_zipcode,
        g.geo_latitude,
        g.geo_longitude,
        l.page_urlhost AS landing_page_host,
        l.page_urlpath AS landing_page_path,
        l2.page_urlhost AS exit_page_host,
        l2.page_urlpath AS exit_page_path,
        s2.mkt_source,
        s2.mkt_medium,
        s2.mkt_term,
        s2.mkt_campaign,
        s2.refr_source,
        s2.refr_medium,
        s2.refr_term,
        s2.refr_urlhost,
        s2.refr_urlpath,
        t.br_name,
        t.br_family,
        t.br_version,
        t.br_type,
        t.br_renderengine,
        t.br_lang,
        t.br_features_director,
        t.br_features_flash,
        t.br_features_gears,
        t.br_features_java,
        t.br_features_pdf,
        t.br_features_quicktime,
        t.br_features_realplayer,
        t.br_features_silverlight,
        t.br_features_windowsmedia,
        t.br_cookies,
        t.os_name,
        t.os_family,
        t.os_manufacturer,
        t.os_timezone,
        t.dvce_type,
        t.dvce_ismobile,
        t.dvce_screenwidth,
        t.dvce_screenheight
      FROM ${sessions_basic.SQL_TABLE_NAME} AS s
      LEFT JOIN ${sessions_geo.SQL_TABLE_NAME} AS g
        ON s.domain_userid = g.domain_userid AND
        s.domain_sessionidx = g.domain_sessionidx
      LEFT JOIN ${sessions_landing_page.SQL_TABLE_NAME} AS l
        ON s.domain_userid = l.domain_userid AND
        s.domain_sessionidx = l.domain_sessionidx
      LEFT JOIN ${sessions_last_page.SQL_TABLE_NAME} AS l2
        ON s.domain_userid = l2.domain_userid AND
        s.domain_sessionidx = l2.domain_sessionidx
      LEFT JOIN ${sessions_source.SQL_TABLE_NAME} AS s2
        ON s.domain_userid = s2.domain_userid AND
        s.domain_sessionidx = s2.domain_sessionidx
      LEFT JOIN ${sessions_technology.SQL_TABLE_NAME} AS t
        ON s.domain_userid = t.domain_userid AND
        s.domain_sessionidx = t.domain_sessionidx
    
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_technology.SQL_TABLE_NAME}
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx, session_start_ts]
    
  fields:
  # DIMENSIONS #
  
  # Basic dimensions #
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: session_index
    type: int
    sql: ${TABLE}.domain_sessionidx
  
  - dimension: session_id
    sql: ${TABLE}.domain_userid || '-' || ${TABLE}.domain_sessionidx
  
  - dimension: session_index_tier
    type: tier
    tiers: [1,2,3,4,5,10,25,100,1000]
    sql: ${session_index}
  
  - dimension: start
    sql: ${TABLE}.session_start_ts
  
  - dimension_group: start
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.session_start_ts
    
  - dimension: end
    sql: ${TABLE}.session_end_ts
    
  # Session duration #

  - dimension: session_duration_seconds
    type: int
    sql: EXTRACT(EPOCH FROM (${TABLE}.session_end_ts - ${TABLE}.session_start_ts))

  - dimension: session_duration_seconds_tiered
    type: tier
    tiers: [0,1,5,10,30,60,300,900]
    sql: ${session_duration_seconds}

  # Events per visit and bounces (infered) #

  - dimension: events_during_session
    type: int
    sql: ${TABLE}.number_of_events
    
  - dimension: events_during_session_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000,10000]
    sql: ${TABLE}.number_of_events
    
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.number_of_events = 1
  
  # New vs returning visitor #
  - dimension: new_vs_returning_visitor
    sql_case:
      new: ${TABLE}.domain_sessionidx = 1
      returning: ${TABLE}.domain_sessionidx > 1
      else: unknown
    html: |
      <%= linked_value %>
      <a href="/dashboards/snowplow/traffic_pulse?new_vs_returning=<%= value %>" target="_new">
      <img src="/images/qr-graph-line@2x.png" height=20 width=20></a>

  # Pages visited #
  - dimension: distinct_pages_viewed
    sql: ${TABLE}.distinct_pages_viewed
    
  - dimension: distinct_pages_viewed_tiered
    type: tier
    tiers: [1,2,3,4,5,10,25,100,1000]
    sql: ${TABLE}.distinct_pages_viewed
  
  - dimension: event_stream
    sql: ${session_id}
    html: |
      <a href=events?fields=events.event_detail*&f[events.session_id]=<%= value%>>Event Stream</a>
  
  # Geo fields #
  
  - dimension: geography_country
    sql: ${TABLE}.geo_country
    html: |
      <%= linked_value %>
      <a href="/dashboards/snowplow/traffic_pulse?country=<%= value %>" target="_new">
      <img src="/images/qr-graph-line@2x.png" height=20 width=20></a>
    
  - dimension: geography_country_three_letter_iso_code
    sql: ${TABLE}.geo_country_code_3_characters
    
  - dimension: geography_country_two_letter_iso_code
    sql: ${TABLE}.geo_country_code_2_characters
  
  - dimension: geography_region
    sql: ${TABLE}.geo_region
    
  - dimension: geography_city
    sql: ${TABLE}.geo_city
    
  - dimension: geography_zipcode
    sql: ${TABLE}.geo_zipcode
    
  - dimension: geography_latitude
    sql: ${TABLE}.geo_latitude
  
  - dimension: geography_longitude
    sql: ${TABLE}.geo_longitude
    
  # Landing page
    
  - dimension: landing_page_host
    sql: ${TABLE}.landing_page_urlhost
    
  - dimension: landing_page_path
    sql: ${TABLE}.landing_page_path
    html: |
      <%= linked_value %>
      <a href="/dashboards/snowplow/traffic_pulse?landing_page=<%= value %>%25" target="_new">
      <img src="/images/qr-graph-line@2x.png" height=20 width=20></a>
    
  - dimension: landing_page
    sql: ${TABLE}.landing_page_host || ${TABLE}.landing_page_path
    
  # Exit page
  
  - dimension: exit_page_host
    sql: ${TABLE}.exit_page_host
    
  - dimension: exit_page_path
    sql: ${TABLE}.exit_page_path
    
  - dimension: exit_page
    sql: ${TABLE}.exit_page_host || ${TABLE}.exit_page_path
    
  # Marketing / traffic source fields
  
  - dimension: referer_medium
    sql_case:
      email: ${TABLE}.refr_medium = 'email'
      search: ${TABLE}.refr_medium = 'search'
      social: ${TABLE}.refr_medium = 'social'
      other_website: ${TABLE}.refr_medium = 'unknown'
      else: direct
    html: |
      <%= linked_value %>
      <a href="/dashboards/snowplow/traffic_pulse?referer_medium=<%= value %>" target="_new">
      <img src="/images/qr-graph-line@2x.png" height=20 width=20></a>
    
  - dimension: referer_source
    sql: ${TABLE}.refr_source
    
  - dimension: referer_term
    sql: ${TABLE}.refr_term
    
  - dimension: referer_url_host
    sql: ${TABLE}.refr_urlhost
  
  - dimension: referer_url_path
    sql: ${TABLE}.refr_urlpath
    
  # MKT fields (paid acquisition channels)
    
  - dimension: campaign_medium
    sql: ${TABLE}.mkt_medium
  
  - dimension: campaign_source
    sql: ${TABLE}.mkt_source
  
  - dimension: campaign_term
    sql: ${TABLE}.mkt_term
  
  - dimension: campaign_name
    sql: ${TABLE}.mkt_campaign

  # Device fields #
    
  - dimension: device_type
    sql: ${TABLE}.dvce_type
    
  - dimension: device_is_mobile
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
    
  - dimension: browser_version
    sql: ${TABLE}.br_version
    
  - dimension: browser_type
    sql: ${TABLE}.br_type
    
  - dimension: browser_renderengine
    sql: ${TABLE}.br_renderengine
    
  - dimension: browser_language
    sql: ${TABLE}.br_lang
    
  - dimension: browser_has_director_plugin
    sql: ${TABLE}.br_features_director
    
  - dimension: browser_has_flash_plugin
    sql: ${TABLE}.br_features_flash
    
  - dimension: browser_has_gears_plugin
    sql: ${TABLE}.br_features_gears
    
  - dimension: browser_has_java_plugin
    sql: ${TABLE}.br_features_java
    
  - dimension: browser_has_pdf_plugin
    sql: ${TABLE}.br_features_pdf
    
  - dimension: browser_has_quicktime_plugin
    sql: ${TABLE}.br_features_quicktime
    
  - dimension: browser_has_realplayer_plugin
    sql: ${TABLE}.br_features_realplayer
    
  - dimension: browser_has_silverlight_plugin
    sql: ${TABLE}.br_features_silverlight
    
  - dimension: browser_has_windowsmedia_plugin
    sql: ${TABLE}.br_features_windowsmedia
    
  - dimension: browser_supports_cookies
    sql: ${TABLE}.br_cookies
  
  # MEASURES #

  - measure: count
    type: count_distinct
    sql: ${session_id}
    detail: individual_detail*

  - measure: visitors_count
    type: count_distinct
    sql: ${user_id}
    detail: detail*
    hidden: true
    
  - measure: bounced_sessions_count
    type: count_distinct
    sql: ${session_id}
    filters:
      bounce: yes
    detail: detail*

  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_sessions_count}/NULLIF(${count},0)::REAL
  
  - measure: sessions_from_new_visitors_count
    type: count_distinct
    sql: ${session_id}
    filters:
      session_index: 1
    detail: individual_detail*
  
  - measure: sessions_from_returning_visitor_count
    type: number
    sql: ${count} - ${sessions_from_new_visitors_count}
    detail: individual_detail*
  
  - measure: new_visitors_count_over_total_visitors_count
    type: number
    decimals: 2
    sql: ${sessions_from_new_visitors_count}/NULLIF(${count},0)::REAL

  - measure: returning_visitors_count_over_total_visitors_count
    type: number
    decimals: 2
    sql: ${sessions_from_returning_visitor_count}/NULLIF(${count},0)::REAL
    
  - measure: events_count
    type: sum
    sql: ${TABLE}.number_of_events
    
  - measure: events_per_session
    type: number
    decimals: 2
    sql: ${events_count}/NULLIF(${count},0)::REAL
    
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${events_count}/NULLIF(${visitors_count},0)::REAL
    
  # Geo measures
  - measure: country_count
    type: count_distinct
    sql: ${geography_country}
    detail: 
    - geography_country
    - detail*
    
  - measure: region_count
    type: count_distinct
    sql: ${geography_region}
    detail: 
    - geography_country
    - geography_region
    - detail*
    
  - measure: city_count
    type: count_distinct
    sql: ${geography_city}
    detail: 
    - geography_country
    - geography_region
    - geography_city
    - detail*
      
  - measure: zip_code_count
    type: count_distinct
    sql: ${geography_zipcode}
    detail:  
    - geography_country
    - geography_region
    - geography_city
    - geography_zipcode
    - detail*
    
  - measure: campaign_medium_count
    type: count_distinct
    sql: ${campaign_medium}
    detail: 
    - campaign_medium
    - detail*
    
  - measure: campaign_source_count
    type: count_distinct
    sql: ${campaign_source}
    detail: 
    - campaign_medium
    - campaign_source
    - detail*
    
  - measure: campaign_term_count
    type: count_distinct
    sql: ${campaign_term}
    detail: 
    - campaign_medium
    - campaign_source
    - campaign_term 
    - detail*
      
  - measure: campaign_count
    type: count_distinct
    sql: ${campaign_name}
    detail: 
    - campaign_medium
    - campaign_source
    - campaign_term
    - campaign_name
    - detail*
    
  - measure: referer_medium_count
    type: count_distinct
    sql: ${referer_medium}
    detail: 
    - referer_medium
    - detail*
    
  - measure: referer_source_count
    type: count_distinct
    sql: ${referer_source}
    detail: 
    - referer_medium
    - referer_source
    - detail*
    
  - measure: referer_term_count
    type: count_distinct
    sql: ${referer_term}
    detail: 
    - referer_medium
    - referer_source
    - referer_term
    - detail*
    
  # Technology measures 
  
  - measure: device_count
    type: count_distinct
    sql: ${device_type}
    detail: detail*
  
  - measure: operating_system_count
    type: count_distinct
    sql: ${operating_system}
    detail: 
    - operating_system
    - detail*
  
  - measure: browser_count
    type: count_distinct
    sql: ${browser}
    detail: 
    - browser
    - detail*
    
  # Detail #
  sets:
  
    detail:
      - count
      - visitors.count
      - bounce_rate
      - sessions_from_new_visitors_count
      - sessions_from_returning_visitors_count
      - new_visitors_count_over_total_visitors_count
      - country_count
      - region_count
      - city_count
      - zipe_code_count
      - campaign_medium_count
      - campaign_source_count
      - campaign_term_count
      - campaign_count
      - referer_medium_count
      - referer_source_count
      - referer_term_count
      - device_count
      - operating_system_count
      - browser_count
  
    individual_detail:
      - user_id
      - session_index
      - visitors.first_touch_date
      - referer_medium
      - referer_source
      - referer_url_host
      - referer_url_path
      - campaign_medium
      - campaign_source
      - campaign_name
      - start_time
      - session_duration_seconds
      - events_during_session
      - distinct_pages_viewed
      - event_stream