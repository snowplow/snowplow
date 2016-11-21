view: page_views {
  derived_table: {
    sql: SELECT

        -- user

        a.user_id AS user_custom_id,
        a.domain_userid AS user_snowplow_domain_id,
        a.network_userid AS user_snowplow_crossdomain_id,

        -- sesssion

        a.domain_sessionid AS session_id,
        a.domain_sessionidx AS session_index,

        -- page view

        a.page_view_id,

        ROW_NUMBER() OVER (PARTITION BY a.domain_userid ORDER BY b.min_tstamp) AS page_view_index,
        ROW_NUMBER() OVER (PARTITION BY a.domain_sessionid ORDER BY b.min_tstamp) AS page_view_in_session_index,

        -- page view: time

        b.min_tstamp AS page_view_start, -- different from canonical SQL
        b.max_tstamp AS page_view_end, -- different from canonical SQL

          -- example derived dimensions

          -- TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD HH24:MI:SS') AS page_view_time,
          -- TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD HH24:MI') AS page_view_minute,
          -- TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD HH24') AS page_view_hour,
          -- TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD') AS page_view_date,
          -- TO_CHAR(DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp)), 'YYYY-MM-DD') AS page_view_week,
          -- TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM') AS page_view_month,
          -- TO_CHAR(DATE_TRUNC('quarter', CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp)), 'YYYY-MM') AS page_view_quarter,
          -- DATE_PART(Y, CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp))::INTEGER AS page_view_year,

        -- page view: time in the user's local timezone

        CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp) AS page_view_start_local,
        CONVERT_TIMEZONE('UTC', a.os_timezone, b.max_tstamp) AS page_view_end_local,

          -- example derived dimensions

          -- TO_CHAR(CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp), 'YYYY-MM-DD HH24:MI:SS') AS page_view_local_time,
          -- TO_CHAR(CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp), 'HH24:MI') AS page_view_local_time_of_day,
          -- DATE_PART(hour, CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp))::INTEGER AS page_view_local_hour_of_day,
          -- TRIM(TO_CHAR(CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp), 'd')) AS page_view_local_day_of_week,
          -- MOD(EXTRACT(DOW FROM CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp))::INTEGER - 1 + 7, 7) AS page_view_local_day_of_week_index,

        -- engagement

        b.time_engaged_in_s,

          -- CASE
            -- WHEN b.time_engaged_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
            -- WHEN b.time_engaged_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
            -- WHEN b.time_engaged_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
            -- WHEN b.time_engaged_in_s > 59 THEN '60s or more'
            -- ELSE NULL
          -- END AS time_engaged_in_s_tier,

        c.hmax AS horizontal_pixels_scrolled,
        c.vmax AS vertical_pixels_scrolled,

        c.relative_hmax AS horizontal_percentage_scrolled,
        c.relative_vmax AS vertical_percentage_scrolled,

          -- CASE
            -- WHEN c.relative_vmax BETWEEN 0 AND 24 THEN '0% to 24%'
            -- WHEN c.relative_vmax BETWEEN 25 AND 49 THEN '25% to 49%'
            -- WHEN c.relative_vmax BETWEEN 50 AND 74 THEN '50% to 74%'
            -- WHEN c.relative_vmax BETWEEN 75 AND 100 THEN '75% to 100%'
            -- ELSE NULL
          -- END AS vertical_percentage_scrolled_tier,

          -- CASE WHEN b.time_engaged_in_s = 0 THEN TRUE ELSE FALSE END AS user_bounced,
          -- CASE WHEN b.time_engaged_in_s >= 30 AND c.relative_vmax >= 25 THEN TRUE ELSE FALSE END AS user_engaged,

        -- page

        a.page_urlhost || a.page_urlpath AS page_url,

        a.page_urlscheme AS page_url_scheme,
        a.page_urlhost AS page_url_host,
        a.page_urlport AS page_url_port,
        a.page_urlpath AS page_url_path,
        a.page_urlquery AS page_url_query,
        a.page_urlfragment AS page_url_fragment,

        a.page_title,

        c.doc_width AS page_width,
        c.doc_height AS page_height,

        -- referer

        a.refr_urlhost || a.refr_urlpath AS referer_url,

        a.refr_urlscheme AS referer_url_scheme,
        a.refr_urlhost AS referer_url_host,
        a.refr_urlport AS referer_url_port,
        a.refr_urlpath AS referer_url_path,
        a.refr_urlquery AS referer_url_query,
        a.refr_urlfragment AS referer_url_fragment,

        CASE
          WHEN a.refr_medium IS NULL THEN 'direct'
          WHEN a.refr_medium = 'unknown' THEN 'other'
          ELSE a.refr_medium
        END AS referer_medium,
        a.refr_source AS referer_source,
        a.refr_term AS referer_term,

        -- marketing

        a.mkt_medium AS marketing_medium,
        a.mkt_source AS marketing_source,
        a.mkt_term AS marketing_term,
        a.mkt_content AS marketing_content,
        a.mkt_campaign AS marketing_campaign,
        a.mkt_clickid AS marketing_click_id,
        a.mkt_network AS marketing_network,

        -- location

        a.geo_country,
        a.geo_region,
        a.geo_region_name,
        a.geo_city,
        a.geo_zipcode,
        a.geo_latitude,
        a.geo_longitude,
        a.geo_timezone, -- often NULL (use os_timezone instead)

        -- IP

        a.user_ipaddress AS ip_address,

        a.ip_isp,
        a.ip_organization,
        a.ip_domain,
        a.ip_netspeed AS ip_net_speed,

        -- application

        a.app_id,

        -- browser

        d.useragent_version AS browser,
        d.useragent_family AS browser_name,
        d.useragent_major AS browser_major_version,
        d.useragent_minor AS browser_minor_version,
        d.useragent_patch AS browser_build_version,
        a.br_renderengine AS browser_engine,

        c.br_viewwidth AS browser_window_width,
        c.br_viewheight AS browser_window_height,

        a.br_lang AS browser_language,

        -- OS

        d.os_version AS os,
        d.os_family AS os_name,
        d.os_major AS os_major_version,
        d.os_minor AS os_minor_version,
        d.os_patch AS os_build_version,
        a.os_manufacturer,
        a.os_timezone,

        -- device

        d.device_family AS device,
        a.dvce_type AS device_type,
        a.dvce_ismobile AS device_is_mobile,

        -- page performance

        e.redirect_time_in_ms,
        e.unload_time_in_ms,
        e.app_cache_time_in_ms,
        e.dns_time_in_ms,
        e.tcp_time_in_ms,
        e.request_time_in_ms,
        e.response_time_in_ms,
        e.processing_time_in_ms,
        e.dom_loading_to_interactive_time_in_ms,
        e.dom_interactive_to_complete_time_in_ms,
        e.onload_time_in_ms,
        e.total_time_in_ms

      FROM ${scratch_pv_01.SQL_TABLE_NAME} AS a

      INNER JOIN ${scratch_pv_02.SQL_TABLE_NAME} AS b
        ON a.page_view_id = b.page_view_id

      INNER JOIN ${scratch_pv_03.SQL_TABLE_NAME} AS c
        ON a.page_view_id = c.page_view_id

      INNER JOIN ${scratch_pv_04.SQL_TABLE_NAME} AS d
        ON a.page_view_id = d.page_view_id

      INNER JOIN ${scratch_pv_05.SQL_TABLE_NAME} AS e
        ON a.page_view_id = e.page_view_id

      WHERE a.br_family != 'Robot/Spider'
        AND a.useragent NOT SIMILAR TO '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
        AND a.domain_userid IS NOT NULL -- rare edge case
        AND a.domain_sessionidx > 0 -- rare edge case
        -- AND a.app_id IN ('demo-app')
        -- AND a.page_urlhost IN ('website.com', 'another.website.com')
        -- AND a.name_tracker = 'namespace'
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${scratch_pv_05.SQL_TABLE_NAME} ;;
    distribution: "user_snowplow_domain_id"
    sortkeys: ["page_view_start"]
  }

  # DIMENSIONS

  # User

  dimension: user_custom_id {
    type: string
    sql: ${TABLE}.user_custom_id ;;
    group_label: "User"
  }

  dimension: user_snowplow_domain_id {
    type: string
    sql: ${TABLE}.user_snowplow_domain_id ;;
    group_label: "User"
  }

  dimension: user_snowplow_crossdomain_id {
    type: string
    sql: ${TABLE}.user_snowplow_crossdomain_id ;;
    group_label: "User"
    hidden: yes
  }

  # Session

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
    group_label: "Session"
  }

  dimension: session_index {
    type: number
    sql: ${TABLE}.session_index ;;
    group_label: "Session"
  }

  # Page View

  dimension: page_view_id {
    type: string
    sql: ${TABLE}.page_view_id ;;
    group_label: "Page View"
  }

  dimension: page_view_index {
    type: number
    # index across all sessions
    sql: ${TABLE}.page_view_index ;;
    group_label: "Page View"
  }

  dimension: page_view_in_session_index {
    type: number
    # index within each session
    sql: ${TABLE}.page_view_in_session_index ;;
    group_label: "Page View"
  }

  # Page View Time

  dimension_group: page_view_start {
    type: time
    timeframes: [time, minute10, hour, date, week, month, quarter, year]
    sql: ${TABLE}.page_view_start ;;
    #X# group_label:"Page View Time"
  }

  dimension_group: page_view_end {
    type: time
    timeframes: [time, minute10, hour, date, week, month, quarter, year]
    sql: ${TABLE}.page_view_end ;;
    #X# group_label:"Page View Time"
    hidden: yes
  }

  # Page View Time (User Timezone)

  dimension_group: page_view_start_local {
    type: time
    timeframes: [time, time_of_day, hour_of_day, day_of_week]
    sql: ${TABLE}.page_view_start_local ;;
    #X# group_label:"Page View Time (User Timezone)"
    convert_tz: no
  }

  dimension_group: page_view_end_local {
    type: time
    timeframes: [time, time_of_day, hour_of_day, day_of_week]
    sql: ${TABLE}.page_view_end_local ;;
    #X# group_label:"Page View Time (User Timezone)"
    convert_tz: no
    hidden: yes
  }

  # Engagement

  dimension: time_engaged {
    type: number
    sql: ${TABLE}.time_engaged_in_s ;;
    group_label: "Engagement"
    value_format: "0\"s\""
  }

  dimension: time_engaged_tier {
    type: tier
    tiers: [0, 10, 30, 60]
    style: integer
    sql: ${time_engaged} ;;
    group_label: "Engagement"
    value_format: "0\"s\""
  }

  dimension: x_pixels_scrolled {
    type: number
    sql: ${TABLE}.horizontal_pixels_scrolled ;;
    group_label: "Engagement"
    value_format: "0\"px\""
  }

  dimension: y_pixels_scrolled {
    type: number
    sql: ${TABLE}.vertical_pixels_scrolled ;;
    group_label: "Engagement"
    value_format: "0\"px\""
  }

  dimension: x_percentage_scrolled {
    type: number
    sql: ${TABLE}.horizontal_percentage_scrolled ;;
    group_label: "Engagement"
    value_format: "0\%"
  }

  dimension: y_percentage_scrolled {
    type: number
    sql: ${TABLE}.vertical_percentage_scrolled ;;
    group_label: "Engagement"
    value_format: "0\%"
  }

  dimension: y_percentage_scrolled_tier {
    type: tier
    tiers: [0, 25, 50, 75, 101]
    style: integer
    sql: ${y_percentage_scrolled} ;;
    group_label: "Engagement"
    value_format: "0\%"
  }

  dimension: user_bounced {
    type: yesno
    sql: ${time_engaged} = 0 ;;
    group_label: "Engagement"
  }

  dimension: user_engaged {
    type: yesno
    sql: ${time_engaged} >= 30 AND ${y_percentage_scrolled} >= 25 ;;
    group_label: "Engagement"
  }

  # Page

  dimension: page_url {
    type: string
    sql: ${TABLE}.page_url ;;
    group_label: "Page"
  }

  dimension: page_url_scheme {
    type: string
    sql: ${TABLE}.page_url_scheme ;;
    group_label: "Page"
    hidden: yes
  }

  dimension: page_url_host {
    type: string
    sql: ${TABLE}.page_url_host ;;
    group_label: "Page"
  }

  dimension: page_url_port {
    type: number
    sql: ${TABLE}.page_url_port ;;
    group_label: "Page"
    hidden: yes
  }

  dimension: page_url_path {
    type: string
    sql: ${TABLE}.page_url_path ;;
    group_label: "Page"
  }

  dimension: page_url_query {
    type: string
    sql: ${TABLE}.page_url_query ;;
    group_label: "Page"
  }

  dimension: page_url_fragment {
    type: string
    sql: ${TABLE}.page_url_fragment ;;
    group_label: "Page"
  }

  dimension: page_title {
    type: string
    sql: ${TABLE}.page_title ;;
    group_label: "Page"
  }

  dimension: page_width {
    type: number
    sql: ${TABLE}.page_width ;;
    group_label: "Page"
  }

  dimension: page_height {
    type: number
    sql: ${TABLE}.page_height ;;
    group_label: "Page"
  }

  # Referer

  dimension: referer_url {
    type: string
    sql: ${TABLE}.referer_url ;;
    group_label: "Referer"
  }

  dimension: referer_url_scheme {
    type: string
    sql: ${TABLE}.referer_url_scheme ;;
    group_label: "Referer"
    hidden: yes
  }

  dimension: referer_url_host {
    type: string
    sql: ${TABLE}.referer_url_host ;;
    group_label: "Referer"
  }

  dimension: referer_url_port {
    type: number
    sql: ${TABLE}.referer_url_port ;;
    group_label: "Referer"
    hidden: yes
  }

  dimension: referer_url_path {
    type: string
    sql: ${TABLE}.referer_url_path ;;
    group_label: "Referer"
  }

  dimension: referer_url_query {
    type: string
    sql: ${TABLE}.referer_url_query ;;
    group_label: "Referer"
  }

  dimension: referer_url_fragment {
    type: string
    sql: ${TABLE}.referer_url_fragment ;;
    group_label: "Referer"
  }

  dimension: referer_medium {
    type: string
    sql: ${TABLE}.referer_medium ;;
    group_label: "Referer"
  }

  dimension: referer_source {
    type: string
    sql: ${TABLE}.referer_source ;;
    group_label: "Referer"
  }

  dimension: referer_term {
    type: string
    sql: ${TABLE}.referer_term ;;
    group_label: "Referer"
  }

  # Marketing

  dimension: marketing_medium {
    type: string
    sql: ${TABLE}.marketing_medium ;;
    group_label: "Marketing"
  }

  dimension: marketing_source {
    type: string
    sql: ${TABLE}.marketing_source ;;
    group_label: "Marketing"
  }

  dimension: marketing_term {
    type: string
    sql: ${TABLE}.marketing_term ;;
    group_label: "Marketing"
  }

  dimension: marketing_content {
    type: string
    sql: ${TABLE}.marketing_content ;;
    group_label: "Marketing"
  }

  dimension: marketing_campaign {
    type: string
    sql: ${TABLE}.marketing_campaign ;;
    group_label: "Marketing"
  }

  dimension: marketing_click_id {
    type: string
    sql: ${TABLE}.marketing_click_id ;;
    group_label: "Marketing"
  }

  dimension: marketing_network {
    type: string
    sql: ${TABLE}.marketing_network ;;
    group_label: "Marketing"
  }

  # Location

  dimension: geo_country {
    type: string
    sql: ${TABLE}.geo_country ;;
    group_label: "Location"
  }

  dimension: geo_region {
    type: string
    sql: ${TABLE}.geo_region ;;
    group_label: "Location"
  }

  dimension: geo_region_name {
    type: string
    sql: ${TABLE}.geo_region_name ;;
    group_label: "Location"
  }

  dimension: geo_city {
    type: string
    sql: ${TABLE}.geo_city ;;
    group_label: "Location"
  }

  dimension: geo_zipcode {
    type: zipcode
    sql: ${TABLE}.geo_zipcode ;;
    group_label: "Location"
  }

  dimension: geo_latitude {
    type: number
    sql: ${TABLE}.geo_latitude ;;
    group_label: "Location"
    # use geo_location instead
    hidden: yes
  }

  dimension: geo_longitude {
    type: number
    sql: ${TABLE}.geo_longitude ;;
    group_label: "Location"
    # use geo_location instead
    hidden: yes
  }

  dimension: geo_timezone {
    type: string
    sql: ${TABLE}.geo_timezone ;;
    group_label: "Location"
    # use os_timezone instead
    hidden: yes
  }

  dimension: geo_location {
    type: location
    sql_latitude: ${geo_latitude} ;;
    sql_longitude: ${geo_longitude} ;;
    group_label: "Location"
  }

  # IP

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
    group_label: "IP"
  }

  dimension: ip_isp {
    type: string
    sql: ${TABLE}.ip_isp ;;
    group_label: "IP"
  }

  dimension: ip_organization {
    type: string
    sql: ${TABLE}.ip_organization ;;
    group_label: "IP"
  }

  dimension: ip_domain {
    type: string
    sql: ${TABLE}.ip_domain ;;
    group_label: "IP"
  }

  dimension: ip_net_speed {
    type: string
    sql: ${TABLE}.ip_net_speed ;;
    group_label: "IP"
  }

  # Application

  dimension: app_id {
    type: string
    sql: ${TABLE}.app_id ;;
    group_label: "Application"
  }

  # Browser

  dimension: browser {
    type: string
    sql: ${TABLE}.browser ;;
    group_label: "Browser"
  }

  dimension: browser_name {
    type: string
    sql: ${TABLE}.browser_name ;;
    group_label: "Browser"
  }

  dimension: browser_major_version {
    type: string
    sql: ${TABLE}.browser_major_version ;;
    group_label: "Browser"
  }

  dimension: browser_minor_version {
    type: string
    sql: ${TABLE}.browser_minor_version ;;
    group_label: "Browser"
  }

  dimension: browser_build_version {
    type: string
    sql: ${TABLE}.browser_build_version ;;
    group_label: "Browser"
  }

  dimension: browser_engine {
    type: string
    sql: ${TABLE}.browser_engine ;;
    group_label: "Browser"
  }

  dimension: browser_window_width {
    type: number
    sql: ${TABLE}.browser_window_width ;;
    group_label: "Browser"
  }

  dimension: browser_window_height {
    type: number
    sql: ${TABLE}.browser_window_height ;;
    group_label: "Browser"
  }

  dimension: browser_language {
    type: string
    sql: ${TABLE}.browser_language ;;
    group_label: "Browser"
  }

  # OS

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
    group_label: "OS"
  }

  dimension: os_name {
    type: string
    sql: ${TABLE}.os_name ;;
    group_label: "OS"
  }

  dimension: os_major_version {
    type: string
    sql: ${TABLE}.os_major_version ;;
    group_label: "OS"
  }

  dimension: os_minor_version {
    type: string
    sql: ${TABLE}.os_minor_version ;;
    group_label: "OS"
  }

  dimension: os_build_version {
    type: string
    sql: ${TABLE}.os_build_version ;;
    group_label: "OS"
  }

  dimension: os_manufacturer {
    type: string
    sql: ${TABLE}.os_manufacturer ;;
    group_label: "OS"
  }

  dimension: os_timezone {
    type: string
    sql: ${TABLE}.os_timezone ;;
    group_label: "OS"
  }

  # Device

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
    group_label: "Device"
  }

  dimension: device_type {
    type: string
    sql: ${TABLE}.device_type ;;
    group_label: "Device"
  }

  dimension: device_is_mobile {
    type: yesno
    sql: ${TABLE}.device_is_mobile ;;
    group_label: "Device"
  }

  # Page performance

  dimension: redirect_time {
    type: number
    sql: ${TABLE}.redirect_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: unload_time {
    type: number
    sql: ${TABLE}.unload_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: app_cache_time {
    type: number
    sql: ${TABLE}.app_cache_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: dns_time {
    type: number
    sql: ${TABLE}.dns_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: tcp_time {
    type: number
    sql: ${TABLE}.tcp_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: request_time {
    type: number
    sql: ${TABLE}.request_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: response_time {
    type: number
    sql: ${TABLE}.response_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: processing_time {
    type: number
    sql: ${TABLE}.processing_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: dom_loading_to_interactive_time {
    type: number
    sql: ${TABLE}.dom_loading_to_interactive_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: dom_interactive_to_complete_time {
    type: number
    sql: ${TABLE}.dom_interactive_to_complete_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: onload_time {
    type: number
    sql: ${TABLE}.onload_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  dimension: total_time {
    type: number
    sql: ${TABLE}.total_time_in_ms ;;
    value_format: "0\"ms\""
    group_label: "Page Performance"
  }

  # MEASURES

  measure: row_count {
    type: count
    group_label: "Counts"
  }

  measure: page_view_count {
    type: count_distinct
    sql: ${page_view_id} ;;
    group_label: "Counts"
  }

  measure: bounced_page_view_count {
    type: count_distinct
    sql: ${page_view_id} ;;

    filters: {
      field: user_bounced
      value: "yes"
    }

    group_label: "Counts"
  }

  measure: engaged_page_view_count {
    type: count_distinct
    sql: ${page_view_id} ;;

    filters: {
      field: user_engaged
      value: "yes"
    }

    group_label: "Counts"
  }

  measure: session_count {
    type: count_distinct
    sql: ${session_id} ;;
    group_label: "Counts"
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_snowplow_domain_id} ;;
    group_label: "Counts"
  }

  # Engagement

  measure: total_time_engaged {
    type: sum
    sql: ${time_engaged} ;;
    value_format: "#,##0\"s\""
    group_label: "Engagement"
  }

  measure: average_time_engaged {
    type: average
    sql: ${time_engaged} ;;
    value_format: "0.00\"s\""
    group_label: "Engagement"
  }

  measure: average_percentage_scrolled {
    type: average
    sql: ${y_percentage_scrolled} ;;
    value_format: "0.00\%"
    group_label: "Engagement"
  }

  # Page performance

  measure: average_request_time {
    type: average
    sql: ${request_time} ;;
    value_format: "#,##0\"ms\""
    group_label: "Page Performance"
  }

  measure: average_response_time {
    type: average
    sql: ${response_time} ;;
    value_format: "#,##0\"ms\""
    group_label: "Page Performance"
  }

  measure: average_time_to_dom_interactive {
    type: average
    sql: ${dom_loading_to_interactive_time} ;;
    value_format: "#,##0\"ms\""
    group_label: "Page Performance"
  }

  measure: average_time_to_dom_complete {
    type: average
    sql: ${dom_interactive_to_complete_time} ;;
    value_format: "#,##0\"ms\""
    group_label: "Page Performance"
  }

  measure: average_onload_time {
    type: average
    sql: ${onload_time} ;;
    value_format: "#,##0\"ms\""
    group_label: "Page Performance"
  }

  measure: average_total_time {
    type: average
    sql: ${total_time} ;;
    value_format: "#,##0\"ms\""
    group_label: "Page Performance"
  }
}
