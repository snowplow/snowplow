view: sessions {
  derived_table: {
    sql: WITH prep AS (

        SELECT

          session_id,

          -- time

          MIN(page_view_start) AS session_start,
          MAX(page_view_end) AS session_end,

          MIN(page_view_start_local) AS session_start_local,
          MAX(page_view_end_local) AS session_end_local,

          -- engagement

          COUNT(*) AS page_views,

          -- SUM(CASE WHEN user_bounced THEN 1 ELSE 0 END) AS bounced_page_views,
          -- SUM(CASE WHEN user_engaged THEN 1 ELSE 0 END) AS engaged_page_views,

          SUM(time_engaged_in_s) AS time_engaged_in_s

        FROM ${page_views.SQL_TABLE_NAME}

        GROUP BY 1
        ORDER BY 1

      )

      SELECT

        -- user

        a.user_custom_id,
        a.user_snowplow_domain_id,
        a.user_snowplow_crossdomain_id,

        -- sesssion

        a.session_id,
        a.session_index,

        -- session: time

        b.session_start,
        b.session_end,

          -- example derived dimensions

          -- TO_CHAR(b.session_start, 'YYYY-MM-DD HH24:MI:SS') AS session_time,
          -- TO_CHAR(b.session_start, 'YYYY-MM-DD HH24:MI') AS session_minute,
          -- TO_CHAR(b.session_start, 'YYYY-MM-DD HH24') AS session_hour,
          -- TO_CHAR(b.session_start, 'YYYY-MM-DD') AS session_date,
          -- TO_CHAR(DATE_TRUNC('week', b.session_start), 'YYYY-MM-DD') AS session_week,
          -- TO_CHAR(b.session_start, 'YYYY-MM') AS session_month,
          -- TO_CHAR(DATE_TRUNC('quarter', b.session_start), 'YYYY-MM') AS session_quarter,
          -- DATE_PART(Y, b.session_start)::INTEGER AS session_year,

        -- session: time in the user's local timezone

        b.session_start_local,
        b.session_end_local,

          -- example derived dimensions

          -- TO_CHAR(b.session_start_local, 'YYYY-MM-DD HH24:MI:SS') AS session_local_time,
          -- TO_CHAR(b.session_start_local, 'HH24:MI') AS session_local_time_of_day,
          -- DATE_PART(hour, b.session_start_local)::INTEGER AS session_local_hour_of_day,
          -- TRIM(TO_CHAR(b.session_start_local, 'd')) AS session_local_day_of_week,
          -- MOD(EXTRACT(DOW FROM b.session_start_local)::INTEGER - 1 + 7, 7) AS session_local_day_of_week_index,

        -- engagement

        b.page_views,

        -- b.bounced_page_views,
        -- b.engaged_page_views,

        b.time_engaged_in_s,

          -- CASE
            -- WHEN b.time_engaged_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
            -- WHEN b.time_engaged_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
            -- WHEN b.time_engaged_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
            -- WHEN b.time_engaged_in_s BETWEEN 60 AND 119 THEN '60s to 119s'
            -- WHEN b.time_engaged_in_s BETWEEN 120 AND 239 THEN '120s to 239s'
            -- WHEN b.time_engaged_in_s > 239 THEN '240s or more'
            -- ELSE NULL
          -- END AS time_engaged_in_s_tier,

          -- CASE WHEN (b.page_views = 1 AND b.bounced_page_views = 1) THEN TRUE ELSE FALSE END AS user_bounced,
          -- CASE WHEN (b.page_views > 2 AND b.time_engaged_in_s > 59) OR b.engaged_page_views > 0 THEN TRUE ELSE FALSE END AS user_engaged,

        -- first page

        a.page_url AS first_page_url,

        a.page_url_scheme AS first_page_url_scheme,
        a.page_url_host AS first_page_url_host,
        a.page_url_port AS first_page_url_port,
        a.page_url_path AS first_page_url_path,
        a.page_url_query AS first_page_url_query,
        a.page_url_fragment AS first_page_url_fragment,

        a.page_title AS first_page_title,

        -- referer

        a.referer_url,

        a.referer_url_scheme,
        a.referer_url_host,
        a.referer_url_port,
        a.referer_url_path,
        a.referer_url_query,
        a.referer_url_fragment,

        a.referer_medium,
        a.referer_source,
        a.referer_term,

        -- marketing

        a.marketing_medium,
        a.marketing_source,
        a.marketing_term,
        a.marketing_content,
        a.marketing_campaign,
        a.marketing_click_id,
        a.marketing_network,

        -- location

        a.geo_country,
        a.geo_region,
        a.geo_region_name,
        a.geo_city,
        a.geo_zipcode,
        a.geo_latitude,
        a.geo_longitude,
        a.geo_timezone, -- can be NULL

        -- IP

        a.ip_address,

        a.ip_isp,
        a.ip_organization,
        a.ip_domain,
        a.ip_net_speed,

        -- application

        a.app_id,

        -- browser

        a.browser,
        a.browser_name,
        a.browser_major_version,
        a.browser_minor_version,
        a.browser_build_version,
        a.browser_engine,

        a.browser_language,

        -- OS

        a.os,
        a.os_name,
        a.os_major_version,
        a.os_minor_version,
        a.os_build_version,
        a.os_manufacturer,
        a.os_timezone,

        -- device

        a.device,
        a.device_type,
        a.device_is_mobile

      FROM ${page_views.SQL_TABLE_NAME} AS a

      INNER JOIN prep AS b
        ON a.session_id = b.session_id

      WHERE a.page_view_in_session_index = 1
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${page_views.SQL_TABLE_NAME} ;;
    distribution: "user_snowplow_domain_id"
    sortkeys: ["session_start"]
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

  dimension: first_or_returning_session {
    type: string

    case: {
      when: {
        sql: ${session_index} = 1 ;;
        label: "First session"
      }

      when: {
        sql: ${session_index} > 1 ;;
        label: "Returning session"
      }

      else: "Error"
    }

    group_label: "Session"
    hidden: yes
  }

  # Session Time

  dimension_group: session_start {
    type: time
    timeframes: [time, minute10, hour, date, week, month, quarter, year]
    sql: ${TABLE}.session_start ;;
    #X# group_label:"Session Time"
  }

  dimension_group: session_end {
    type: time
    timeframes: [time, minute10, hour, date, week, month, quarter, year]
    sql: ${TABLE}.session_end ;;
    #X# group_label:"Session Time"
    hidden: yes
  }

  dimension: session_start_window {
    case: {
      when: {
        sql: ${session_start_time} >= DATEADD(day, -28, GETDATE()) ;;
        label: "current_period"
      }

      when: {
        sql: ${session_start_time} >= DATEADD(day, -56, GETDATE()) AND ${session_start_time} < DATEADD(day, -28, GETDATE()) ;;
        label: "previous_period"
      }

      else: "unknown"
    }

    hidden: yes
  }

  # Session Time (User Timezone)

  dimension_group: session_start_local {
    type: time
    timeframes: [time, time_of_day, hour_of_day, day_of_week]
    sql: ${TABLE}.session_start_local ;;
    #X# group_label:"Session Time (User Timezone)"
    convert_tz: no
  }

  dimension_group: session_end_local {
    type: time
    timeframes: [time, time_of_day, hour_of_day, day_of_week]
    sql: ${TABLE}.session_end_local ;;
    #X# group_label:"Session Time (User Timezone)"
    convert_tz: no
    hidden: yes
  }

  # Engagement

  dimension: page_views {
    type: number
    sql: ${TABLE}.page_views ;;
    group_label: "Engagement"
  }

  dimension: time_engaged {
    type: number
    sql: ${TABLE}.time_engaged_in_s ;;
    group_label: "Engagement"
    value_format: "0\"s\""
  }

  dimension: time_engaged_tier {
    type: tier
    tiers: [0, 10, 30, 60, 120, 240]
    style: integer
    sql: ${time_engaged} ;;
    group_label: "Engagement"
    value_format: "0\"s\""
  }

  dimension: user_bounced {
    type: yesno
    sql: ${page_views} = 1 AND ${time_engaged} = 0 ;;
    group_label: "Engagement"
  }

  dimension: user_engaged {
    type: yesno
    sql: ${page_views} > 2 AND ${time_engaged} > 59 ;;
    group_label: "Engagement"
  }

  # First Page

  dimension: first_page_url {
    type: string
    sql: ${TABLE}.first_page_url ;;
    group_label: "First Page"
  }

  dimension: first_page_url_scheme {
    type: string
    sql: ${TABLE}.first_page_url_scheme ;;
    group_label: "First Page"
    hidden: yes
  }

  dimension: first_page_url_host {
    type: string
    sql: ${TABLE}.first_page_url_host ;;
    group_label: "First Page"
  }

  dimension: first_page_url_port {
    type: number
    sql: ${TABLE}.first_page_url_port ;;
    group_label: "First Page"
    hidden: yes
  }

  dimension: first_page_url_path {
    type: string
    sql: ${TABLE}.first_page_url_path ;;
    group_label: "First Page"
  }

  dimension: first_page_url_query {
    type: string
    sql: ${TABLE}.first_page_url_query ;;
    group_label: "First Page"
  }

  dimension: first_page_url_fragment {
    type: string
    sql: ${TABLE}.first_page_url_fragment ;;
    group_label: "First Page"
  }

  dimension: first_page_title {
    type: string
    sql: ${TABLE}.first_page_title ;;
    group_label: "First Page"
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

  # MEASURES

  measure: row_count {
    type: count
    group_label: "Counts"
  }

  measure: page_view_count {
    type: sum
    sql: ${page_views} ;;
    group_label: "Counts"
  }

  measure: session_count {
    type: count_distinct
    sql: ${session_id} ;;
    group_label: "Counts"
    drill_fields: [session_count]
  }
  set: session_count{
    fields: [session_id, session_start_date, first_page_url, referer_medium, page_view_count, total_time_engaged]
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_snowplow_domain_id} ;;
    group_label: "Counts"
    drill_fields: [user_count]
  }
  set: user_count{
    fields: [user_snowplow_domain_id, users.first_page_url, session_count, average_time_engaged, total_time_engaged]
  }

  measure: new_user_count {
    type: count_distinct
    sql: ${user_snowplow_domain_id} ;;

    filters: {
      field: session_index
      value: "1"
    }

    group_label: "Counts"
    drill_fields: [new_user_count]
    }
  set: new_user_count{
    fields: [user_snowplow_domain_id, users.first_page_url, session_count, average_time_engaged, total_time_engaged]
  }

  measure: bounced_user_count {
    type: count_distinct
    sql: ${user_snowplow_domain_id} ;;

    filters: {
      field: user_bounced
      value: "yes"
    }

    group_label: "Counts"
  }

  measure: engaged_user_count {
    type: count_distinct
    sql: ${user_snowplow_domain_id} ;;

    filters: {
      field: user_engaged
      value: "yes"
    }

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
}
