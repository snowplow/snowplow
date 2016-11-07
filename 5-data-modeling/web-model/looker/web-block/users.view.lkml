view: users {
  derived_table: {
    sql: WITH prep AS (

        SELECT

          user_snowplow_domain_id,

          -- time

          MIN(session_start) AS first_session_start,
          MIN(session_start_local) AS first_session_start_local,

          MAX(session_end) AS last_session_end,

          -- engagement

          SUM(page_views) AS page_views,
          COUNT(*) AS sessions,

          SUM(time_engaged_in_s) AS time_engaged_in_s

        FROM ${sessions.SQL_TABLE_NAME}

        GROUP BY 1
        ORDER BY 1

      )

      SELECT

        -- user

        a.user_custom_id,
        a.user_snowplow_domain_id,
        a.user_snowplow_crossdomain_id,

        -- first sesssion: time

        b.first_session_start,

          -- example derived dimensions

          -- TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24:MI:SS') AS first_session_time,
          -- TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24:MI') AS first_session_minute,
          -- TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24') AS first_session_hour,
          -- TO_CHAR(b.first_session_start, 'YYYY-MM-DD') AS first_session_date,
          -- TO_CHAR(DATE_TRUNC('week', b.first_session_start), 'YYYY-MM-DD') AS first_session_week,
          -- TO_CHAR(b.first_session_start, 'YYYY-MM') AS first_session_month,
          -- TO_CHAR(DATE_TRUNC('quarter', b.first_session_start), 'YYYY-MM') AS first_session_quarter,
          -- DATE_PART(Y, b.first_session_start)::INTEGER AS first_session_year,

        -- first session: time in the user's local timezone

        b.first_session_start_local,

          -- example derived dimensions

          -- TO_CHAR(b.first_session_start_local, 'YYYY-MM-DD HH24:MI:SS') AS first_session_local_time,
          -- TO_CHAR(b.first_session_start_local, 'HH24:MI') AS first_session_local_time_of_day,
          -- DATE_PART(hour, b.first_session_start_local)::INTEGER AS first_session_local_hour_of_day,
          -- TRIM(TO_CHAR(b.first_session_start_local, 'd')) AS first_session_local_day_of_week,
          -- MOD(EXTRACT(DOW FROM b.first_session_start_local)::INTEGER - 1 + 7, 7) AS first_session_local_day_of_week_index,

        -- last session: time

        b.last_session_end,

        -- engagement

        b.page_views,
        b.sessions,

        b.time_engaged_in_s,

        -- first page

        a.first_page_url,

        a.first_page_url_scheme,
        a.first_page_url_host,
        a.first_page_url_port,
        a.first_page_url_path,
        a.first_page_url_query,
        a.first_page_url_fragment,

        a.first_page_title,

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

        -- application

        a.app_id

      FROM ${sessions.SQL_TABLE_NAME} AS a

      INNER JOIN prep AS b
        ON a.user_snowplow_domain_id = b.user_snowplow_domain_id

      WHERE a.session_index = 1
       ;;
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions.SQL_TABLE_NAME} ;;
    distribution: "user_snowplow_domain_id"
    sortkeys: ["first_session_start"]
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

  # First Session Time

  dimension_group: first_session_start {
    type: time
    timeframes: [time, minute10, hour, date, week, month, quarter, year]
    sql: ${TABLE}.first_session_start ;;
    #X# group_label:"First Session Time"
  }

  dimension: first_session_start_window {
    case: {
      when: {
        sql: ${first_session_start_time} >= DATEADD(day, -28, GETDATE()) ;;
        label: "current_period"
      }

      when: {
        sql: ${first_session_start_time} >= DATEADD(day, -56, GETDATE()) AND ${first_session_start_time} < DATEADD(day, -28, GETDATE()) ;;
        label: "previous_period"
      }

      else: "unknown"
    }

    hidden: yes
  }

  # Last Session Time

  dimension_group: last_session_end {
    type: time
    timeframes: [time, minute10, hour, date, week, month, quarter, year]
    sql: ${TABLE}.last_session_end ;;
    #X# group_label:"Last Session Time"
  }

  # First Session Time (User Timezone)

  dimension_group: first_session_start_local {
    type: time
    timeframes: [time, time_of_day, hour_of_day, day_of_week]
    sql: ${TABLE}.first_session_start_local ;;
    #X# group_label:"First Session Time (User Timezone)"
    convert_tz: no
  }

  # Engagement

  dimension: page_views {
    type: number
    sql: ${TABLE}.page_views ;;
    group_label: "Engagement"
  }

  dimension: sessions {
    type: number
    sql: ${TABLE}.sessions ;;
    group_label: "Engagement"
  }

  dimension: time_engaged {
    type: number
    sql: ${TABLE}.time_engaged_in_s ;;
    group_label: "Engagement"
    value_format: "0\"s\""
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

  # Application

  dimension: app_id {
    type: string
    sql: ${TABLE}.app_id ;;
    group_label: "Application"
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
    type: sum
    sql: ${sessions} ;;
    group_label: "Counts"
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_snowplow_domain_id} ;;
    group_label: "Counts"
  }
}
