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

- dashboard: web
  title: Snowplow web dashboard (4 week period)
  layout: grid

  rows:
    - elements: [number_of_users, number_of_new_users, number_of_engaged_new_users, number_of_new_users_that_returned]
      height: 200
    - elements: [referer_breakdown, map]
      height: 450
    - elements: [sessions_per_referer_medium, page_performance_per_browser]
      height: 450
    - elements: [devices_per_hour]
      height: 450

  elements:

  # Row 1

  - name: number_of_users
    title: Number of users
    type: single_value
    model: snowplow_web_block
    explore: sessions
    dimensions: sessions.session_start_window
    measures: sessions.user_count
    dynamic_fields:
    - table_calculation: change
      label: Change since last period
      expression: offset(${sessions.user_count}, 0) - offset(${sessions.user_count}, 1)
    sorts: sessions.session_start_window
    show_comparison: true
    comparison_type: change
    comparison_label: compared to last period
    show_comparison_label: true

  - name: number_of_new_users
    title: Number of new users
    type: single_value
    model: snowplow_web_block
    explore: sessions
    dimensions: sessions.session_start_window
    measures: sessions.new_user_count
    dynamic_fields:
    - table_calculation: change
      label: Change since last period
      expression: offset(${sessions.new_user_count}, 0) - offset(${sessions.new_user_count}, 1)
    sorts: sessions.session_start_window
    show_comparison: true
    comparison_type: change
    comparison_label: compared to last period
    show_comparison_label: true

  - name: number_of_engaged_new_users
    title: Number of engaged new users
    type: single_value
    model: snowplow_web_block
    explore: sessions
    dimensions: sessions.session_start_window
    measures: sessions.new_user_count
    dynamic_fields:
    - table_calculation: change
      label: Change since last period
      expression: offset(${sessions.new_user_count}, 0) - offset(${sessions.new_user_count}, 1)
    filters:
      sessions.user_engaged: yes
    sorts: sessions.session_start_window
    show_comparison: true
    comparison_type: change
    comparison_label: compared to last period
    show_comparison_label: true

  - name: number_of_new_users_that_returned
    title: Number of new users that returned
    type: single_value
    model: snowplow_web_block
    explore: sessions
    dimensions: users.first_session_start_window
    measures: sessions.user_count
    dynamic_fields:
    - table_calculation: change
      label: Change since last period
      expression: offset(${sessions.user_count}, 0) - offset(${sessions.user_count}, 1)
    filters:
      sessions.session_index: '>1'
    sorts: users.first_session_start_window
    show_comparison: true
    comparison_type: change
    comparison_label: compared to last period
    show_comparison_label: true

  # Row 2

  - name: referer_breakdown
    title: Referer breakdown
    type: looker_donut_multiples
    model: snowplow_web_block
    explore: sessions
    dimensions: [sessions.first_or_returning_session, sessions.referer_medium]
    pivots: sessions.referer_medium
    measures: sessions.row_count
    sorts: [sessions.first_or_returning_session, sessions.referer_medium]
    filters:
      sessions.session_start_time: 28 days

  - name: map
    title: User location
    type: looker_geo_coordinates
    model: snowplow_web_block
    explore: sessions
    dimensions: sessions.geo_location
    measures: sessions.user_count
    filters:
      sessions.geo_location: inside box from 85.0511287798066, -540 to -85.05112877980659, 180
      sessions.session_start_time: 28 days
    limit: 1000
    map: world
    map_scale_indicator: 'on'
    map_pannable: true
    map_zoomable: true
    map_zoom: 2

  # Row 3

  - name: sessions_per_referer_medium
    title: Sessions per referer medium
    type: looker_area
    model: snowplow_web_block
    explore: sessions
    dimensions: [sessions.referer_medium, sessions.session_start_date]
    pivots: sessions.referer_medium
    measures: sessions.session_count
    filters:
      sessions.session_start_date: 28 days
    sorts: [sessions.referer_medium, sessions.session_start_date desc]
    stacking: normal
    x_axis_label: Date
    y_axis_labels: Sessions

  - name: page_performance_per_browser
    title: Page performance per browser
    type: looker_column
    model: snowplow_web_block
    explore: page_views
    dimensions: page_views.browser_name
    measures: [page_views.average_request_time, page_views.average_response_time, page_views.average_onload_time, page_views.average_time_to_dom_interactive, page_views.average_time_to_dom_complete]
    filters:
      page_views.page_view_count: '>2500'
    sorts: page_views.browser_name
    stacking: normal
    hide_legend: true

    # Row 4

  - name: devices_per_hour
    title: Hourly page views per device
    type: looker_column
    model: snowplow_web_block
    explore: page_views
    dimensions: [page_views.page_view_start_local_hour_of_day, page_views.device_type]
    pivots: [page_views.device_type]
    measures: [page_views.page_view_count]
    filters:
      page_views.device_type: -Game console,-Unknown
      page_views.page_view_start_local_hour_of_day: NOT NULL
      sessions.session_start_time: 28 days
    sorts: [page_views.page_view_start_local_hour_of_day, page_views.device_type]
    limit: '500'
    column_limit: '50'
    stacking: normal
