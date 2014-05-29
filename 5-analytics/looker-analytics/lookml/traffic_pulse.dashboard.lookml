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

- dashboard: traffic_pulse
  title: "Traffic pulse"
  layout: tile
  tile_size: 120
  
  filters:

  - name: date
    title: "Date"
    type: date_filter
    default_value: last 30 days

  - name: new_vs_returning
    title: "New vs returning visitors"
    type: select_filter
    base_view: sessions
    dimension: sessions.new_vs_returning_visitor
  
  - name: referer_medium
    title: "Referer medium"
    type: select_filter
    base_view: sessions
    dimension: sessions.referer_medium
    
  - name: referer_host
    title: "Referer URL host"
    type: select_filter
    base_view: sessions
    dimension: sessions.referer_url_host

  - name: country
    title: Country
    type: select_filter
    base_view: sessions
    dimension: sessions.geography_country
    
  - name: landing_page
    title: "Landing page path"
    type: select_filter
    base_view: sessions
    dimension: sessions.landing_page_path
    
  
  elements:
  
  - name: new_vs_returning_visitors
    title: New vs returning sessions count
    type: looker_pie
    base_view: sessions
    dimensions: [sessions.new_vs_returning_visitor]
    measures: [sessions.count]
    filters:
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    limit: 500
    width: 3
    height: 2
    legend_align:

  - name: sessions_by_referer_medium
    title: Sessions count by referer medium
    type: looker_pie
    base_view: sessions
    dimensions: [sessions.referer_medium]
    measures: [sessions.count]
    filters:
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.referer_medium desc]
    limit: 500
    width: 3
    height: 2
    legend_align:
    
  - name: sessions_by_country 
    title: Sessions count by country
    type: looker_pie
    base_view: sessions
    dimensions: [sessions.geography_country]
    measures: [sessions.count]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    limit: 500
    width: 3
    height: 2
    legend_align:

  - name: sessions_by_landing_page
    title: Sessions count by landing page
    type: looker_pie
    base_view: sessions
    dimensions: [sessions.landing_page_path]
    measures: [sessions.count]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    limit: 500
    width: 3
    height: 2
    legend_align:


  - name: sessions_by_country_map 
    title: Sessions count by country
    type: looker_geo_choropleth
    map: world
    base_view: sessions
    dimensions: [sessions.geography_country_three_letter_iso_code]
    measures: [sessions.count, sessions.events_per_session]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    limit: 500
    width: 6
    height: 4
    legend_align:

  - name: new_vs_returning_visitors_by_day
    title: "New vs returning visitors"
    type: looker_area
    base_view: sessions
    dimensions: [sessions.start_date]
    pivots: [sessions.new_vs_returning_visitor]
    measures: [sessions.count]
    filters:
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    limit: 500
    width: 6
    height: 4
    legend_align:
    hide_legend:
    stacking: normal
    x_axis_label:
    x_axis_datetime:
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min:
    y_axis_max:
    hide_points: true

  - name: sessions_by_day_by_refr_medium
    title: Sessions by day by referer medium
    type: looker_area
    base_view: sessions
    dimensions: [sessions.start_date]
    pivots: [sessions.referer_medium]
    measures: [sessions.count]
    filters:
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.start_date desc]
    limit: 500
    width: 6
    height: 4
    legend_align:
    hide_legend:
    stacking: normal
    x_axis_label:
    x_axis_datetime:
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min:
    y_axis_max:
    hide_points: true

  - name: sessions_and_engagement_levels_for_most_popular_landing_pages
    title: "Sessions count and events per session for top 10 landing pages"
    type: looker_column
    base_view: sessions
    dimensions: [sessions.landing_page_path]
    measures: [sessions.count, sessions.events_per_session]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.start_date desc]
    sorts: [sessions.count desc]
    limit: 10
    width: 6
    height: 4
    legend_align:
    hide_legend:
    stacking:
    x_axis_label:
    x_axis_datetime:
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min:
    y_axis_max:
    
  - name: cohort_analysis
    title: "Cohort analysis"
    type: table
    base_view: sessions
    dimensions: [visitors.first_touch_week]
    pivots: [sessions.start_week]
    measures: [visitors.count]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    limit: 500
    width:
    height:


  - name: session_count_and_engagement_level_by_referer_host
    title: "Session count and events per sesion for top 10 referer hosts"
    type: looker_column
    base_view: sessions
    dimensions: [sessions.referer_url_host]
    measures: [sessions.count, sessions.bounce_rate, sessions.events_per_session]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    limit: 10
    width:
    height:
    legend_align:
    hide_legend:
    stacking:
    x_axis_label:
    x_axis_datetime:
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min:
    y_axis_max:

  - name: session_count_by_session_index
    title: "Split in number of sessions by number of previous visits"
    type: looker_pie
    base_view: sessions
    dimensions: [sessions.session_index_tier]
    measures: [sessions.count]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    inner_radius: 75%
    limit: 500
    width:
    height:
    legend_align:
    sorts: [sessions.session_index_tier]

  - name: session_count_by_engagement_level
    title: "Split in number of sessions by number of events per session"
    type: looker_pie
    base_view: sessions
    dimensions: [sessions.events_during_session_tiered]
    measures: [sessions.count]
    listen:
      date: sessions.start_date
      referer_medium: sessions.referer_medium
      new_vs_returning: sessions.new_vs_returning_visitor
      country: sessions.geography_country
      landing_page: sessions.landing_page_path
      referer_host: sessions.referer_url_host
    sorts: [sessions.count desc]
    inner_radius: 75%
    limit: 500
    width:
    height:
    legend_align:
    sorts: [sessions.events_during_session_tiered]
