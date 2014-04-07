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

- dashboard: last_7_days
  title: "Last 7 days"
  layout: tile
  tile_size: 100
  elements:

  - name: visits_last_7_days
    title: Visits in the last 7 days
    type: single_value
    base_view: visits
    measures: [visits.visits_count]
    filters:
      visits.occurred_date: last 7 days
    limit: 500
    width: 
    height: 

  - name: visits_by_day_by_refr_medium
    title: Visits by day by referer medium
    type: area
    base_view: visits
    dimensions: [visits.occurred_date]
    pivots: [source_original.refr_medium]
    measures: [visits.visits_count]
    filters:
      visits.occurred_date: last 7 days
    limit: 500
    width:
    height:
    legend_align:
    stacking: normal
    x_axis_label:
    x_axis_datetime: true
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min:
    y_axis_max:


  - name: visits_and_engagement_by_refr_medium
    title: Visits and engagement by referer medium
    type: column
    base_view: visits
    dimensions: [source.refr_medium]
    measures: [visits.visits_count, visits.events_per_visit, visits.bounce_rate]
    filters:
      visits.occurred_date: last 7 days
    sorts: [visits.visits_count desc]
    limit: 500
    width: 
    height:
    legend_align:
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

  - name: visits_by_landing_page
    title: Visits by landing page
    type: pie
    base_view: visits
    dimensions: [landing_page.landing_page]
    measures: [visits.visits_count]
    filters:
      visits.occurred_date: last 7 days
    sorts: [visits.visits_count desc]
    limit: 500
    width:
    height:
    legend_align:

  - name: new_vs_returning
    title: New vs returning visits
    type: pie
    base_view: visits
    dimensions: [visits.new_vs_returning_visitor]
    measures: [visits.visits_count]
    filters:
      visits.occurred_date: last 7 days
    sorts: [visits.visits_count desc]
    limit: 500
    width:
    height:
    legend_align:

    
  - name: inbound_links
    title: Visits and engagement by top inbound links driving users to website
    type: table
    base_view: visits
    dimensions: [source_original.refr_urlhost, source_original.refr_urlpath, landing_page.landing_page]
    measures: [visits.visits_count, visits.bounce_rate, visits.events_per_visit]
    filters:
      source_original.refr_medium: other_website
      visits.occurred_date: last 7 days
    limit: 500
    width:
    height:

  - name: search_traffic_by_landing_page
    title: Search traffic (volume and engagement level) by landing page
    type: table
    base_view: visits
    dimensions: [landing_page.landing_page]
    measures: [visits.visits_count, visits.bounce_rate, visits.events_per_visit]
    filters:
      source_original.refr_medium: search
      visits.occurred_date: last 7 days
    sorts: [visits.visits_count desc]
    limit: 500
    width:
    height:

  - name: social_traffic_by_landing_page
    title: Social traffic (volume and engagement level) by landing page
    type: table
    base_view: visits
    dimensions: [landing_page.landing_page]
    measures: [visits.visits_count, visits.bounce_rate, visits.events_per_visit]
    filters:
      source_original.refr_medium: social
      visits.occurred_date: last 7 days
    sorts: [visits.visits_count desc]
    limit: 500
    width:
    height:
