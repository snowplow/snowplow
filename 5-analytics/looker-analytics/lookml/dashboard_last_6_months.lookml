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

- dashboard: last_6_months
  title: "Last 6 months"
  layout: tile
  tile_size: 100
  elements:

  - name: new_vs_returning_visitors_by_week
    title: New vs returning visitors by week
    type: area
    base_view: visits
    dimensions: [visits.occurred_week]
    pivots: [visits.new_vs_returning_visitor]
    measures: [visits.visits_count]
    filters:
      visits.occurred_month: last 6 months
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
    y_axis_labels: Visits
    y_axis_min: 0
    y_axis_max:


  - name: new_visitors_by_refr_medium
    title: New visitors by referer medium, by week
    type: area
    base_view: visitors
    dimensions: [visitors.first_touch_week]
    pivots: [source_original.refr_medium]
    measures: [visitors.visitors_count]
    filters:
      visitors.first_touch_date: last 6 months
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
    y_axis_min: 0
    y_axis_max:

  - name: cohort_analysis
    title: Cohort analysis (user retention by month first touched website)
    type: table
    base_view: visits
    dimensions: [visitors.first_touch_week]
    pivots: [visits.occurred_week]
    measures: [visits.visitors_count]
    filters:
      visitors.first_touch_date: last 6 months
    sorts: [visitors.first_touch_week]
    limit: 500
    width:
    height:
  
  - name: visitors_and_events_by_cohort
    title: Visitors and events per visitor by cohort
    type: column
    base_view: visitors
    dimensions: [visitors.first_touch_month]
    measures: [visitors.visitors_count, visitors.events_per_visitor]
    filters:
      visitors.first_touch_month: last 6 months
    sorts: [visitors.first_touch_month desc]
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


  - name: fraction_of_visits_from_returning_visitors
    title: Fraction of total visits from returning (rather than new) visits by week
    type: line
    base_view: visits
    dimensions: [visits.occurred_week]
    measures: [visits.returning_visitors_count_over_total_visitors_count]
    filters:
      visits.occurred_month: last 6 months
    limit: 500
    width:
    height:
    legend_align:
    stacking:
    x_axis_label:
    x_axis_datetime: true
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min: 0
    y_axis_max:
    hide_points:

  - name: average_events_per_visit
    title: Average events per visit by week
    type: line
    base_view: events
    dimensions: [events.occurred_week]
    measures: [events.events_per_visitor]
    filters:
      events.occurred_date: last 6 months
    limit: 500
    width:
    height:
    legend_align:
    stacking:
    x_axis_label:
    x_axis_datetime: true
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min: 0
    y_axis_max:
    hide_points:

  - name: bounce_rate_by_week
    title: Bounce rate by week
    type: line
    base_view: visits
    dimensions: [visits.occurred_week]
    measures: [visitors.bounce_rate]
    filters:
      visits.occurred_date: last 6 months
    limit: 500
    width:
    height:
    legend_align:
    stacking:
    x_axis_label:
    x_axis_datetime: true
    x_axis_datetime_label:
    x_axis_label_rotation:
    y_axis_orientation:
    y_axis_combined:
    y_axis_labels:
    y_axis_min: 0
    y_axis_max:
    hide_points:
    
  - name: visits_and_engagement_by_landing_page
    title: Number of visits and engagement levels by landing page
    type: table
    base_view: visits
    dimensions: [landing_page.landing_page]
    measures: [visits.visits_count, visits.events_per_visit]
    filters:
      visits.occurred_date: last 6 months
    limit: 500
    width:
    height:



