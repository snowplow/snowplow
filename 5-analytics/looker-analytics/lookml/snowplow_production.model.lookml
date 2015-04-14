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

- connection: snowplow_production

- scoping: true                  # for backward compatibility
- include: "*.view.lookml"       # include all the views
- include: "*.dashboard.lookml"  # include all the dashboards

- explore: events
  joins:
  - join: sessions
    sql_on: |
      events.domain_userid = sessions.domain_userid AND
      events.domain_sessionidx = sessions.domain_sessionidx
    relationship: many_to_one
  - join: visitors
    sql_on: |
      events.domain_userid = visitors.domain_userid
    relationship: many_to_one

- explore: sessions
  joins: 
  - join: visitors
    sql_on: |
      sessions.domain_userid = visitors.domain_userid
    relationship: many_to_one

- explore: visitors