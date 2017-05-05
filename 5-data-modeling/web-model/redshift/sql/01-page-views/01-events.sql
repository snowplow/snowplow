-- Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     0.1.0
--
-- Authors:     Christophe Bogaert
-- Copyright:   Copyright (c) 2016 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

DROP TABLE IF EXISTS scratch.web_events;
CREATE TABLE scratch.web_events
  DISTKEY(page_view_id)
  SORTKEY(page_view_id)
AS (

  -- select the relevant dimensions from atomic.events

  WITH prep AS (

    SELECT

      ev.user_id,
      ev.domain_userid,
      ev.network_userid,

      ev.domain_sessionid,
      ev.domain_sessionidx,

      wp.page_view_id,

      ev.page_title,

      ev.page_urlscheme,
      ev.page_urlhost,
      ev.page_urlport,
      ev.page_urlpath,
      ev.page_urlquery,
      ev.page_urlfragment,

      ev.refr_urlscheme,
      ev.refr_urlhost,
      ev.refr_urlport,
      ev.refr_urlpath,
      ev.refr_urlquery,
      ev.refr_urlfragment,

      ev.refr_medium,
      ev.refr_source,
      ev.refr_term,

      ev.mkt_medium,
      ev.mkt_source,
      ev.mkt_term,
      ev.mkt_content,
      ev.mkt_campaign,
      ev.mkt_clickid,
      ev.mkt_network,

      ev.geo_country,
      ev.geo_region,
      ev.geo_region_name,
      ev.geo_city,
      ev.geo_zipcode,
      ev.geo_latitude,
      ev.geo_longitude,
      ev.geo_timezone,

      ev.user_ipaddress,

      ev.ip_isp,
      ev.ip_organization,
      ev.ip_domain,
      ev.ip_netspeed,

      ev.app_id,

      ev.useragent,
      ev.br_name,
      ev.br_family,
      ev.br_version,
      ev.br_type,
      ev.br_renderengine,
      ev.br_lang,
      ev.dvce_type,
      ev.dvce_ismobile,

      ev.os_name,
      ev.os_family,
      ev.os_manufacturer,
      ev.os_timezone,

      ev.name_tracker, -- included to filter on
      ev.dvce_created_tstamp -- included to sort on

    FROM atomic.events AS ev

    INNER JOIN scratch.web_page_context AS wp -- an INNER JOIN guarantees that all rows have a page view ID
      ON ev.event_id = wp.root_id

    WHERE ev.platform = 'web' AND ev.event_name = 'page_view' -- filtering on page view events removes the need for a FIRST_VALUE function

  )

  -- more than one page view event per page view ID? select the first one

  SELECT * FROM (SELECT *, ROW_NUMBER () OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp) AS n FROM prep) WHERE n = 1

);
