-- Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
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
-- OLAP compatible views at page level of granularity
--
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Yali Sassoon
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0


-- Create schema
CREATE SCHEMA cubes_pages;

-- PART 1: General page-level analytics

-- VIEW 1
-- Simplest page-level view (aggregated by page per session)
CREATE VIEW cubes_pages.basic AS
	SELECT
		page_urlscheme,
		page_urlhost,
		page_urlpath,
		domain_userid,
		domain_sessionidx,
		COUNT(*) AS number_of_events,
		MIN(pp_xoffset_min) AS pp_xoffset_min,
		MAX(pp_xoffset_max) AS pp_xoffset_max,
		MIN(pp_yoffset_min) AS pp_yoffset_min,
		MAX(pp_yoffset_max) AS pp_yoffset_max,
		MAX(doc_width) AS doc_width,
		MAX(doc_height) AS doc_height,
		AVG(dvce_screenwidth) AS dvce_screenwidth,
		AVG(dvce_screenheight) AS dvce_screenheight
	FROM
		atomic.events
	GROUP BY 1,2,3,4,5;

-- VIEW 2
-- Page views by page by session
CREATE VIEW cubes_pages.views_by_session AS
	SELECT
		page_urlscheme,
		page_urlhost,
		page_urlpath,
		domain_userid,
		domain_sessionidx,
		COUNT(*) AS pageviews_by_session
	FROM
		atomic.events
	WHERE
		event = 'page_view'
	GROUP BY 1,2,3,4,5;

-- VIEW 3
-- Page pings by page by session
CREATE VIEW cubes_pages.pings_by_session AS
	SELECT
		page_urlscheme,
		page_urlhost,
		page_urlpath,
		domain_userid,
		domain_sessionidx,
		COUNT(*) AS pagepings_by_session
	FROM
		atomic.events
	WHERE
		event = 'page_ping'
	GROUP BY 1,2,3,4,5;

-- VIEW 4
-- Consolidate data from above 3 views (pages per session)
CREATE VIEW cubes_pages.complete AS
	SELECT
		basic.*,
		v.pageviews_by_session,
		pp.pagepings_by_session
	FROM
		cubes_pages.basic basic
		LEFT JOIN pages.views_by_session AS v 
			ON basic.page_urlscheme = v.page_urlscheme
			AND basic.page_urlhost = v.page_urlhost
			AND basic.page_urlpath = v.page_urlpath
			AND basic.domain_userid = v.domain_userid
			AND basic.domain_sessionidx = v.domain_sessionidx
		LEFT JOIN pages.pings_by_session AS pp 
			ON basic.page_urlscheme = pp.page_urlscheme
			AND basic.page_urlhost = pp.page_urlhost
			AND basic.page_urlpath = pp.page_urlpath
			AND basic.domain_userid = pp.domain_userid
			AND basic.domain_sessionidx = pp.domain_sessionidx;

-- PART 2: LANDING PAGE ANALYTICS
CREATE VIEW recipes_catalog.traffic_driven_to_site_per_page_per_week AS
	SELECT
		page_urlpath AS "page",
		DATE_TRUNC('week', collector_tstamp) AS week,
		refr_medium,
		refr_source,
		refr_term,
		refr_urlhost,
		refr_urlpath,
		mkt_medium,
		mkt_source,
		mkt_term,
		mkt_campaign,
		mkt_content
		COUNT(*) AS "Landing page views"
	FROM
		"atomic".events
	WHERE "event" = 'page_view'
	AND   "refr_medium" != 'internal'
	GROUP BY 1,2,3,4,5,6
	ORDER BY 7 DESC;

CREATE VIEW recipes_catalog.traffic_driven_to_site_per_page_per_month AS
	SELECT
		page_urlpath AS "page",
		DATE_TRUNC('month', collector_tstamp) AS month,
		refr_medium,
		refr_source,
		refr_term,
		refr_urlhost,
		refr_urlpath,
		mkt_medium,
		mkt_source,
		mkt_term,
		mkt_campaign,
		mkt_content
		COUNT(*) AS "Landing page views"
	FROM
		"atomic".events
	WHERE "event" = 'page_view'
	AND   "refr_medium" != 'internal'
	GROUP BY 1,2,3,4,5,6
	ORDER BY 7 DESC;

