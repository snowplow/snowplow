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
-- OLAP compatible views at visit (session) level of granularity
--
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Yali Sassoon
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0


-- Create schema
CREATE SCHEMA cubes_visits;

-- VIEW 1
-- Simplest visit-level view
CREATE VIEW cubes_visits.basic AS
	SELECT
		domain_userid,
		domain_sessionidx,
		network_userid,
		geo_country,
		geo_region,
		geo_city,
		geo_zipcode,
		geo_latitude,
		geo_longitude,
		MIN(collector_tstamp) AS visit_start_ts,
		MIN(dvce_tstamp) AS dvce_visit_start_ts,
		MAX(dvce_tstamp) AS dvce_visit_finish_ts,
		EXTRACT(EPOCH FROM (MAX(dvce_tstamp) - MIN(dvce_tstamp))) AS visit_duration_s,
		COUNT(*) AS number_of_events,
		COUNT(DISTINCT(page_urlpath)) AS distinct_pages_viewed,
		tr_orderid AS ecomm_orderid
	FROM
		atomic.events
	GROUP BY 1,2,3,4,5,6,7,8,9, tr_orderid;

-- VIEW 2
-- Referer data returned in a format that makes it easy to join with the visits view above
CREATE VIEW cubes_visits.referer_basic AS
	SELECT
		domain_userid,
		domain_sessionidx,
		network_userid,
		mkt_source,
		mkt_medium,
		mkt_campaign,
		mkt_term,
		refr_source,
		refr_medium,
		refr_term,
		refr_urlhost,
		refr_urlpath
	FROM
		atomic.events
	WHERE 
		refr_medium != 'internal'
		AND refr_medium IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;


-- VIEW 3
-- View that joins refer data (view 2) with visit data (view 1)
CREATE VIEW cubes_visits.referer AS
	SELECT
		v.domain_userid,
		v.domain_sessionidx,
		v.network_userid,
		v.geo_country,
		v.geo_region,
		v.geo_city,
		v.geo_zipcode,
		v.geo_latitude,
		v.geo_longitude,
		r.mkt_source,
		r.mkt_medium,
		r.mkt_campaign,
		r.mkt_term,
		r.refr_medium,
		r.refr_term,
		r.refr_urlhost,
		r.refr_urlpath,
		v.visit_start_ts,
		v.visit_duration_s,
		v.number_of_events,
		v.distinct_pages_viewed,
		v.ecomm_orderid
	FROM 
		cubes_visits.basic v
	LEFT JOIN cubes_visits.referer_basic r
	ON v.domain_userid = r.domain_userid
	AND v.domain_sessionidx = r.domain_sessionidx;

-- VIEW 4
-- Entry and exit pages by visit
CREATE VIEW cubes_visits.entry_and_exit_pages AS
	SELECT
		v.domain_userid,
		v.domain_sessionidx,
		v.network_userid,
		v.geo_country,
		v.geo_region,
		v.geo_city,
		v.geo_zipcode,
		v.geo_latitude,
		v.geo_longitude,
		v.visit_start_ts,
		v.visit_duration_s,
		e1.page_urlhost AS entry_page_host,
		e1.page_urlpath AS entry_page_path,
		e2.page_urlhost AS exit_page_host,
		e2.page_urlpath AS exit_page_path,
		v.number_of_events,
		v.distinct_pages_viewed,
		v.ecomm_orderid
	FROM
		cubes_visits.basic v
		LEFT JOIN atomic.events e1
			ON v.domain_userid = e1.domain_userid
			AND v.domain_sessionidx = e1.domain_sessionidx
			AND v.dvce_visit_start_ts = e1.dvce_tstamp
		LEFT JOIN atomic.events e2
			ON v.domain_userid = e2.domain_userid
			AND v.domain_sessionidx = e2.domain_sessionidx
			AND v.dvce_visit_finish_ts = e2.dvce_tstamp;


-- VIEW 5
-- Consolidated table with visits data (VIEW 3) and entry / exit page data (VIEW 4)
CREATE VIEW cubes_visits.referer_entries_and_exits AS
	SELECT
		a.*,
		b.entry_page_host,
		b.entry_page_path,
		b.exit_page_host,
		b.exit_page_path
	FROM
		cubes_visits.referer a
		LEFT JOIN cubes_visits.entry_and_exit_pages b
	ON a.domain_userid = b.domain_userid
	AND a.domain_sessionidx = b.domain_sessionidx;
