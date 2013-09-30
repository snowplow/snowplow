-- OLAP compatible view at page level of granularity
CREATE SCHEMA pages;

-- VIEW 1
-- Simplest page-level view (aggregated by page per session)
CREATE VIEW pages.basic AS
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
FROM atomic.events
GROUP BY 1,2,3,4,5;

-- VIEW 2
-- Page views by page by session
CREATE VIEW pages.views_by_session AS
SELECT
page_urlscheme,
page_urlhost,
page_urlpath,
domain_userid,
domain_sessionidx,
COUNT(*) AS pageviews_by_session
FROM atomic.events
WHERE event = 'page_view'
GROUP BY 1,2,3,4,5;

-- VIEW 3
-- Page pings by page by session
CREATE VIEW pages.pings_by_session AS
SELECT
page_urlscheme,
page_urlhost,
page_urlpath,
domain_userid,
domain_sessionidx,
COUNT(*) AS pagepings_by_session
FROM atomic.events
WHERE event = 'page_ping'
GROUP BY 1,2,3,4,5;

-- VIEW 4
-- Consolidate data from above 3 views (pages per session)
CREATE VIEW pages.complete AS
SELECT
basic.*,
v.pageviews_by_session,
pp.pagepings_by_session
FROM
pages.basic basic
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