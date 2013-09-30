CREATE SCHEMA basic_recipes;


-- Uniques and visits by day
CREATE VIEW basic_recipes.uniques_and_visits_by_day AS
SELECT
DATE_TRUNC('day', collector_tstamp) as "Date",
COUNT(distinct(domain_userid)) as "Uniques",
COUNT(distinct(domain_userid || '-' || domain_sessionidx)) as "Visits"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
GROUP BY 1
ORDER BY 1;


-- Pageviews by day
CREATE VIEW basic_recipes.pageviews_by_day AS
SELECT 
DATE_TRUNC('day', collector_tstamp) AS "Date",
COUNT(*) AS "page_views"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
AND event = 'page_view'
GROUP BY 1
ORDER BY 1;


-- Events by day by type
CREATE VIEW basic_recipes.events_by_day AS
SELECT
DATE_TRUNC('day', collector_tstamp) AS "Date",
event,
COUNT(*) AS "Number"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
GROUP BY 1,2
ORDER BY 1,2;


-- Pages per visit (frequency table for last month of data)
CREATE VIEW basic_recipes.pages_per_visit AS
SELECT
pages_visited,
COUNT(*) as "frequency"
FROM (
	SELECT
	domain_userid || '-' || domain_sessionidx AS "session",
	COUNT(*) as "pages_visited"
	FROM "atomic".events
	WHERE event = 'page_view'
	AND collector_tstamp > current_date - integer '31' 
	GROUP BY session
) AS page_view_per_visit
GROUP BY 1
ORDER BY 1;


-- Bounce rate by day
CREATE VIEW AS basic_recipes.bounce_rate_by_day AS 
SELECT
DATE_TRUNC('day', time_first_touch) AS "Date",
SUM(bounces)::REAL/COUNT(*) as "Bounce rate"
FROM (
	SELECT
	domain_userid,
	domain_sessionidx,
	MIN(collector_tstamp) as "time_first_touch",
	COUNT(*) as "number_of_events",
	CASE WHEN count(*) = 1 THEN 1 ELSE 0 END AS bounces
	FROM "atomic".events
	WHERE collector_tstamp > current_date - integer '31'
	GROUP BY 1,2
) v
GROUP BY 1
ORDER BY 1;


-- % New visits
CREATE VIEW basic_recipes.fraction_new_visits_by_day AS
SELECT
DATE_TRUNC('day', time_first_touch) AS "Date",
SUM(first_visit)::REAL/COUNT(*) as "fraction_of_visits_that_are_new"
FROM (
	SELECT
	MIN(collector_tstamp) AS "time_first_touch",
	domain_userid, 
	domain_sessionidx,
	CASE WHEN domain_sessionidx = 1 THEN 1 ELSE 0 END AS "first_visit"
	FROM "atomic".events
	WHERE collector_tstamp > current_date - integer '31'
	GROUP BY domain_userid, domain_sessionidx) v
GROUP BY 1
ORDER BY 1;


-- Average visit duration
CREATE VIEW basic_recipes.avg_visit_duration_by_day AS 
SELECT
DATE_TRUNC('day', start_time) AS "Date",
AVG(duration)/1000000 as "average_visit_duration_seconds"
FROM (
	SELECT
	domain_userid,
	domain_sessionidx,
	MIN(collector_tstamp) as "start_time",
	MAX(collector_tstamp) as "finish_time",
	MAX(collector_tstamp) - min(collector_tstamp) as "duration"
	FROM "atomic".events
	WHERE collector_tstamp > current_date - integer '31'
	GROUP BY 1,2
) v
group by 1
order by 1;


-- Demographics: language
CREATE VIEW AS basic_recipes.visitors_by_language AS
SELECT
br_lang,
COUNT(DISTINCT(domain_userid)) as "visitors"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
GROUP BY br_lang
ORDER BY 2 DESC;


-- Demographics: location
CREATE VIEW basic_recipes.visits_by_country AS 
SELECT
geo_country AS "Country",
COUNT(DISTINCT(domain_userid)) as "Visitors"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
GROUP BY 1
ORDER BY 2 DESC;


-- Behavior: new vs returning
CREATE VIEW basic_recipes.new_vs_returning AS
SELECT
DATE_TRUNC('day', time_first_touch) AS "Date",
SUM(first_visit)::REAL/COUNT(*) as "fraction_of_visits_that_are_new"
FROM (
	SELECT
	MIN(collector_tstamp) AS "time_first_touch",
	domain_userid, 
	domain_sessionidx,
	CASE WHEN domain_sessionidx = 1 THEN 1 ELSE 0 END AS "first_visit"
	FROM "atomic".events
	WHERE collector_tstamp > current_date - integer '31'
	GROUP BY domain_userid, domain_sessionidx) v
GROUP BY 1
ORDER BY 1;


-- Behavior: frequency
CREATE VIEW basic_recipes.behavior_frequency AS
SELECT
domain_sessionidx as "Number of visits",
COUNT(DISTINCT(domain_userid)) as "Frequency"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
GROUP BY 1
ORDER BY 1;

-- Behavior: recency
CREATE VIEW basic_recipes.behavior_recency AS
SELECT
"Days between visits",
COUNT(*) as "Number of visits"
FROM (
	SELECT
	n.domain_userid,
	n.domain_sessionidx,
	EXTRACT(EPOCH FROM (n.time_first_touch - p.time_first_touch))/3600/24 as "days_between_visits",
	CASE
		WHEN n.domain_sessionidx = 1 THEN '0'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 1 THEN '1'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 2 THEN '2'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 3 THEN '3'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 4 THEN '4'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 5 THEN '5'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 10 THEN '6-10'
		WHEN extract(epoch FROM (n.time_first_touch - p.time_first_touch))/3600/24 < 25 THEN '11-25'
		ELSE '25+' END as "Days between visits"
	FROM (
		SELECT
		domain_userid,
		domain_sessionidx,
		domain_sessionidx - 1 as "previous_domain_sessionidx",
		MIN(collector_tstamp) as "time_first_touch"
		FROM "atomic".events
		WHERE collector_tstamp > current_date - integer '31'
		GROUP BY 1,2
	) n
	LEFT JOIN (
		SELECT
		domain_userid,
		domain_sessionidx,
		MIN(collector_tstamp) as "time_first_touch"
		FROM "atomic".events
		GROUP BY 1,2
	) p ON n.previous_domain_sessionidx = p.domain_sessionidx
	AND n.domain_userid = p.domain_userid
) t
GROUP BY 1
ORDER BY 1;