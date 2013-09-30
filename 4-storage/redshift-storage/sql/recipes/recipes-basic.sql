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

-- Visits by country
SELECT
geo_country AS "Country",
COUNT(DISTINCT(domain_userid)) as "Visitors"
FROM "atomic".events
WHERE collector_tstamp > current_date - integer '31'
GROUP BY 1
ORDER BY 2 DESC;

-- Behavior: new vs returning
select
domain_userid,
domain_sessionidx,
min(collector_tstamp) as time_first_touch,
case when domain_sessionidx = 1 then 'new' else 'returning' end as "new_vs_returning"
from events
where collector_tstamp > current_date - integer '31'
group by domain_userid, domain_sessionidx;
