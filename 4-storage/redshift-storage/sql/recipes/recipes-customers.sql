CREATE SCHEMA customer_recipes;


-- Map different user identifiers to one another
CREATE VIEW user_id_map AS
SELECT
domain_userid,
network_userid,
user_id,
user_ipaddress,
user_fingerprint
FROM "atomic".events
WHERE domain_userid IS NOT NULL
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;


-- CUSTOMER LIFETIME VALUE
-- Total transaction value by domain_userid over time
CREATE VIEW total_transaction_value_by_user AS
SELECT
domain_userid,
SUM(tr_total) AS "total_transaction_value_by_user"
FROM (
	SELECT
	domain_userid,
	tr_orderid,
	tr_total
	FROM "atomic".events
	WHERE event='transaction'
	GROUP BY 1,2,3 ) AS t -- deduped transaction table
GROUP BY 1


-- MEASURING USER ENGAGEMENT
-- Users by number of days per month visit website
CREATE VIEW engagement_users_by_days_per_month_visit_website AS
SELECT
"Month",
"Days_visited_website",
COUNT(domain_userid) AS "Frequency"
FROM (
	SELECT
	DATE_TRUNC('month', collector_tstamp) AS "Month",
	domain_userid,
	COUNT(DISTINCT(DATE_TRUNC('day', collector_tstamp))) AS "Days_visited_website"
	FROM "atomic".events
	GROUP BY 1,2 ) t
GROUP BY 1,2
ORDER BY 1,2;


-- Users by number of days per week visit website
CREATE VIEW engagement_users_by_days_per_week_visit_website AS
SELECT
"Week",
"Days_visited_website",
COUNT(domain_userid) AS "Frequency"
FROM (
	SELECT
	DATE_TRUNC('week', collector_tstamp) AS "Week",
	domain_userid,
	COUNT(DISTINCT(DATE_TRUNC('day', collector_tstamp))) AS "Days_visited_website"
	FROM "atomic".events
	GROUP BY 1,2 ) t
GROUP BY 1,2
ORDER BY 1,2;


-- Users by number of visits per month
CREATE VIEW engagement_users_by_visits_per_month
SELECT
"Month",
"Visits_per_month",
COUNT(*) AS "Frequency"
FROM (
	SELECT
	DATE_TRUNC('month', collector_tstamp) AS "Month",
	domain_userid,
	COUNT(DISTINCT(domain_sessionidx)) AS "Visits_per_month"
	FROM "atomic".events
	GROUP BY 1,2 
) t
GROUP BY 1,2
ORDER BY 1,2;

-- Users by number of visits per week

