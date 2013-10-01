CREATE SCHEMA customer_recipes;


-- Map different user identifiers to one another
CREATE VIEW customer_recipes.user_id_map AS
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
CREATE VIEW customer_recipes.total_transaction_value_by_user AS
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
GROUP BY 1;


-- MEASURING USER ENGAGEMENT
-- Users by number of days per month visit website
CREATE VIEW customer_recipes.engagement_users_by_days_p_month_on_site AS
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
CREATE VIEW customer_recipes.engagement_users_by_days_p_week_on_site AS
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
CREATE VIEW customer_recipes.engagement_users_by_visits_per_month AS 
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
CREATE VIEW customer_recipes.engagement_users_by_visits_per_week AS
SELECT
"Week",
"Visits_per_week",
COUNT(*) AS "Frequency"
FROM (
	SELECT
	DATE_TRUNC('week', collector_tstamp) AS "Week",
	domain_userid,
	COUNT(DISTINCT(domain_sessionidx)) AS "Visits_per_week"
	FROM "atomic".events
	GROUP BY 1,2 
) t
GROUP BY 1,2
ORDER BY 1,2;


-- COHORT ANALYSIS
-- STAGE 1. Assigning users to cohorts

-- Cohort based on month that user first visited website
CREATE VIEW customer_recipes.cohort_user_map_month_first_touch_website AS
SELECT
domain_userid,
DATE_TRUNC('month', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
GROUP BY domain_userid;

-- Cohort based on week that user first touched website
CREATE VIEW customer_recipes.cohort_user_map_week_first_touch_website AS
SELECT
domain_userid,
DATE_TRUNC('week', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
GROUP BY domain_userid;

-- Cohort based on time first signed up (se_action = 'sign-up')
CREATE VIEW customer_recipes.cohort_user_map_month_signed_up AS
SELECT
domain_userid,
DATE_TRUNC('week', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
WHERE se_action = 'sign-up'
GROUP BY domain_userid;

-- STAGE 2. Metrics by user

-- Retention by month by user
CREATE VIEW customer_recipes.retention_by_user_by_month AS
SELECT
domain_userid,
DATE_TRUNC('month', collector_tstamp) AS months_active
FROM "atomic".events
GROUP BY 1,2;

CREATE VIEW customer_recipes.retention_by_user_by_week AS
SELECT
domain_userid,
DATE_TRUNC('week', collector_tstamp) AS weeks_active
FROM "atomic".events
GROUP BY 1,2;

-- STAGE 3: combine views in 1 and 2 to perform cohort analysis

-- Cohort analysis: retention by month
CREATE VIEW customer_recipes.cohort_retention_by_month AS
SELECT
cohort,
months_active,
rank() OVER (PARTITION BY cohort ORDER BY months_active ASC) AS "Month",
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM customer_recipes.cohort_user_map_week_first_touch_website c
JOIN customer_recipes.retention_by_user_by_month m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;


