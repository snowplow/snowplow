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
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Yali Sassoon
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0


-- Create the schema
CREATE SCHEMA recipes_customer;


-- Map different user identifiers to one another
CREATE VIEW recipes_customer.id_map_domain_to_network AS
SELECT
domain_userid,
network_userid
FROM "atomic".events
WHERE domain_userid IS NOT NULL
AND network_userid IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_domain_to_user AS
SELECT
domain_userid,
user_id
FROM "atomic".events
WHERE domain_userid IS NOT NULL
AND user_id IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_domain_to_ipaddress AS
SELECT
domain_userid,
user_ipaddress
FROM "atomic".events
WHERE domain_userid IS NOT NULL
AND user_ipaddress IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_domain_to_fingerprint AS
SELECT
domain_userid,
user_fingerprint
FROM "atomic".events
WHERE domain_userid IS NOT NULL
AND user_fingerprint IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_network_to_user AS
SELECT
network_userid,
user_id
FROM "atomic".events
WHERE network_userid IS NOT NULL
AND user_id IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_network_to_ipaddress AS
SELECT
network_userid,
user_ipaddress
FROM "atomic".events
WHERE network_userid IS NOT NULL
AND user_ipaddress IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_network_to_fingerprint AS
SELECT
network_userid,
user_fingerprint
FROM "atomic".events
WHERE network_userid IS NOT NULL
AND user_fingerprint IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_user_to_ipaddress AS
SELECT
user_id,
user_ipaddress
FROM "atomic".events
WHERE user_id IS NOT NULL
AND user_ipaddress IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_user_to_fingerprint AS
SELECT
user_id,
user_fingerprint
FROM "atomic".events
WHERE user_id IS NOT NULL
AND user_ipaddress IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

CREATE VIEW recipes_customer.id_map_ipaddress_to_fingerprint AS
SELECT
user_ipaddress,
user_fingerprint
FROM "atomic".events
WHERE user_ipaddress IS NOT NULL
AND user_ipaddress IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;



-- CUSTOMER LIFETIME VALUE
-- Total transaction value by domain_userid over time
CREATE VIEW recipes_customer.clv_total_transaction_value_by_user_by_month AS
SELECT
domain_userid,
month,
SUM(tr_total) AS "total_transaction_value_by_user"
FROM (
	SELECT
	domain_userid,
	DATE_TRUNC('month', collector_tstamp) AS month,
	tr_orderid,
	tr_total
	FROM "atomic".events
	WHERE event='transaction'
	GROUP BY 1,2,3,4 ) AS t -- deduped transaction table
GROUP BY 1,2;

CREATE VIEW recipes_customer.clv_total_transaction_value_by_user_by_week AS
SELECT
domain_userid,
week,
SUM(tr_total) AS "total_transaction_value_by_user"
FROM (
	SELECT
	domain_userid,
	DATE_TRUNC('week', collector_tstamp) AS week,
	tr_orderid,
	tr_total
	FROM "atomic".events
	WHERE event='transaction'
	GROUP BY 1,2,3,4 ) AS t -- deduped transaction table
GROUP BY 1,2;


-- MEASURING USER ENGAGEMENT
-- Users by number of days per month visit website
CREATE VIEW recipes_customer.engagement_users_by_days_p_month_on_site AS
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
CREATE VIEW recipes_customer.engagement_users_by_days_p_week_on_site AS
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
CREATE VIEW recipes_customer.engagement_users_by_visits_per_month AS 
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
CREATE VIEW recipes_customer.engagement_users_by_visits_per_week AS
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

-- Cohort based on time that user first visited website
CREATE VIEW recipes_customer.cohort_dfn_by_month_first_touch_website AS
SELECT
domain_userid,
DATE_TRUNC('month', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
GROUP BY 1;

CREATE VIEW recipes_customer.cohort_dfn_by_week_first_touch_website AS
SELECT
domain_userid,
DATE_TRUNC('week', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
GROUP BY domain_userid;

-- Cohort based on time first signed up (se_action = 'sign-up')
CREATE VIEW recipes_customer.cohort_dfn_by_month_signed_up AS
SELECT
domain_userid,
DATE_TRUNC('month', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
WHERE se_action = 'sign-up'
GROUP BY domain_userid;

CREATE VIEW recipes_customer.cohort_dfn_by_week_signed_up AS
SELECT
domain_userid,
DATE_TRUNC('week', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
WHERE se_action = 'sign-up'
GROUP BY domain_userid;

-- Cohort based on time first transacted 
CREATE VIEW recipes_customer.cohort_dfn_by_month_first_transact AS
SELECT
domain_userid,
DATE_TRUNC('month', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
WHERE event = 'transaction'
GROUP BY domain_userid;

CREATE VIEW recipes_customer.cohort_dfn_by_week_first_transact AS
SELECT
domain_userid,
DATE_TRUNC('week', MIN(collector_tstamp)) AS cohort
FROM "atomic".events
WHERE event = 'transaction'
GROUP BY domain_userid;

-- Cohort based on marketing channel (paid) first acquired
CREATE VIEW recipes_customer.cohort_dfn_by_paid_channel_acquired_by_month AS
SELECT
domain_userid,
channel_acquired_medium,
channel_acquired_source,
month_acquired
FROM (
	SELECT 
	domain_userid, 
	mkt_medium AS channel_acquired_medium,
	mkt_source AS channel_acquired_source,
	DATE_TRUNC('month', collector_tstamp) AS month_acquired,
 	rank() over (partition by domain_userid order by collector_tstamp) AS r
	FROM "atomic".events
	WHERE mkt_medium IS NOT NULL
	AND mkt_medium != ''
	AND domain_sessionidx = 1
) t
WHERE r = 1;

CREATE VIEW recipes_customer.cohort_dfn_by_paid_channel_acquired_by_week AS
SELECT
domain_userid,
channel_acquired_medium,
channel_acquired_source,
week_acquired
FROM (
	SELECT 
	domain_userid, 
	mkt_medium AS channel_acquired_medium,
	mkt_source AS channel_acquired_source,
	DATE_TRUNC('week', collector_tstamp) AS week_acquired,
 	rank() over (partition by domain_userid order by collector_tstamp) AS r
	FROM "atomic".events
	WHERE mkt_medium IS NOT NULL
	AND mkt_medium != ''
	AND domain_sessionidx = 1
) t
WHERE r = 1;

-- Cohort based on referal channel first acquired
CREATE VIEW recipes_customer.cohort_dfn_by_refr_channel_acquired_by_month AS
SELECT
domain_userid,
refr_acquired_medium,
refr_acquired_source,
month_acquired
FROM (
	SELECT 
	domain_userid, 
	refr_medium AS refr_acquired_medium,
	refr_source AS refr_acquired_source,
	DATE_TRUNC('month', collector_tstamp) AS month_acquired,
 	rank() over (partition by domain_userid order by collector_tstamp) AS r
	FROM "atomic".events
	WHERE refr_medium != 'internal'
) t
WHERE r = 1;


CREATE VIEW recipes_customer.cohort_dfn_by_refr_channel_acquired_by_week AS
SELECT
domain_userid,
refr_acquired_medium,
refr_acquired_source,
week_acquired
FROM (
	SELECT 
	domain_userid, 
	refr_medium AS refr_acquired_medium,
	refr_source AS refr_acquired_source,
	DATE_TRUNC('week', collector_tstamp) AS week_acquired,
 	rank() over (partition by domain_userid order by collector_tstamp) AS r
	FROM "atomic".events
	WHERE refr_medium != 'internal'
) t
WHERE r = 1;



-- STAGE 2. Metrics by user

-- Retention by month by user
CREATE VIEW recipes_customer.retention_by_user_by_month AS
SELECT
domain_userid,
DATE_TRUNC('month', collector_tstamp) AS months_active
FROM "atomic".events
GROUP BY 1,2;

-- Retention by week by user
CREATE VIEW recipes_customer.retention_by_user_by_week AS
SELECT
domain_userid,
DATE_TRUNC('week', collector_tstamp) AS weeks_active
FROM "atomic".events
GROUP BY 1,2;

-- Also refer to CLV and engagement metrics

-- STAGE 3: combine views in 1 and 2 to perform cohort analysis

-- Cohort analysis: retention by month, by first touch
CREATE VIEW recipes_customer.cohort_retention_by_month_first_touch AS
SELECT
cohort,
months_active AS month_actual,
rank() OVER (PARTITION BY cohort ORDER BY months_active ASC) AS month_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_month_first_touch_website c
JOIN recipes_customer.retention_by_user_by_month m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;

-- Cohort analysis: retention by week, by first touch
CREATE VIEW recipes_customer.cohort_retention_by_week_first_touch AS
SELECT
cohort,
weeks_active AS weeks_actual,
rank() OVER (PARTITION BY cohort ORDER BY weeks_active ASC) AS weeks_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_week_first_touch_website c
JOIN recipes_customer.retention_by_user_by_week m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;

-- Cohort analysis: retention by month, by first signup
CREATE VIEW recipes_customer.cohort_retention_by_month_signed_up AS
SELECT
cohort,
months_active AS month_actual,
rank() OVER (PARTITION BY cohort ORDER BY months_active ASC) AS month_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_month_signed_up c
JOIN recipes_customer.retention_by_user_by_month m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;

-- Cohort analysis: retention by week, by first signup
CREATE VIEW recipes_customer.cohort_retention_by_week_signed_up AS
SELECT
cohort,
weeks_active AS weeks_actual,
rank() OVER (PARTITION BY cohort ORDER BY weeks_active ASC) AS weeks_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_week_signed_up c
JOIN recipes_customer.retention_by_user_by_week m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;


-- Cohort analysis: retention by month, by first transaction
CREATE VIEW recipes_customer.cohort_retention_by_month_first_transact AS
SELECT
cohort,
months_active AS month_actual,
rank() OVER (PARTITION BY cohort ORDER BY months_active ASC) AS month_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_month_first_transact c
JOIN recipes_customer.retention_by_user_by_month m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;

-- Cohort analysis: retention by week, by first signup
CREATE VIEW recipes_customer.cohort_retention_by_week_first_transact AS
SELECT
cohort,
weeks_active AS weeks_actual,
rank() OVER (PARTITION BY cohort ORDER BY weeks_active ASC) AS weeks_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY cohort))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_week_first_transact c
JOIN recipes_customer.retention_by_user_by_week m
ON c.domain_userid = m.domain_userid
GROUP BY 1,2
ORDER BY 1,2;

-- Cohort analysis: retention by marketing channel acquired
CREATE VIEW recipes_customer.cohort_retention_by_month_by_paid_channel_acquired AS
SELECT 
channel_acquired_medium,
channel_acquired_source,
month_acquired,
months_active AS month_actual,
rank() OVER (PARTITION BY channel_acquired_medium, channel_acquired_source, month_acquired ORDER BY months_active ASC) AS month_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY channel_acquired_medium, channel_acquired_source, month_acquired))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_paid_channel_acquired_by_month c 
JOIN recipes_customer.retention_by_user_by_month m 
ON c.domain_userid = m.domain_userid
WHERE months_active >= month_acquired
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

CREATE VIEW recipes_customer.cohort_retention_by_week_by_paid_channel_acquired AS
SELECT 
channel_acquired_medium,
channel_acquired_source,
week_acquired,
weeks_active AS week_actual,
rank() OVER (PARTITION BY channel_acquired_medium, channel_acquired_source, week_acquired ORDER BY weeks_active ASC) AS week_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY channel_acquired_medium, channel_acquired_source, week_acquired))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_paid_channel_acquired_by_week c 
JOIN recipes_customer.retention_by_user_by_week m 
ON c.domain_userid = m.domain_userid
WHERE weeks_active >= week_acquired
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;


-- Cohort analysis: retention by referer
CREATE VIEW recipes_customer.cohort_retention_by_month_by_refr_acquired AS
SELECT 
refr_acquired_medium,
refr_acquired_source,
month_acquired,
months_active AS month_actual,
rank() OVER (PARTITION BY refr_acquired_medium, refr_acquired_source, month_acquired ORDER BY months_active ASC) AS month_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY refr_acquired_medium, refr_acquired_source, month_acquired))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_refr_channel_acquired_by_month c 
JOIN recipes_customer.retention_by_user_by_month m 
ON c.domain_userid = m.domain_userid
WHERE months_active >= month_acquired
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

CREATE VIEW recipes_customer.cohort_retention_by_week_by_refr_acquired AS
SELECT 
refr_acquired_medium,
refr_acquired_source,
week_acquired,
weeks_active AS week_actual,
rank() OVER (PARTITION BY refr_acquired_medium, refr_acquired_source, week_acquired ORDER BY weeks_active ASC) AS week_rank,
COUNT(DISTINCT(m.domain_userid)) AS uniques,
COUNT(DISTINCT(m.domain_userid)) / (first_value(COUNT(DISTINCT(m.domain_userid))) OVER (PARTITION BY refr_acquired_medium, refr_acquired_source, week_acquired))::REAL AS fraction_retained
FROM recipes_customer.cohort_dfn_by_refr_channel_acquired_by_week c 
JOIN recipes_customer.retention_by_user_by_week m 
ON c.domain_userid = m.domain_userid
WHERE weeks_active >= week_acquired
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;


