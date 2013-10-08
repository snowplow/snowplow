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
CREATE SCHEMA recipes_catalog;


-- PART 1 - PRODUCT ANALYTICS

-- Uniques and page views by page
CREATE VIEW recipes_catalog.uniques_and_pvs_by_page_by_month AS
SELECT 
page_urlpath,
DATE_TRUNC('month', collector_tstamp) AS month,
COUNT(DISTINCT(domain_userid)) AS unique_visitors,
COUNT(*) AS page_views
FROM "atomic".events
WHERE event = 'page_view'
GROUP BY 1,2
ORDER BY 2 DESC;

CREATE VIEW recipes_catalog.uniques_and_pvs_by_page_by_week AS
SELECT 
page_urlpath,
DATE_TRUNC('week', collector_tstamp) AS week,
COUNT(DISTINCT(domain_userid)) AS unique_visitors,
COUNT(*) AS page_views
FROM "atomic".events
WHERE event = 'page_view'
GROUP BY 1,2
ORDER BY 2 DESC;


-- Add to baskets by page
CREATE VIEW recipes_catalog.add_to_baskets_by_page_by_month AS
SELECT
page_urlpath,
DATE_TRUNC('month', collector_tstamp) AS month,
se_label AS product_sku,
COUNT(DISTINCT(domain_userid)) AS uniques_that_add_to_basket,
COUNT(*) AS number_of_add_to_baskets,
SUM(se_property) AS number_of_products_added_to_basket
FROM "atomic".events
WHERE se_action = 'add-to-basket'
GROUP BY 1,2,3
ORDER BY 1,2,2 DESC;

CREATE VIEW recipes_catalog.add_to_baskets_by_page_by_week AS
SELECT
page_urlpath,
DATE_TRUNC('week', collector_tstamp) AS week,
se_label AS product_sku,
COUNT(DISTINCT(domain_userid)) AS uniques_that_add_to_basket,
COUNT(*) AS number_of_add_to_baskets,
SUM(se_property) AS number_of_products_added_to_basket
FROM "atomic".events
WHERE se_action = 'add-to-basket'
GROUP BY 1,2,3
ORDER BY 1,2,2 DESC;


-- Purchases by product  
CREATE VIEW recipes_catalog.purchases_by_product_by_month AS
SELECT
ti_sku,
DATE_TRUNC('month', collector_tstamp) AS month,
COUNT(DISTINCT(domain_userid)) AS uniques_that_purchase,
COUNT(DISTINCT(ti_orderid)) AS number_of_orders,
SUM(ti_quantity) AS actual_number_sold
FROM "atomic".events
WHERE event = 'transaction_item'
GROUP BY 1,2;

CREATE VIEW recipes_catalog.purchases_by_product_by_week AS
SELECT
ti_sku,
DATE_TRUNC('week', collector_tstamp) AS week,
COUNT(DISTINCT(domain_userid)) AS uniques_that_purchase,
COUNT(DISTINCT(ti_orderid)) AS number_of_orders,
SUM(ti_quantity) AS actual_number_sold
FROM "atomic".events
WHERE event = 'transaction_item'
GROUP BY 1,2;


-- Combine the above queries (uniques, pvs, add-to-baskets, purchases) in one
CREATE VIEW recipes_catalog.all_product_metrics_by_month AS
SELECT
	a.page_urlpath,
	a.month,
	b.product_sku,
	a.unique_visitors,
	a.page_views,
	b.uniques_that_add_to_basket,
	b.number_of_add_to_baskets,
	b.number_of_products_added_to_basket,
	c.uniques_that_purchase,
	c.number_of_orders,
	c.actual_number_sold
FROM recipes_catalog.uniques_and_pvs_by_page_by_month a
LEFT JOIN recipes_catalog.add_to_baskets_by_page_by_month b 
ON a.page_urlpath = b.page_urlpath AND a.month = b.month
LEFT JOIN recipes_catalog.purchases_by_product_by_month c
ON b.product_sku = c.ti_sku AND b.month = c.month
ORDER BY 1,2,3;


-- PART 2 - CONTENT PAGE ANALYTICS

