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
-- OLAP-compatible views at transaction level of granularity
--
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Yali Sassoon
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0


-- NOTE: This needs to be run *after* `cube-visits.sql`

-- Create schema
CREATE SCHEMA cubes_ecomm;

-- VIEW 1
-- Basic data by transaction
CREATE VIEW cubes_ecomm.transactions_basic AS
	SELECT
		domain_userid,
		domain_sessionidx,
		network_userid,
		tr_orderid,
		tr_affiliation,
		tr_total,
		tr_tax,
		tr_shipping,
		tr_city,
		tr_state,
		tr_country
	FROM 
		atomic.events
	WHERE
		event = 'transaction'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

-- VIEW 2
-- Item data by transaction
CREATE VIEW cubes_ecomm.transactions_items_basic AS
	SELECT 
		ti_orderid,
		SUM(ti_quantity) AS "number_of_products_bought",
		COUNT(DISTINCT(ti_sku)) AS "number_of_distinct_products_bought"
	FROM 
		"atomic".events
	WHERE 
		event = 'transaction_item'
	GROUP BY 
		ti_orderid;

-- VIEW 3
-- Consolidate data in View 1 and View 2
CREATE VIEW cubes_ecomm.transactions AS
	SELECT
		t.*,
		i."number_of_products_bought",
		i."number_of_distinct_products_bought"
	FROM
		cubes_ecomm.transactions_basic t
	JOIN
		cubes_ecomm.transactions_items_basic i
	ON t.tr_orderid = i.ti_orderid;

-- VIEW 4
-- Consolidate data in view 3 with visit level data
CREATE VIEW cubes_ecomm.transactions_with_visits AS
	SELECT
	v.*,
	e.tr_orderid,
	e.tr_affiliation,
	e.tr_total,
	e.tr_tax,
	e.tr_shipping,
	e.tr_city,
	e.tr_state,
	e.tr_country,
	e.number_of_products_bought,
	e.number_of_distinct_products_bought
	FROM
	cubes_visits.referer_entries_and_exits v
	LEFT JOIN cubes_ecomm.transactions e
	ON v.domain_userid = e.domain_userid
	AND v.domain_sessionidx = v.domain_sessionidx;
