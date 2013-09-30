-- OLAP compatible view at transaction level of granularity
CREATE SCHEMA ecomm;

-- VIEW 1
-- Basic data by transaction
CREATE VIEW ecomm.transactions_basic AS
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
CREATE VIEW ecomm.transactions_items_basic AS
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
CREATE VIEW ecomm.transactions AS
	SELECT
		t.*,
		i."number_of_products_bought",
		i."number_of_distinct_products_bought"
	FROM
		customer_recipes.ecomm_transactions_basic t
	JOIN
		customer_recipes.ecomm_transaction_items_basic i
	ON t.tr_orderid = i.ti_orderid;

-- VIEW 4
-- Consolidate data in view 3 with visit level data
CREATE VIEW ecomm.transactions_with_visits AS
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
	FROM visits.referer_entries_and_exits v
	LEFT JOIN ecomm.transactions e
	ON v.domain_userid = e.domain_userid
	AND v.domain_sessionidx = v.domain_sessionidx;