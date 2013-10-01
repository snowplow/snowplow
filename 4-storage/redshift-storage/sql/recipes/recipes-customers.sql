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