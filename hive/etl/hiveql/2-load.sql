SET hive.exec.dynamic.partition=true ;

INSERT OVERWRITE TABLE `events`
PARTITION (dt='${DATA_DATE}', user_id)
SELECT
tm,
txn_id,
user_ipaddress,
visit_id,
page_url,
page_title,
page_referrer,
mkt_source,
mkt_medium,
mkt_term,
mkt_content,
mkt_name,
ev_category,
ev_action,
ev_label,
ev_property,
ev_value,
br_name,
br_family,
br_version,
br_type,
br_renderengine,
br_lang,
br_features,
br_cookies,
os_name,
os_family,
os_manufacturer,
dvce_type,
dvce_ismobile,
dvce_screenwidth,
dvce_screenheight,
user_id
FROM `extracted_logs_${RUN_DATE}`
WHERE dt='${DATA_DATE}' ;
