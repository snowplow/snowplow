CREATE EXTERNAL TABLE IF NOT EXISTS `events` (
app_id string,
platform string,
dt_dt string,
tm string,
event string,
event_vendor string, -- New in 0.0.6
event_id string,
txn_id string,
v_tracker string,
v_collector string,
v_etl string,
user_id string,
user_ipaddress string,
user_fingerprint string,
visit_id int,
page_url string,
page_title string,
page_referrer string,
page_urlscheme string, -- New in 0.0.6
page_urlhost string, -- New in 0.0.6
page_urlport int, -- New in 0.0.6
page_urlpath string, -- New in 0.0.6
page_urlquery string, -- New in 0.0.6
page_urlfragment string, -- New in 0.0.6
mkt_source string,
mkt_medium string,
mkt_term string,
mkt_content string,
mkt_campaign string,
ev_category string,
ev_action string,
ev_label string,
ev_property string,
ev_value string,
tr_orderid string,
tr_affiliation string,
tr_total string,
tr_tax string,
tr_shipping string,
tr_city string,
tr_state string,
tr_country string,
ti_orderid string,
ti_sku string,
ti_name string,
ti_category string,
ti_price string,
ti_quantity string,
pp_xoffset_min int, -- New in 0.0.6
pp_xoffset_max int, -- New in 0.0.6
pp_yoffset_min int, -- New in 0.0.6
pp_yoffset_max int, -- New in 0.0.6
useragent string,
br_name string,
br_family string,
br_version string,
br_type string,
br_renderengine string,
br_lang string,
br_features_pdf tinyint,
br_features_flash tinyint,
br_features_java tinyint,
br_features_director tinyint,
br_features_quicktime tinyint,
br_features_realplayer tinyint,
br_features_windowsmedia tinyint,
br_features_gears tinyint,
br_features_silverlight tinyint,
br_cookies tinyint,
br_colordepth string,
br_viewwidth int, -- New in 0.0.6
br_viewheight int, -- New in 0.0.6
os_name string,
os_family string,
os_manufacturer string,
os_timezone string,
dvce_type string,
dvce_ismobile tinyint,
dvce_screenwidth int,
dvce_screenheight int,
doc_charset string, -- New in 0.0.6
doc_width int, -- New in 0.0.6
doc_height int -- New in 0.0.6
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${EVENTS_TABLE}' ;
