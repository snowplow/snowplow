-- Copyright (c) 2013-2022 Snowplow Analytics Ltd. All rights reserved.
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
-- Authors: Christophe Bogaert
-- Copyright: Copyright (c) 2015-2022 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: deduplicate
-- Version: 0.3
--
-- Requires atomic.events 0.7.0
--
-- Steps (a) to (c) deduplicate rows that have the same event_id and event_fingerprint. Because these are
-- identical events, the queries keep the earliest event in atomic while all others are moved to duplicates.
--
-- Step (d) is an optional step that moves all remaining duplicates (same event_id but different event_fingerprint)
-- to duplicates. Note that this might delete legitimate events from atomic.
--
-- These SQL queries can be run without modifications.

DROP TABLE IF EXISTS duplicates.tmp_events;
DROP TABLE IF EXISTS duplicates.tmp_events_id;
DROP TABLE IF EXISTS duplicates.tmp_events_id_remaining;

-- (a) list all event_id & event_fingerprint combinations that occur more than once

CREATE TABLE duplicates.tmp_events_id
  DISTKEY (event_id)
  SORTKEY (event_id)
AS (

  SELECT event_id, event_fingerprint
  FROM (

    SELECT event_id, event_fingerprint, COUNT(*) AS count
    FROM atomic.events
    WHERE event_fingerprint IS NOT NULL
      --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
    GROUP BY 1,2

  )

  WHERE count > 1

);

-- (b) create a new table with events that match these critera

CREATE TABLE duplicates.tmp_events
  DISTKEY (event_id)
  SORTKEY (event_id)
AS (

  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id, event_fingerprint ORDER BY dvce_created_tstamp) as event_number
  FROM atomic.events
  WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id)
    AND event_fingerprint IN (SELECT event_fingerprint FROM duplicates.tmp_events_id)
    --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time

);

-- (c) delete the relevant rows from atomic, then re-insert rows with event_number = 1 and move all other rows to duplicates

BEGIN;

  DELETE FROM atomic.events
  WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id)
    AND event_fingerprint IN (SELECT event_fingerprint FROM duplicates.tmp_events_id)
    --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
  ;

  INSERT INTO atomic.events (

    SELECT

      app_id, platform, etl_tstamp, collector_tstamp, dvce_created_tstamp, event, event_id, txn_id,
      name_tracker, v_tracker, v_collector, v_etl,
      user_id, user_ipaddress, user_fingerprint, domain_userid, domain_sessionidx, network_userid,
      geo_country, geo_region, geo_city, geo_zipcode, geo_latitude, geo_longitude, geo_region_name,
      ip_isp, ip_organization, ip_domain, ip_netspeed, page_url, page_title, page_referrer,
      page_urlscheme, page_urlhost, page_urlport, page_urlpath, page_urlquery, page_urlfragment,
      refr_urlscheme, refr_urlhost, refr_urlport, refr_urlpath, refr_urlquery, refr_urlfragment,
      refr_medium, refr_source, refr_term, mkt_medium, mkt_source, mkt_term, mkt_content, mkt_campaign,
      se_category, se_action, se_label, se_property, se_value,
      tr_orderid, tr_affiliation, tr_total, tr_tax, tr_shipping, tr_city, tr_state, tr_country,
      ti_orderid, ti_sku, ti_name, ti_category, ti_price, ti_quantity,
      pp_xoffset_min, pp_xoffset_max, pp_yoffset_min, pp_yoffset_max,
      useragent, br_name, br_family, br_version, br_type, br_renderengine, br_lang, br_features_pdf, br_features_flash,
      br_features_java, br_features_director, br_features_quicktime, br_features_realplayer, br_features_windowsmedia,
      br_features_gears, br_features_silverlight, br_cookies, br_colordepth, br_viewwidth, br_viewheight,
      os_name, os_family, os_manufacturer, os_timezone, dvce_type, dvce_ismobile, dvce_screenwidth, dvce_screenheight,
      doc_charset, doc_width, doc_height, tr_currency, tr_total_base, tr_tax_base, tr_shipping_base,
      ti_currency, ti_price_base, base_currency, geo_timezone, mkt_clickid, mkt_network, etl_tags,
      dvce_sent_tstamp, refr_domain_userid, refr_dvce_tstamp, domain_sessionid,
      derived_tstamp, event_vendor, event_name, event_format, event_version, event_fingerprint, true_tstamp

    FROM duplicates.tmp_events WHERE event_number = 1

  );

  INSERT INTO duplicates.events (

    SELECT

      app_id, platform, etl_tstamp, collector_tstamp, dvce_created_tstamp, event, event_id, txn_id,
      name_tracker, v_tracker, v_collector, v_etl,
      user_id, user_ipaddress, user_fingerprint, domain_userid, domain_sessionidx, network_userid,
      geo_country, geo_region, geo_city, geo_zipcode, geo_latitude, geo_longitude, geo_region_name,
      ip_isp, ip_organization, ip_domain, ip_netspeed, page_url, page_title, page_referrer,
      page_urlscheme, page_urlhost, page_urlport, page_urlpath, page_urlquery, page_urlfragment,
      refr_urlscheme, refr_urlhost, refr_urlport, refr_urlpath, refr_urlquery, refr_urlfragment,
      refr_medium, refr_source, refr_term, mkt_medium, mkt_source, mkt_term, mkt_content, mkt_campaign,
      se_category, se_action, se_label, se_property, se_value,
      tr_orderid, tr_affiliation, tr_total, tr_tax, tr_shipping, tr_city, tr_state, tr_country,
      ti_orderid, ti_sku, ti_name, ti_category, ti_price, ti_quantity,
      pp_xoffset_min, pp_xoffset_max, pp_yoffset_min, pp_yoffset_max,
      useragent, br_name, br_family, br_version, br_type, br_renderengine, br_lang, br_features_pdf, br_features_flash,
      br_features_java, br_features_director, br_features_quicktime, br_features_realplayer, br_features_windowsmedia,
      br_features_gears, br_features_silverlight, br_cookies, br_colordepth, br_viewwidth, br_viewheight,
      os_name, os_family, os_manufacturer, os_timezone, dvce_type, dvce_ismobile, dvce_screenwidth, dvce_screenheight,
      doc_charset, doc_width, doc_height, tr_currency, tr_total_base, tr_tax_base, tr_shipping_base,
      ti_currency, ti_price_base, base_currency, geo_timezone, mkt_clickid, mkt_network, etl_tags,
      dvce_sent_tstamp, refr_domain_userid, refr_dvce_tstamp, domain_sessionid,
      derived_tstamp, event_vendor, event_name, event_format, event_version, event_fingerprint, true_tstamp

    FROM duplicates.tmp_events WHERE event_number > 1

  );

COMMIT;

-- (d) move remaining duplicates to another table (optional)

--CREATE TABLE duplicates.tmp_events_id_remaining
  --DISTKEY (event_id)
  --SORTKEY (event_id)
--AS (

  --SELECT event_id
  --FROM (

    --SELECT event_id, COUNT(*) AS count
    --FROM atomic.events
    --WHERE event_fingerprint IS NOT NULL
      --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
    --GROUP BY 1

  --)

  --WHERE count > 1

--);

--BEGIN;

  --INSERT INTO duplicates.events (

    --SELECT * FROM atomic.events
    --WHERE event_fingerprint IS NOT NULL
      --AND event_id IN (SELECT event_id FROM duplicates.tmp_events_id_remaining)
      --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time

  --);

  --DELETE FROM atomic.events
  --WHERE event_fingerprint IS NOT NULL
    --AND event_id IN (SELECT event_id FROM duplicates.tmp_events_id_remaining)
    --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
  --;

--COMMIT;

-- (e) drop tables

DROP TABLE IF EXISTS duplicates.tmp_events;
DROP TABLE IF EXISTS duplicates.tmp_events_id;
DROP TABLE IF EXISTS duplicates.tmp_events_id_remaining;
