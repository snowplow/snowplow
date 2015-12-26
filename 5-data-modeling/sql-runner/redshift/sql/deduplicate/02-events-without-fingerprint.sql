-- Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Copyright: Copyright (c) 2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: deduplicate
-- Version: 0.3
--
-- Requires atomic.events 0.7.0
--
-- This script is OPTIONAL. It should be run after 01-events and deduplicates rows with the same event_id
-- where at least one row has no event_fingerprint (older events).
--
-- Steps (a) to (c) deduplicate rows that have the same event_id. The approach is similar to the one in
-- 01-events but an event fingerprint is generated in SQL. When 2 or more events have the same event_id
-- and custom_fingerprint, the earliest event is kept in atomic while all others are moved to duplicates.

-- Step (d) is an optional step that moves all remaining duplicates (same event_id but different fingerprint)
-- to duplicates. Note that this might delete legitimate events from atomic.
--
-- These SQL queries can be run without modifications.

DROP TABLE IF EXISTS duplicates.tmp_events;
DROP TABLE IF EXISTS duplicates.tmp_events_id;
DROP TABLE IF EXISTS duplicates.tmp_events_id_remaining;

-- (a) list all event_id that occur more than once

CREATE TABLE duplicates.tmp_events_id
  DISTKEY (event_id)
  SORTKEY (event_id)
AS (SELECT event_id FROM (SELECT event_id, COUNT(*) AS count FROM atomic.events GROUP BY 1) WHERE count > 1);

-- (b) create a new table with these events and replicate the event fingerprint

CREATE TABLE duplicates.tmp_events
  DISTKEY (event_id)
  SORTKEY (event_id)
AS (

  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id, custom_fingerprint ORDER BY dvce_created_tstamp) as event_number
  FROM (
    SELECT *,
      MD5(
        NVL(CAST(app_id AS VARCHAR),'') || '-' ||
        NVL(CAST(platform AS VARCHAR),'') || '-' ||
        NVL(TO_CHAR(dvce_created_tstamp, 'YYYY-MM-DD HH24:MI:SS:US'),'') || '-' ||
        NVL(CAST(event AS VARCHAR),'') || '-' ||
        NVL(CAST(user_id AS VARCHAR),'') || '-' ||
        NVL(CAST(domain_sessionidx AS VARCHAR),'') || '-' ||
        NVL(CAST(domain_userid AS VARCHAR),'') || '-' ||
        NVL(CAST(user_fingerprint AS VARCHAR),'') || '-' ||
        NVL(CAST(user_ipaddress AS VARCHAR),'') || '-' ||
        NVL(CAST(page_url AS VARCHAR),'') || '-' ||
        NVL(CAST(page_title AS VARCHAR),'') || '-' ||
        NVL(CAST(page_referrer AS VARCHAR),'') || '-' ||
        NVL(CAST(name_tracker AS VARCHAR),'') || '-' ||
        NVL(CAST(v_tracker AS VARCHAR),'') || '-' ||
        NVL(CAST(os_timezone AS VARCHAR),'') || '-' ||
        NVL(TO_CHAR(true_tstamp, 'YYYY-MM-DD HH24:MI:SS:US'),'') || '-' ||
        NVL(CAST(se_value AS VARCHAR),'') || '-' ||
        NVL(CAST(se_property AS VARCHAR),'') || '-' ||
        NVL(CAST(se_label AS VARCHAR),'') || '-' ||
        NVL(CAST(se_action AS VARCHAR),'') || '-' ||
        NVL(CAST(se_category AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_orderid AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_country AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_state AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_city AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_shipping AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_tax AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_total AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_affiliation AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_currency AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_orderid AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_quantity AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_price AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_category AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_name AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_sku AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_currency AS VARCHAR),'') || '-' ||
        NVL(CAST(ti_price_base AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_total_base AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_shipping_base AS VARCHAR),'') || '-' ||
        NVL(CAST(tr_tax_base AS VARCHAR),'') || '-' ||
        NVL(CAST(pp_xoffset_min AS VARCHAR),'') || '-' ||
        NVL(CAST(pp_yoffset_max AS VARCHAR),'') || '-' ||
        NVL(CAST(pp_yoffset_min AS VARCHAR),'') || '-' ||
        NVL(CAST(pp_xoffset_max AS VARCHAR),'') || '-' ||
        NVL(CAST(useragent AS VARCHAR),'') || '-' ||
        NVL(CAST(dvce_screenwidth AS VARCHAR),'') || '-' ||
        NVL(CAST(dvce_screenheight AS VARCHAR),'') || '-' ||
        NVL(CAST(CAST(br_cookies AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(br_lang AS VARCHAR),'') || '-' ||
        NVL(CAST(CAST(br_features_java AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_windowsmedia AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_realplayer AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_quicktime AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_director AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_pdf AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_flash AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_gears AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(CAST(br_features_silverlight AS INT) AS VARCHAR), '') || '-' ||
        NVL(CAST(br_viewwidth AS VARCHAR),'') || '-' ||
        NVL(CAST(br_viewheight AS VARCHAR),'') || '-' ||
        NVL(CAST(br_colordepth AS VARCHAR),'') || '-' ||
        NVL(CAST(doc_charset AS VARCHAR),'') || '-' ||
        NVL(CAST(doc_height AS VARCHAR),'') || '-' ||
        NVL(CAST(doc_width AS VARCHAR),'') || '-' ||
        NVL(CAST(base_currency AS VARCHAR),'') || '-' ||
        NVL(CAST(mkt_network AS VARCHAR),'') || '-' ||
        NVL(CAST(mkt_clickid AS VARCHAR),'') || '-' ||
        NVL(CAST(refr_domain_userid AS VARCHAR),'') || '-' ||
        NVL(CAST(domain_sessionid AS VARCHAR),'') || '-' ||
        NVL(TO_CHAR(refr_dvce_tstamp, 'YYYY-MM-DD HH24:MI:SS:US'),'')
      ) AS custom_fingerprint
    FROM atomic.events
    WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id)
  )

);

-- (c) delete the duplicates from the original table and insert the deduplicated rows with event_number = 1 and move the remaining to another table

BEGIN;

  DELETE FROM atomic.events
  WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id);

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
--AS (SELECT event_id FROM (SELECT event_id, COUNT(*) AS count FROM atomic.events GROUP BY 1) WHERE count > 1);

--BEGIN;

  --INSERT INTO duplicates.events (SELECT * FROM atomic.events WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id_remaining));
  --DELETE FROM atomic.events WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id_remaining);

--COMMIT;

-- (e) drop tables

DROP TABLE IF EXISTS duplicates.tmp_events;
DROP TABLE IF EXISTS duplicates.tmp_events_id;
DROP TABLE IF EXISTS duplicates.tmp_events_id_remaining;
