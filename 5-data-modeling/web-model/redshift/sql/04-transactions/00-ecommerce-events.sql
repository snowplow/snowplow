DROP TABLE IF EXISTS scratch.web_ecommerce_events;

CREATE TABLE scratch.web_ecommerce_events
  DISTKEY(event_id) -- used for joins downstream
  SORTKEY(event_timestamp)
AS (

  WITH step1 AS (

    -- select the relevant dimensions from atomic.events

    SELECT

      ev.user_id,
      ev.domain_userid,
      ev.network_userid,

      ev.domain_sessionid,
      ev.domain_sessionidx,

      wp.page_view_id,

      ev.page_title,

      ev.page_urlscheme,
      ev.page_urlhost,
      ev.page_urlport,
      ev.page_urlpath,
      ev.page_urlquery,
      ev.page_urlfragment,

      ev.refr_urlscheme,
      ev.refr_urlhost,
      ev.refr_urlport,
      ev.refr_urlpath,
      ev.refr_urlquery,
      ev.refr_urlfragment,

      ev.refr_medium,
      ev.refr_source,
      ev.refr_term,

      ev.mkt_medium,
      ev.mkt_source,
      ev.mkt_term,
      ev.mkt_content,
      ev.mkt_campaign,
      ev.mkt_clickid,
      ev.mkt_network,

      ev.geo_country,
      ev.geo_region,
      ev.geo_region_name,
      ev.geo_city,
      ev.geo_zipcode,
      ev.geo_latitude,
      ev.geo_longitude,
      ev.geo_timezone,

      ev.user_ipaddress,

      ev.ip_isp,
      ev.ip_organization,
      ev.ip_domain,
      ev.ip_netspeed,

      ev.app_id,

      ev.useragent,
      ev.br_name,
      ev.br_family,
      ev.br_version,
      ev.br_type,
      ev.br_renderengine,
      ev.br_lang,
      ev.dvce_type,
      ev.dvce_ismobile,

      ev.os_name,
      ev.os_family,
      ev.os_manufacturer,
      ev.os_timezone,

      ev.name_tracker, -- included to filter on
      ev.dvce_created_tstamp, -- included to sort on

      ev.event_name,
      ev.event_id,

      ev.derived_tstamp

    FROM snowplow.events AS ev

    INNER JOIN snowplow_scratch.web_page_context AS wp -- an INNER JOIN guarantees that all rows have a page view ID
      ON ev.event_id = wp.root_id

    WHERE ev.platform = 'web'
      AND ev.event_name IN ('transaction', 'transaction_item', 'add_to_cart', 'remove_from_cart')

  ),

  -- more than one event per event ID? select the first one

  step2 AS (

    SELECT
      *,
      row_number() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) AS n

    FROM step1

  )

  SELECT -- only the fields from step1

  user_id, domain_userid, network_userid, domain_sessionid, domain_sessionidx,
  page_view_id, page_title, page_urlscheme, page_urlhost, page_urlport,
  page_urlpath, page_urlquery, page_urlfragment, refr_urlscheme, refr_urlhost,
  refr_urlport, refr_urlpath, refr_urlquery, refr_urlfragment, refr_medium,
  refr_source, refr_term, mkt_medium, mkt_source, mkt_term, mkt_content,
  mkt_campaign, mkt_clickid, mkt_network, geo_country, geo_region, geo_region_name,
  geo_city, geo_zipcode, geo_latitude, geo_longitude, geo_timezone, user_ipaddress,
  ip_isp, ip_organization, ip_domain, ip_netspeed, app_id, useragent, br_name,
  br_family, br_version, br_type, br_renderengine, br_lang, dvce_type,
  dvce_ismobile, os_name, os_family, os_manufacturer, os_timezone, name_tracker,
  dvce_created_tstamp, event_name, event_id, derived_tstamp

  FROM step2

  WHERE n = 1

);
