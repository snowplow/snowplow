-- requires trackTrans, trackAddToCart and trackRemoveFromCart

DROP TABLE IF EXISTS web.transactions_tmp;

CREATE TABLE web.transactions_tmp
  DISTKEY(user_snowplow_domain_id)
  SORTKEY(event_timestamp)
AS (

  -- bringing it all together

  SELECT

    -- user

    a.user_id AS user_custom_id,
    a.domain_userid AS user_snowplow_domain_id,
    a.network_userid AS user_snowplow_crossdomain_id,

    -- session

    a.domain_sessionid AS session_id,
    a.domain_sessionidx AS session_index,

    -- event

    a.event_id,
    a.event_name,
    a.page_view_id,

    -- event: time (replace Europe/London with the relevant timezone - no issues with DST)

    CONVERT_TIMEZONE('UTC', 'Europe/London', a.derived_tstamp) AS event_timestamp,

    -- event: time in the user's local timezone

    CONVERT_TIMEZONE('UTC', a.os_timezone, a.derived_tstamp) AS event_timestamp_local,

    -- location

    a.geo_country,
    a.geo_region,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone, -- often NULL (use os_timezone instead)

    -- IP

    a.user_ipaddress AS ip_address,

    a.ip_isp,
    a.ip_organization,
    a.ip_domain,
    a.ip_netspeed AS ip_net_speed,

    -- application

    a.app_id,

    -- browser

    b.useragent_version AS browser,
    b.useragent_family AS browser_name,
    b.useragent_major AS browser_major_version,
    b.useragent_minor AS browser_minor_version,
    b.useragent_patch AS browser_build_version,
    a.br_renderengine AS browser_engine,
    a.br_lang AS browser_language,

    -- OS

    b.os_version AS os,
    b.os_family AS os_name,
    b.os_major AS os_major_version,
    b.os_minor AS os_minor_version,
    b.os_patch AS os_build_version,
    a.os_manufacturer,
    a.os_timezone,

    -- device

    b.device_family AS device,
    a.dvce_type AS device_type,
    a.dvce_ismobile AS device_is_mobile,

    -- ecommerce

    c.tr_orderid AS transaction_order_id,
    c.tr_affiliation AS transaction_affiliation,
    c.tr_total AS transaction_total,
    c.tr_tax AS transaction_tax,
    c.tr_shipping AS transaction_shipping,
    c.tr_total_base AS transaction_total_base,
    c.tr_tax_base AS transaction_tax_base,
    c.tr_shipping_base AS transaction_shipping_base,
    c.tr_city AS transaction_city,
    c.tr_state AS transaction_state,
    c.tr_country AS transaction_country,
    c.tr_currency AS transaction_currency,

    c.ti_orderid AS item_bought_order_id,
    c.ti_sku AS item_bought_sku,
    c.ti_name AS item_bought_name,
    c.ti_category AS item_bought_category,
    c.ti_price AS item_bought_price,
    c.ti_price_base AS item_bought_price_base,
    c.ti_quantity AS item_bought_quantity,
    c.ti_currency AS item_bought_currency,

    c.base_currency,

    c.item_added_sku,
    c.item_added_name,
    c.item_added_category,
    c.item_added_unit_price,
    c.item_added_quantity,
    c.item_added_currency,

    c.item_removed_sku,
    c.item_removed_name,
    c.item_removed_category,
    c.item_removed_unit_price,
    c.item_removed_quantity,
    c.item_removed_currency,

    -- first marketing touch all time

    d.mkt_network AS first_touch_marketing_network,
    d.mkt_clickid AS first_touch_marketing_click_id,

    d.mkt_medium AS first_touch_marketing_medium,
    d.mkt_source AS first_touch_marketing_source,
    d.mkt_campaign AS first_touch_marketing_campaign,
    d.mkt_term AS first_touch_marketing_term,
    d.mkt_content AS first_touch_marketing_content,

    CASE
      WHEN d.refr_medium = 'unknown' THEN 'other'
      ELSE d.refr_medium
    END AS first_touch_referer_medium,
    d.refr_source AS first_touch_referer_source,
    d.refr_term AS first_touch_referer_term,

    -- first marketing touch 90 days

    e.mkt_network AS first_touch_90_marketing_network,
    e.mkt_clickid AS first_touch_90_marketing_click_id,

    e.mkt_medium AS first_touch_90_marketing_medium,
    e.mkt_source AS first_touch_90_marketing_source,
    e.mkt_campaign AS first_touch_90_marketing_campaign,
    e.mkt_term AS first_touch_90_marketing_term,
    e.mkt_content AS first_touch_90_marketing_content,

    CASE
      WHEN e.refr_medium = 'unknown' THEN 'other'
      ELSE e.refr_medium
    END AS first_touch_90_referer_medium,
    e.refr_source AS first_touch_90_referer_source,
    e.refr_term AS first_touch_90_referer_term,

    -- first marketing touch 30 days

    f.mkt_network AS first_touch_30_marketing_network,
    f.mkt_clickid AS first_touch_30_marketing_click_id,

    f.mkt_medium AS first_touch_30_marketing_medium,
    f.mkt_source AS first_touch_30_marketing_source,
    f.mkt_campaign AS first_touch_30_marketing_campaign,
    f.mkt_term AS first_touch_30_marketing_term,
    f.mkt_content AS first_touch_30_marketing_content,

    CASE
      WHEN f.refr_medium = 'unknown' THEN 'other'
      ELSE f.refr_medium
    END AS first_touch_30_referer_medium,
    f.refr_source AS first_touch_30_referer_source,
    f.refr_term AS first_touch_30_referer_term,

    -- first marketing touch 10 days

    g.mkt_network AS first_touch_10_marketing_network,
    g.mkt_clickid AS first_touch_10_marketing_click_id,

    g.mkt_medium AS first_touch_10_marketing_medium,
    g.mkt_source AS first_touch_10_marketing_source,
    g.mkt_campaign AS first_touch_10_marketing_campaign,
    g.mkt_term AS first_touch_10_marketing_term,
    g.mkt_content AS first_touch_10_marketing_content,

    CASE
      WHEN g.refr_medium = 'unknown' THEN 'other'
      ELSE g.refr_medium
    END AS first_touch_10_referer_medium,
    g.refr_source AS first_touch_10_referer_source,
    g.refr_term AS first_touch_10_referer_term,

    --last marketing touch

    h.mkt_network AS last_touch_marketing_network,
    h.mkt_clickid AS last_touch_marketing_click_id,

    h.mkt_medium AS last_touch_marketing_medium,
    h.mkt_source AS last_touch_marketing_source,
    h.mkt_campaign AS last_touch_marketing_campaign,
    h.mkt_term AS last_touch_marketing_term,
    h.mkt_content AS last_touch_marketing_content,

    CASE
      WHEN h.refr_medium = 'unknown' THEN 'other'
      ELSE h.refr_medium
    END AS last_touch_referer_medium,
    h.refr_source AS last_touch_referer_source,
    h.refr_term AS last_touch_referer_term

  FROM scratch.web_ecommerce_events AS a -- the INNER JOIN requires that all contexts are set

  INNER JOIN scratch.web_ua_parser_context AS b
    ON a.page_view_id = b.page_view_id

  INNER JOIN scratch.web_ecommerce_context AS c -- web_ecommerce_events and web_ecommerce_context should have identical event_id columns
  ON a.event_id = c.event_id

  LEFT JOIN scratch.web_first_marketing_touch AS d
    ON a.event_id = d.transaction_event_id

  LEFT JOIN scratch.web_first_marketing_touch_90_days AS e
    ON a.event_id = e.transaction_event_id

  LEFT JOIN scratch.web_first_marketing_touch_30_days AS f
    ON a.event_id = f.transaction_event_id

  LEFT JOIN scratch.web_first_marketing_touch_10_days AS g
    ON a.event_id = g.transaction_event_id

  LEFT JOIN scratch.web_last_marketing_touch AS h
    ON a.event_id = h.transaction_event_id


  WHERE a.br_family != 'Robot/Spider'
    AND a.useragent NOT SIMILAR TO '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
    AND a.user_id IS NOT NULL -- rare edge case
    AND a.domain_userid IS NOT NULL -- rare edge case
    AND a.domain_sessionidx > 0 -- rare edge case
    AND a.event_name IN ('transaction', 'transaction_item', 'add_to_cart', 'remove_from_cart')

);
