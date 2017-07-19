-- requires trackTrans, trackAddToCart and trackRemoveFromCart

DROP TABLE IF EXISTS scratch.web_ecommerce_context;

CREATE TABLE scratch.web_ecommerce_context
  DISTKEY(event_id) -- used for joins downstream
  SORTKEY(event_id)
AS (

  WITH step1 AS (

    -- select the relevant fields from the atomic tables (canonical events and shredded) and add page_view_id

    SELECT
      wp.page_view_id,
      ev.event_name,
      ev.event_id,

      ev.tr_orderid,
      ev.tr_affiliation,
      ev.tr_total,
      ev.tr_tax,
      ev.tr_shipping,
      ev.tr_total_base,
      ev.tr_tax_base,
      ev.tr_shipping_base,
      ev.tr_city,
      ev.tr_state,
      ev.tr_country,
      ev.tr_currency,

      ev.ti_orderid,
      ev.ti_sku,
      ev.ti_name,
      ev.ti_category,
      ev.ti_price,
      ev.ti_price_base,
      ev.ti_quantity,
      ev.ti_currency,

      ev.base_currency,

      ac.sku AS item_added_sku,
      ac.name AS item_added_name,
      ac.category AS item_added_category,
      ac.unit_price AS item_added_unit_price,
      ac.quantity AS item_added_quantity,
      ac.currency AS item_added_currency,

      rc.sku AS item_removed_sku,
      rc.name AS item_removed_name,
      rc.category AS item_removed_category,
      rc.unit_price AS item_removed_unit_price,
      rc.quantity AS item_removed_quantity,
      rc.currency AS item_removed_currency,

      ev.dvce_created_tstamp -- to deduplicate

    FROM atomic.events AS ev

    INNER JOIN scratch.web_page_context AS wp
      ON ev.event_id = wp.root_id

    LEFT JOIN atomic.com_snowplowanalytics_snowplow_add_to_cart_1 AS ac
      ON ev.event_id = ac.root_id AND ev.collector_tstamp = ac.root_tstamp

    LEFT JOIN atomic.com_snowplowanalytics_snowplow_remove_from_cart_1 AS rc
      ON ev.event_id = rc.root_id AND ev.collector_tstamp = rc.root_tstamp

    WHERE ev.platform = 'web'
      AND ev.event_name IN ('transaction', 'transaction_item', 'add_to_cart', 'remove_from_cart')
      AND ev.domain_userid IS NOT NULL

  ),

  -- more than one event per event ID? select the first one

  step2 AS (

    SELECT
      *,
      row_number() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) AS n

    FROM step1

  )

  SELECT -- only the relevant fields from step1

    page_view_id, event_name, event_id, tr_orderid, tr_affiliation, tr_total,
    tr_tax, tr_shipping, tr_total_base, tr_tax_base, tr_shipping_base, tr_city,
    tr_state, tr_country, tr_currency, ti_orderid, ti_sku, ti_name,
    ti_category, ti_price, ti_price_base, ti_quantity, ti_currency,
    base_currency, item_added_sku, item_added_name, item_added_category,
    item_added_unit_price, item_added_quantity, item_added_currency, item_removed_sku,
    item_removed_name, item_removed_category, item_removed_unit_price, item_removed_quantity,
    item_removed_currency

  FROM step2

  WHERE n = 1

);
