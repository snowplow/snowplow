-- requires trackTrans, trackAddToCart and trackRemoveFromCart

DROP TABLE IF EXISTS scratch.web_marketing_touches;

CREATE TABLE scratch.web_marketing_touches
  DISTKEY(event_id)
  SORTKEY(event_id)
AS (

  WITH step1 AS (

    -- select the relevant fields from atomic.events and add page_view_id

    SELECT

      ev.user_id,
      ev.domain_userid,
      ev.derived_tstamp,
      ev.event_id,
      wp.page_view_id,

      ev.mkt_network,
      ev.mkt_clickid,

      ev.mkt_medium,
      ev.mkt_source,
      ev.mkt_campaign,
      ev.mkt_term,
      ev.mkt_content,

      ev.refr_medium,
      ev.refr_source,
      ev.refr_term

    FROM atomic.events AS ev

    INNER JOIN scratch.web_page_context AS wp
      ON ev.event_id = wp.root_id

    WHERE ev.platform = 'web'
      AND ev.refr_medium != 'internal' -- not a marketing touch, so exclude it
      AND ev.refr_medium IS NOT NULL
      AND ev.event_name IN ('page_view', 'transaction', 'transaction_item', 'add_to_cart', 'remove_from_cart')

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15

  ),

  -- more than one event per event ID? select the first one

  step2 AS (

    SELECT
      *,
      row_number() OVER (PARTITION BY event_id ORDER BY derived_tstamp) AS n

    FROM step1

  )

  SELECT -- only the relevant fields from step1

    user_id, domain_userid, derived_tstamp, event_id, page_view_id, mkt_network,
    mkt_clickid, mkt_medium, mkt_source, mkt_campaign, mkt_term, mkt_content,
    refr_medium, refr_source, refr_term

  FROM step2

  WHERE n = 1

);
