-- requires trackTrans, trackAddToCart and trackRemoveFromCart

DROP TABLE IF EXISTS scratch.web_last_marketing_touch;

CREATE TABLE scratch.web_last_marketing_touch
  DISTKEY(transaction_event_id) -- used for joins downstream
  SORTKEY(transaction_event_id)
AS (

  WITH step1 AS (

    -- for each marketing touch or transaction event, find all the associated marketing events (ie all other marketing events by the same user).
    -- If mixing marekting events with marketing events doesn't make sense, check the last line where we ultimately filter out unwanted stuff.

    SELECT

      a.domain_userid,
      a.marketing_event_timestamp,
      a.transaction_event_timestamp,
      a.marketing_event_id,
      a.transaction_event_id,
      b.marketing_event_timestamp AS associated_mkt_event_tstamp,
      b.marketing_event_id AS associated_mkt_event_id,
      a.event_type

    FROM scratch.web_marketing_touches_and_transaction_events AS a
    LEFT JOIN scratch.web_marketing_touches_and_transaction_events AS b
      ON a.domain_userid = b.domain_userid

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

  ),

    step2 AS (

    -- find the last associated marketing event

    SELECT
      domain_userid,
      transaction_event_timestamp,
      LAST_VALUE(marketing_event_id IGNORE NULLS) OVER (PARTITION BY domain_userid ORDER BY marketing_event_timestamp, transaction_event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_marketing_event_id,
      transaction_event_id,
      event_type

    FROM step1

    WHERE associated_mkt_event_tstamp < nvl(transaction_event_timestamp, marketing_event_timestamp) -- only marketing touches from before the transaction took place

  ),

    step3 AS (

    -- deduplicate

    SELECT *

    FROM step2

    GROUP BY 1, 2, 3, 4, 5

  )

  -- get all the relevant marketing and referrer fields

  SELECT
    s3.domain_userid,
    s3.transaction_event_timestamp,
    s3.last_marketing_event_id,
    s3.transaction_event_id,

    m.mkt_network,
    m.mkt_clickid,

    m.mkt_medium,
    m.mkt_source,
    m.mkt_campaign,
    m.mkt_term,
    m.mkt_content,

    m.refr_medium,
    m.refr_source,
    m.refr_term

  FROM step3 AS s3

  RIGHT JOIN scratch.web_marketing_touches AS m
    ON s3.last_marketing_event_id = m.event_id -- only perform the join for the last touch event

  WHERE s3.event_type = 'transaction event' -- only fetch transaction events

);
