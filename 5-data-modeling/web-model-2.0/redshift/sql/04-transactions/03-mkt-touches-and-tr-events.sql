-- requires trackTrans, trackAddToCart and trackRemoveFromCart

DROP TABLE IF EXISTS scratch.web_marketing_touches_and_transaction_events;

CREATE TABLE scratch.web_marketing_touches_and_transaction_events
  DISTKEY(domain_userid) -- used for joins downstream
  SORTKEY(domain_userid)
AS (

  SELECT -- all marketing touches

    domain_userid,
    derived_tstamp AS marketing_event_timestamp,
    NULL AS transaction_event_timestamp,
    event_id AS marketing_event_id,
    NULL AS transaction_event_id,
    'marketing touch' AS event_type

  FROM scratch.web_marketing_touches

  UNION

  SELECT -- all transation events

    domain_userid,
    NULL AS marketing_event_timestamp,
    event_timestamp AS transaction_event_timestamp,
    NULL AS marketing_event_id,
    event_id AS transaction_event_id,
    'transaction event' AS event_type

  FROM scratch.web_ecommerce_events

);
