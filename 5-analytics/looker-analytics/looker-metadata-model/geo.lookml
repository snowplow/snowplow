- view: geo
  derived_table:
    sql: |
      SELECT 
        domain_userid,
        domain_sessionidx,
        domain_userid || '-' || domain_sessionidx AS visit_id,
        geo_country,
        geo_region,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          geo_country,
          geo_region,
          geo_city,
          geo_zipcode,
          geo_latitude,
          geo_longitude,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank" -- Oddly, we have users who show multiple locations per session. Maybe they're working behind proxy IPs?
        FROM atomic.events
        WHERE geo_country IS NOT NULL
        AND geo_country != '' ) AS t
      WHERE "rank" = 1;
    
    persist_for: 6 hour
    
  fields:
  
  # DIMENSIONS #
  
  # Basic dimensions #
  
  - dimension: visit_id
    primary_key: true
    sql: ${TABLE}.visit_id
  
  - dimension: domain_userid
    sql: ${TABLE}.domain_userid
    hidden: true
    
  - dimension: domain_sessionidx
    type: int
    sql: ${TABLE}.domain_sessionidx
    hidden: true

  # Geo fields #
  
  - dimension: country
    sql: ${TABLE}.geo_country
  
  - dimension: region
    sql: ${TABLE}.geo_region
    
  - dimension: city
    sql: ${TABLE}.geo_city
    
  - dimension: zipcode
    sql: ${TABLE}.zipcode
    
  - dimension: latitude
    sql: ${TABLE}.geo_latitude
  
  - dimension: longitude
    sql: ${TABLE}.geo_longitude
    
  # MEASURES #
  # Add measures for count of countries and average longitude / latitude
  # Add detail for drill down
  
  - measure: country_count
    type: count_distinct
    sql: ${country}
    detail: 
      - country
      - region_count
      - detail*
    
  - measure: region_count
    type: count_distinct
    sql: ${region}
    detail: 
      - region 
      - city_count
      - detail*
    
  - measure: city_count
    type: count_distinct
    sql: ${city}
    detail: 
      - city
      - detail*
  
  sets:
    detail:
      - visits.visits_count
      - visits.bounce_rate
      - events.approx_usage_per_visitor_in_minutes
      - events.events_per_visit
  