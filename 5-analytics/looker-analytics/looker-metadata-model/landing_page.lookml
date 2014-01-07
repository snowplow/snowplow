- view: landing_page
  derived_table:
    sql: |
       SELECT
        domain_userid,
        domain_sessionidx,
        domain_userid || domain_sessionidx AS visit_id,
        MAX(page_urlhost) AS page_urlhost, -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
        MAX(page_urlpath) AS page_urlpath  -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
        FROM (
          SELECT
          domain_userid,
          domain_sessionidx,
          page_urlhost,
          page_urlpath,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank_asc"
          FROM atomic.events
          WHERE event = 'page_view') AS a
        WHERE rank_asc = 1                -- Filter so only return landing pages
        GROUP BY 1,2,3                    -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
      

    persist_for: 6 hour

  fields:
    
  # DIMENSIONS #
  
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

  - dimension: host
    sql: ${TABLE}.page_urlhost
    
  - dimension: path
    sql: ${TABLE}.page_urlpath
    
  - dimension: landing_page
    sql: ${TABLE}.page_urlhost || ${TABLE}.page_urlpath
  
  # MEASURES # 
  
  - measure: landing_page_count
    type: count_distinct
    sql: ${landing_page}
    detail: landing_page_detail*
    
  # Detail #
  sets:
    landing_page_detail:
      - landing_page
      - visits.visits_count
      - visits.bounce_rate
      - visits.approx_usage_per_visit_in_minutes
      - events.events_per_visit
      - visits.bounce_rate
