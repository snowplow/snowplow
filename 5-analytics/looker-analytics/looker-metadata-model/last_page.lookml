- view: last_page
  derived_table:
    sql: |
       SELECT
        domain_userid,
        domain_sessionidx,
        domain_userid || '-' || domain_sessionidx AS visit_id,
        MAX(page_urlhost) AS page_urlhost, -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
        MAX(page_urlpath) AS page_urlpath  -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)
        FROM (
          SELECT
          domain_userid,
          domain_sessionidx,
          page_urlhost,
          page_urlpath,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp DESC) AS "rank_desc"
          FROM atomic.events
          WHERE event = 'page_view') AS a
        WHERE rank_desc = 1                -- Filter so only return landing pages
        GROUP BY 1,2,3                     -- Remove duplicates (v. occasional case where two page views have exactly the same dvce_tstamp)

    persist_for: 3 hours

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
    
  # MEASURES #
  
  - measure: page_views_count
    type: count
    detail: detail*
    
  # Detail #
  sets:
    detail:
      - domain_userid
      - domain_sessionidx
      - page_urlhost
      - page_urlpath