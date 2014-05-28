- view: sessions_last_page
  derived_table:
    sql: |
      SELECT
        domain_userid,
        domain_sessionidx,
        page_urlhost, 
        page_urlpath 
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          LAST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
          LAST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
        FROM atomic.events) AS a
      GROUP BY 1,2,3,4

    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_landing_page.SQL_TABLE_NAME} # Generate this table after the sessions_landing page
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx]

  fields:
    
  # DIMENSIONS #
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: session_index
    type: int
    sql: ${TABLE}.domain_sessionidx

  - dimension: exit_page_host
    sql: ${TABLE}.page_urlhost
    
  - dimension: exit_page_path
    sql: ${TABLE}.page_urlpath
    
  - measure: count
    type: count
    
  