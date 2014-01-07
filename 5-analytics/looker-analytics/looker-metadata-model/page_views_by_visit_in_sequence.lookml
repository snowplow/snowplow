- view: page_views_by_visit_in_sequence
  derived_table:
    sql: |
      SELECT
      domain_userid,
      domain_sessionidx,
      domain_userid || '-' || domain_sessionidx AS visit_id,
      page_urlhost,
      page_urlpath,
      rank_asc,
      rank_desc
      FROM (
        SELECT
        domain_userid,
        domain_sessionidx,
        page_urlhost,
        page_urlpath,
        RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank_asc",
        RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp DESC) AS "rank_desc"
        FROM atomic.events
        WHERE event = 'page_view') AS t
      GROUP BY 1,2,3,4,5,6,7; -- remove duplicates

    persist_for: 6 hour

  fields:
    
  # DIMENSIONS #
  
  - dimension: visit_id
    primary_key: true
    sql: ${TABLE}.visit_id
  
  - dimension: domain_userid
    sql: ${TABLE}.domain_userid
    
  - dimension: domain_sessionidx
    type: int
    sql: ${TABLE}.domain_sessionidx
    
  - dimension: page_urlhost
    sql: ${TABLE}.page_urlhost
    
  - dimension: page_urlpath
    sql: ${TABLE}.page_urlpath
    
  - dimension: rank_asc
    type: int
    sql: ${TABLE}.rank_asc
    
  - dimension: rank_desc
    type: int
    sql: rank_desc

# Tried to get landing pages - now trying a different technique
#  - dimension: landing_page_host
#    sql: ${TABLE}.page_urlhost
#    filters:
#      rank_asc: 1
#  
#  - dimension: landing_page_path
#    sql: ${TABLE}.page_urlpath
#    filters:
#      rank_asc: 1
    
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
      - rank_asc