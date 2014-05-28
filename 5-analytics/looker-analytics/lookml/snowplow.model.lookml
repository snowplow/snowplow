- connection: snowplow

- scoping: true           # for backward compatibility
- include: "*.lookml"     # include all the lookml files in the same directory as the model

- base_view: events  
  joins:
  - join: sessions
    sql_on: |
      events.domain_userid = sessions.domain_userid AND
      events.domain_sessionidx = sessions.domain_sessionidx
  - join: visitors
    sql_on: |
      events.domain_userid = visitors.domain_userid

# Views to support events
- base_view: atomic_events
    
- base_view: sessions
  joins: 
  - join: visitors
    sql_on: |
      sessions.domain_userid = visitors.domain_userid


# Views supporting sessions (making queryable for debug purposes - remove later)
# - base_view: sessions_basic
# - base_view: sessions_geo
# - base_view: sessions_landing_page
# - base_view: sessions_last_page
# - base_view: sessions_source
# - base_view: sessions_technology

- base_view: visitors

# Views supporting visitors
# - base_view: visitors_basic
  
- base_view: transactions

- base_view: transaction_items
  joins:
  - join: transactions
    sql_foreign_key: transaction_items.ti_orderid
