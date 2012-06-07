# Bread and butter web analytics queries

The following queries return basic web analytics data that someone could expect from any standard web analytics package.

1. [Number of unique visitors](#counting-unique-visitors)
2. [Number visits](#counting-visits)
3. [Number of pageviews](#counting-pageviews)
4. [Number of events](#counting-events)
5. [Pages per visit](#pages-per-visit)
6. [Bounce rate](#bounce-rate)
7. [% New visits](#new-visits)
8. [Repeating queries: a note about efficiency](#efficiency)

<a name="counting-unique-visitors" />
## 1. Number of unique visitors 

The number of unique visitors can be calculated by summing the number of distinct `user_id`s in a specified time period e.g. day. (Because each user is assigned a unique user_id, based on a lack of SnowPlow tracking cookies on their browser):

	SELECT 
	dt,
	COUNT(DISTINCT (user_id))
	FROM events
	GROUP BY dt ;

Or by week:

	SELECT
	YEAR(dt),
	WEEKOFYEAR(dt),
	COUNT(DISTINCT(user_id))
	FROM events
	GROUP BY YEAR(dt), WEEKOFYEAR(dt) ;

Or by month:

	SELECT
	YEAR(dt),
	MONTH(dt),
	COUNT(DISTINCT(user_id))
	FROM events
	GROUP BY YEAR(dt), MONTH(dt) ;

<a name="counting-visits" />
## 2. Number of visits

Because each user might visit a site more than once, summing the number of `user_id`s returns the number if *visitors*, NOT the number of *visits*. Every time a user visits the site, however, SnowPlow assigns that session with a `visit_id` (e.g. `1` for their first visit, `2` for their second.) Hence, to count the number of visits in a time period, we concatenate the unique `user_id` with the `visit_id` and then count the number of distinct concatenated entry in the events table:

	SELECT
	dt,
	COUNT( DISTINCT( CONCAT( user_id, visit_id )))
	FROM events 
	GROUP BY dt ;

Again, we can group by week:

	SELECT
	YEAR(dt),
	WEEKOFYEAR(dt),
	COUNT(DISTINCT(CONCAT(user_id, visit_id)))
	FROM events
	GROUP BY YEAR(dt), WEEKOFYEAR(dt) ;


Or month:

	SELECT
	YEAR(dt),
	MONTH(dt),
	COUNT(DISTINCT(CONCAT(user_id, visit_id)))
	FROM events
	GROUP BY YEAR(dt), MONTH(dt) ;

<a name="counting-pageviews" />
## 3. Number of page views

Page views are one type of event that are stored in the SnowPlow events table. Their defining feature is that the `page_title` contain values (are not `NULL`). In the case of an *event* that is not a page view (e.g. an _add to basket_) these fields would all be `NULL`, and the event fields (`ev_category`, `ev_action`, `ev_label` etc.) would contain values. For details, see the [Introduction to the SnowPlow events table](https://github.com/snowplow/snowplow/blog/master/docs/07_snowplow_hive_tables_introduction.md).

To count the number of page views by day, then we simply execute the following query:

	SELECT
	dt,
	COUNT(txn_id)
	FROM events
	WHERE page_title IS NOT NULL
	GROUP BY dt ;

By week:

	SELECT
	YEAR(dt),
	WEEKOFYEAR(dt)
	COUNT(txn_id)
	FROM events
	WHERE page_title IS NOT NULL
	GROUP BY YEAR(dt), WEEKOFYEAR(dt) ;

By month:

	SELECT
	YEAR(dt),
	MONTH(dt)
	COUNT(txn_id)
	FROM events
	WHERE page_title IS NOT NULL
	GROUP BY YEAR(dt), MONTH(dt) ;

<a name="counting-events" />
## 4. Number of events / transactions

Although the number of page views is a standard metric in web analytics, this reflects the web's history as a set of hyperlinked documents rather than the modern reality of web applications that are comprise lots of AJAX events (that need not necessarily result in a page load.)

As a result, counting the total number of events (including page views but also other AJAX events) is actually a more meaningful thing to do than to count the number of page views, as we have done above. We recommend setting up SnowPlow so that *all* events / actions that a user takes are tracked. Hence, running the below queries should return a total sum of events on the site by time period:

	SELECT
	dt,
	COUNT(txn_id)
	FROM events
	GROUP BY dt ;

By week:

	SELECT
	YEAR(dt),
	WEEKOFYEAR(dt)
	COUNT(txn_id)
	FROM events
	GROUP BY YEAR(dt), WEEKOFYEAR(dt) ;

By month:

	SELECT
	YEAR(dt),
	MONTH(dt)
	COUNT(txn_id)
	FROM events
	GROUP BY YEAR(dt), MONTH(dt) ;

As well as looking at page views by time period, we can also look by `user_id`. This gives a good impression of the engagement level of each of our users: how does this vary across our user population, how it varies for a particular user over time, for example.

For example, to examine the engagement by user by month, we execute the following query:

	SELECT
	YEAR(dt),
	MONTH(dt),
	user_id,
	COUNT(txn_id)
	FROM events
	GROUP BY YEAR(dt), MONTH(dt), user_id ;

There is scope to taking a progressively more nuanced approach to measuring user engagement levels. Some of the approaches are described in [this blog post](http://www.keplarllp.com/blog/2012/05/different-approaches-to-measuring-user-engagement-with-snowplow)

<a name="pages-per-visit" />
## 5. Pages per visit

The number of pages per visit can be calculated by visit very straightforwardly:

	SELECT
	user_id,
	visit_id,
	COUNT(txn_id) AS pages_per_visit
	FROM
	events
	WHERE page_title IS NOT NULL
	GROUP BY user_id, visit_id ;

To calculate the average page views per day we average over the results in the above query. This can be performed in Hive in two steps: 

	1. Performing the above query, but storing the results in a new table
	2. Averaging over that table values

The queries are given below:

	CREATE TABLE pages_per_visit (
	dt STRING,
	user_id STRING,
	visit_id INT,
	pages INT );

	INSERT OVERWRITE TABLE pages_per_visit
	SELECT
	dt,
	user_id,
	visit_id,
	COUNT(txn_id)
	FROM events
	WHERE page_title IS NULL
	GROUP BY dt, user_id, visit_id ;

	SELECT 
	dt,
	AVG(pages)
	FROM pages_per_visit
	GROUP BY dt ;

<a name="bounce-rate" />
## 6. Bounce rate

[TO WRITE]

<a name="new-visits" />
## 7. % New visits

A new visit is easily identified as a visit where the visit_id = 1. Hence, to calculate the % of new visits, we need to sum all the visits where `visit_id` = 1 and divide by the total number of visits, in the time period.

First, we calculate the number of new visits in the time period:

	CREATE TABLE new_visits_by_day (
	dt STRING,
	number_new_visits INT) ;

	INSERT OVERWRITE TABLE new_visits_by_day
	SELECT
	dt,
	COUNT(DISTINCT (user_id) )
	FROM events
	WHERE visit_id=1
	GROUP BY dt ;

Secondly, we calculate the total number of visits in the time period:

	CREATE TABLE visits_by_day (
	dt STRING,
	number_of_visits INT) ;

	INSERT OVERWRITE TABLE visits_by_day
	SELECT
	dt,
	COUNT(DISTINCT (CONCAT(user_id,visit_id)) )
	FROM events
	GROUP BY dt ;

Lastly, we take the number of new visits per time period, and divide by the total number of visits

	SELECT
	n.dt,
	number_new_visits / number_of_visits AS percentage_new_visits
	FROM new_visits_by_day n JOIN visits_by_day v ON n.dt = v.dt
	GROUP BY dt ; 

<a name="efficiency" />
## 8. A note about efficiency

Hive and Hadoop more generally are very powerful tools to process large volumes of data. However, data processing is an expensive task, in the sense that every time you execute the query, you have to pay EMR fees to crunch through your data. As a result, where possible, it is advisable not to repeat the same analysis multiple times: for repeated analyses you should save the results of the analysis, and only perform subsequent analysis on new data.

To take the example of logging the number of unique visitors by day: we could run a query to fetch calculate this data up to and included yesterday:

	SELECT 
	dt,
	COUNT(DISTINCT (user_id))
	FROM events
	WHERE dt < '{{TODAY's DATE}}'
	GROUP BY dt ;

We would then save this data in a suitable database / Excel spreadsheet, and add to it by querying just *new* data e.g.

	SELECT 
	dt,
	COUNT(DISTINCT (user_id))
	FROM events
	WHERE dt > '{{NEW DATES}}'
	GROUP BY dt ;

At the moment, the analyst has to manually append the new data to the old. Going forwards, we will build out the SnowPlow functionality so that it is straightforward to build ETL processes to migrate useful cuts of data into analytics databases for further analysis, where Hadoop / Hive is not required for that additional analysis.