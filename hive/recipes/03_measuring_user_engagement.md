# Measuring user engagement

User engagement is a critical metric to understand for every business with an online component. What constitutes "successful engagement" depends very much on the specific business: if you are a content business, you should be interested in getting users to engage either frequently (e.g. every day for newspapers) and deeply (e.g. view several articles per session). For a search engine, successful engagement might look completely different however: a successful engagement might mean finding the right link in as short a time as possible and then disappearing from the site. In that case, shorter visitor duration might indicate a better user experience.

Because what constitutes "successful engagement" varies depending on the type of business and type of website, we cannot offer a definitive guide to measuring engagement. Instead, we give a number of different queries, which reflect a number of different approaches to measuring engagement. SnowPlow is flexible enough that most businesses should be able to develop robust engagement metrics and use SnowPlow to report on those metrics.

Note: for a more in-depth discussion of measuring user engagement, particularly with respect to performing cohort analyses, see [Different approaches to measuring user engagement](http://www.keplarllp.com/blog/2012/05/different-approaches-to-measuring-user-engagement-with-snowplow) on the [Keplar blog](http://www.keplarllp.com/blog).

*How often a user visits the site* i.e. engagement breadth

1. [Number of days per time week / month that user visits site](#days-per-time-period) 
2. [Number of visits by each user per day / week / month](#visits-per-time-period)

*How deeply a user engages with the site* i.e. engagement depth

3. [Number of events per visit](#events-per-visit)
4. [Weighting events by value](#weighted-events-per-visit)

<a name="days-per-time-period" />
## 1. Measuring the number of days per week / month that users visit the site

This is a key metric employed by the social network Facebook, amongst others.

To calculate it, we first calculate the figure per user_id per time period (e.g. month):

	CREATE TABLE days_visited_per_user_per_month (
	user_id STRING,	
	yr INT,
	mnth INT,
	visits INT
	) ;

	INSERT OVERWRITE TABLE days_visited_per_user_per_month 
	SELECT
	user_id,
	YEAR(dt),
	MONTH(dt),
	COUNT(DISTINCT (visit_id) )
	FROM events
	GROUP BY user_id, YEAR(dt), month(dt) ;

We can then look at the distribution of users by the number of days per month they have logged in, for any month in particular:

	SELECT
	visits,
	count(user_id)
	FROM days_visited_per_month
	WHERE yr=12 AND mnth=5
	GROUP BY visits ;

We might also want to see how the distribution evolves by month:

	SELECT
	yr,
	mnth,
	visits,
	count(user_id)
	FROM days_visited_per_month
	GROUP BY yr, month, visits ;

Note: as well as looking at how the distribution of users by engagement level changes over time, we might also want to look at how it changes for a fixed group of users. This is normally performed as part of a [cohort analysis](http://www.keplarllp.com/blog/2012/05/performing-cohort-analysis-on-web-analytics-data-using-snowplow).

<a name="visits-per-time-period" />
## 2. Number of visits by each user per day / week / month

A similar metric is to count the number of visits that each user makes in a given time period. The difference here, is that if a user visits a site more than once a day, each individual visit contributes to the "engagement" value assigned to that user. When we look at the number of days per month a user visits a website, by contrast, we do not distinguish users who've visited once from users who've visited twice. 

Which approach is better depends on the type of product / service you offer online and the way users engage with it. If users typically open your service in a browser window, and then leave it on (but in the background), distinguishing users who visit once and twice a day may not be meaningful. If each visit is distinct, however, this metric might be preferable.

To calculate it, we first calculate the number of visits performed per user per time period. (In the below example we use a month as a time period):

	CREATE TABLE visits_by_user_by_month (
	user_id STRING,
	yr INT,
	mnth INT,
	visits INT
	) ;

	INSERT OVERWRITE TABLE visits_by_user_by_month
	SELECT
	user_id,
	YEAR(dt),
	MONTH(dt),
	COUNT( DISTINCT( visit_id ))
	GROUP BY user_id, YEAR(dt), MONTH(dt) ;

Now we can look at the distribution of users, by numbers of visits per time period, in each time period:

	SELECT
	yr,
	mnth,
	visits,
	COUNT(user_id)
	FROM visits_by_user_by_month
	GROUP BY yr, mnth, visits ;

<a name="events-per-visit" />
## 3. Number of events per visit

We can take the number of "events" that occur on each visit as a proxy for how "engaged" the user was during that visit. (With more events indicating a deeper level of engagement.) 

Counting the number of events per user per visit is straightforward:

	CREATE TABLE engagement_by_visit (
	user_id STRING,
	visit_id INT,
	engagement INT
	) ;

	INSERT OVERWRITE TABLE engagement_by_visit
	SELECT
	user_id,
	visit_id,
	COUNT(txn_id)
	FROM events
	GROUP BY user_id, visit_id ;

We can then look at the distribution of visits by engagement level:

	SELECT
	engagement,
	COUNT(*)
	FROM events
	GROUP BY engagement ;

If we want to see whether this metric is improving over time, we can repeat the above, but this time note the date of each visit, and aggregate by time period:

	CREATE TABLE engagement_by_visit (
	user_id STRING,
	visit_id INT,
	dt STRING,
	engagement INT
	) ;

	INSERT OVERWRITE TABLE engagement_by_visit
	SELECT
	user_id,
	visit_id,
	MIN(dt),
	COUNT(txn_id)
	FROM events
	GROUP BY user_id, visit_id ;	

	SELECT
	dt,
	engagement,
	COUNT(*)
	FROM events
	GROUP BY dt, engagement ;

<a name="weighted-events-per-visit" />
## 4. Weighting events by value

[TO WRITE]