# Bread and butter web analytics queries part 2

In the [previous document](https://github.com/snowplow/snowplow/blob/master/hive/recipes/01_bread_and_butter_web_analytics_queries_pt1.md) we looked at the queries to return the standard web analytics data e.g. Google Analytics returns on its *Overview* page.

In this document we give the queries required to return the web metrics Google Analytics provides under its *Overview* section i.e.:

[1. Demographics: language](#language)
[2. Demographics: location](#location)
[3. Behaviour: new vs returning](#new-vs-returning)
[4. Behaviour: frequency](#frequency)
[5. Behaviour: recency](#recency)
[6. Behaviour: engagement](#engagement)
[7. Technology: browser](#browser)
[8. Technology: operating system](#os)
[9. Technology: mobile](#mobile)


Remember, these are *NOT* the queries SnowPlow was designed to perform. SnowPlow was built from the ground up to make a wide range of analytics, that are *not* possible with Google Analytics and other web analytics packages, straightforward to perform. (Especially customer and product analytics.) However, the purpose of providing these queries is to show that these *traditional* queries are still very easy to perform in SnowPlow, because SnowPlow is so flexible.

<a name="language" />
## 1. Demographics: language

For each event the browser language is stored in the `br_language` field. As a result, counting the number of visits in a time period by language is trivial:

	SELECT 
	br_language,
	COUNT(DISTINCT (CONCAT(user_id, visit_id))) AS visits
	FROM events
	WHERE {{ENTER-DESIRED-TIME-PERIOD}}
	GROUP BY br_language 
	ORDER BY COUNT(DISTINCT (CONCAT(user_id, visit_id))) DESC ;

<a name="location" />
## 2. Demographics: location

[THIS NEEDS TO BE DONE IN CONJUNCTION WITH MAXIMIND DATABASE OR OTHER GEOIP DATABASE, BASED ON IP - TO WRITE]

<a name="new-vs-returning" />
## 3. Behaviour: new vs returning

Within a given time period, we can compare the number of new visitors (for whom `visit_id` = 1) with returning visitors (for whom `visit_id` > 1). First we create a visits table, and differentiate new visits from returning visits

	CREATE TABLE visits_with_new_vs_returning_info (
	user_id STRING,
	visit_id STRING,
	new TINYINT,
	returning TINYINT
	) ;

	INSERT OVERWRITE TABLE visits_with_new_vs_returning_info
	SELECT
	user_id,
	visit_id,
	IF((MAX(visit_id)=1),1,0),
	IF((MAX(visit_id)>1),1,0)
	FROM events
	WHERE {{INSERT-TIME-PERIOD-RESTRICTIONS}}
	GROUP BY user_id, visit_id ;

Now we can sum over the table to calculate the number of new visits vs returning visits

	SELECT
	COUNT(visit_id) AS total_visits,
	SUM(new) AS new_visitors,
	SUM(returning) AS returning_visitors,
	SUM(new)/COUNT(visit_id) AS fraction_new,
	SUM(returning)/COUNT(visit_id) AS fraction_returning
	FROM visits_with_new_vs_returning_info ;

<a name="frequency" />
## 4. Behaviour: frequency

We can look at the distribution of users by number of visits they have performed in a given time period. First, we count the number of visits each user has performed in the specific time period:

	CREATE TABLE users_by_frequency (
	user_id STRING,
	visit_count INT
	) ;

	INSERT OVERWRITE TABLE users_by_frequency 
	SELECT
	user_id,
	COUNT(DISTINCT (visit_id))
	FROM events
	WHERE {{INSERT-CONDITIONS-FOR-TIME-PERIOD-YOU-WANT-TO-EXAMINE}}
	GROUP BY user_id ;

Now we need to categorise each user by the number of visits performed in the time period, and sum the number of users in each category:

	SELECT
	visit_count,
	COUNT(user_id)
	FROM users_by_frequency
	GROUP BY visit_count ;	

<a name="recency" />
## 5. Behaviour: recency

We can look in a specific time period at each user who has visited, and see how many days it has been since they last visited. First, we identify all the users who have visited in our time frame, and grab the timestamp for their last event for each:

	CREATE TABLE users_by_recency (
	user_id STRING,
	last_action_timestamp STRING,
	days_from_today BIGINT
	) ;

	INSERT OVERWRITE TABLE users_by_recency
	SELECT
	user_id,
	MAX(CONCAT(dt, " ", tm)) AS last_action_timestamp,
	CEILING((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(MAX(CONCAT(dt, " ", tm))))/(60*60*24)) AS days_from_today
	FROM events
	WHERE dt>'{{ENTER-START-DATE}}'
	GROUP BY user_id ;

Now we categorise the users by the number of days since they last visited, and sum the number in each category:

	SELECT 
	days_from_today,
	COUNT( user_id )
	FROM users_by_recency
	GROUP BY days_from_today ;

<a name="engagement" />
## 6. Behaviour: engagement

Google Analytics provides two sets of metrics to indicate *engagement*. We think that both are weak (the duration of each visit and the number of page views per visit). Nonetheless, they are both easy to measure using SnowPlow. To start with the duration per visit, we simply execute the following query:

	SELECT
	user_id,
	visit_id,
	UNIX_TIMESTAMP(MAX(CONCAT(dt," ",tm)))-UNIX_TIMESTAMP(MIN(CONCAT(dt," ",tm))) AS duration
	FROM events
	WHERE {{ANY-TIME-PERIOD-LIMITATIONS}}
	GROUP BY user_id, visit_id ;

In the same way, we can look at the number of page views per visit:

	SELECT
	user_id,
	visit_id,
	COUNT(txn_id)
	FROM events
	WHERE page_title IS NOT NULL
	GROUP BY user_id, visit_id ;

<a name="browser" />
## 7. Technology: browser

Browser details are stored in the events table in the `br_name`, `br_family`, `br_version`, `br_type`, `br_renderingengine`, `br_features` and `br_cookies` fields.

Looking at the distribution of visits by browser is straightforward:

	SELECT 
	br_name,
	COUNT( DISTINCT (CONCAT(user_id, visit_id)))
	FROM events
	WHERE {{ANY DATE CONDITIONS}}
	GROUP BY br_name ;

If you didn't want to distinguish between different versions of the same browser in the results, replace `br_name` in the query with `br_family`.

<a name="os" />
## 8. Technology: operating system

Operating system details are stored in the events table in the `os_name`, `os_family` and `os_manufacturer` fields.

Looking at the distribution of visits by operating system is straightforward:

	SELECT
	os_name,
	COUNT( DISTINCT (CONCAT(user_id, visit_id)))
	FROM events
	WHERE {{ANY DATE CONDITIONS}}
	GROUP BY os_name ;

<a name="mobile" />
## 9. Technology: mobile

Mobile technology details are stored in the 4 device/hardware fields: `dvce_type`, `dvce_ismobile`, `dvce_screenwidth`, `dvce_screenheight`.

To work out how the number of visits in a given time period splits between visitors on mobile and those not, simply execute the following query:

	SELECT
	dvce_ismobile,
	COUNT( DISTINCT (CONCAT(user_id, visit_id)))
	FROM events
	WHERE {{ANY DATE CONDITIONS}}
	GROUP BY dvce_ismobile ;

To break down the number of visits from mobile users by device type execute the following query:

	SELECT
	dvce_type,
	COUNT( DISTINCT (CONCAT(user_id, visit_id)))
	FROM events
	WHERE {{ANY DATE CONDITIONS}} AND dvce_ismobile = TRUE
	GROUP BY dvce_type ;