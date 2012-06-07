# Converting SnowPlow Cloudfront logs into Hive tables optimised for analysis

## Contents

1. [Why convert SnowPlow Cloudfront logs into data tables with a different format?](#why-convert-cloudfront-logs-into-snowplow-formatted-data-tables) (When we could do analysis on the SnowPlow Cloudfront logs themselves?)
2. [Performing the ETL](#performing-the-etl-process)
3. [Implementing regular (daily) jobs to move data](#implementing-the-daily-etl)
4. [Using the optimized events table](#using-the-optimized-events-table)

<a name="why-convert-cloudfront-logs-into-snowplow-formatted-data-tables"/>
## 1. Why convert Cloudfront logs into SnowPlow formatted data tables?

SnowPlow tracking tags store data about how users have engaged on your website to Amazon S3, via Cloudfront logging. The [snowplow-log-deserializer](https://github.com/snowplow) makes it possible to define a table in Hive populated with the data stored in the SnowPlow cloudfronts logs, and query that data directly.

We recommend, however, that rather than doing the analysis directly on that data, transferring it in S3 into a new bucket partitioned by date and user_id. There are two advantages:

1. This allows for faster, more efficient fetching of the data. (Especially when only fetching data in a specific date range.)
2. This allows us to delete data elements that are captured in the cloudfront logs, that do not reveal anything interesting about user behaviour on your website. 

<a name="performing-the-etl-process"/>
## 2. Performing the ETL process
 
The ETL can be run as a batch job. The queries to run the batch job are stored in the file [daily-etl](https://github.com/snowplow/snowplow/hive/etl/daily-etl.q). The queries take one days worth of data (specified when running the job, and copy that data from the Cloudfront logs into a new partition in a new `events` table.

To run the script, you need to upload it to S3 e.g. via the [Amazon S3 web UI](https://console.aws.amazon.com). Once uploaded, you can run it using the Elastic MapReduce Command Line Tools, by executing the following command:

	./elastic-mapreduce --create --name "daily ETL" \
		--hive-script s3://psychicbazaar-snowplow-static/daily-snowplow-etl-dynamic.q \
		--hive-versions 0.7 \
		--args -d,CLOUDFRONTLOGS=s3://{{BUCKET-WHERE-CLOUDFRONT-LOGS-ARE-STORED}}/ \
		--args -d,EVENTSTABLE=s3://{{NEW-BUCKET-WHERE-THE-OPTIMIZED-TABLE-WILL-BE-STORED}}/ \
		--args -d,DATE={{THE-DATE-FOR-WHICH-YOU-WISH-TO-TRANSFER-DATA e.g. 2012-05-28}}

We recommend setting up a daily CRON job that copies the previous days worth of data into the optimized table. The CRON job just needs to calculate yesterday's date and use that date as the 3rd argument to pass into Hive using the above command.

<a name="implementing-the-daily-etl"/>
## 3. Implementing the daily ETL

There are two parts to performing the ETL: 

	1. Copying the data from the log files into the optimised format in S3, using the above script
	2. Deleting (or archiving) the old log files

Running the above query will only copy data from the SnowPlow Cloudfront logs into the partitioned table. It is important that once data has been copied, the log files are archived (e.g. moved into a different bucket) or deleted. If this doesn't happen, the script will become more expensive to run, as it processes all the log files present, so without archiving the cost of performing the query will increase with the data volume.

[ALEX TO WRITE?]

<a name="using-the-optimized-events-table"/>
## 4. Using the optimized events table

Accessing the optimized table in Hive is straightforward. Because the data is stored in Hive's default format, there is no need to use a deserializer to access it:

	CREATE EXTERNAL TABLE events
	tm string,
	txn_id string,
	user_ipaddress string,
	visit_id int,
	page_url string,
	page_title string,
	page_referrer string,
	mkt_source string,
	mkt_medium string,
	mkt_term string,
	mkt_content string,
	mkt_name string,
	ev_category string,
	ev_action string,
	ev_label string,
	ev_property string,
	ev_value string,
	br_name string,
	br_family string,
	br_version string,
	br_type string,
	br_renderengine string,
	br_lang string,
	br_features array<string>,
	br_cookies boolean,
	os_name string,
	os_family string,
	os_manufacturer string,
	dvce_type string,
	dvce_ismobile boolean,
	dvce_screenwidth int,
	dvce_screenheight int	
	)
	PARTITIONED BY (dt STRING, user_id STRING)
	LOCATION 's3://{{BUCKET-NAME-WHERE-SNOWPLOW-EVENTS-TABLE-IS-STORED}}/' ;	

Once created, you need to tell Hive to look for the partitions that exist

	ALTER TABLE events RECOVER PARTITIONS ;

You can then list the partitions that exist:

	SHOW PARTITIONS events ;

Note: because the fields in the table are *exactly* the same as those in the SnowPlow Cloudfront logs, any query that works for one works for the other. (For that reason, you can use any of the [recipes](https://github.com/snowplow/snowplow/tree/master/hive/receipes) we document on either table.) However, query performance should be faster (and hence Amazon EMR costs should be lower) if you perform the queries against the partitioned table above, especially if your queries are limited to particular data partitions. (E.g. if you specify a date or user_id rangein you WHERE clause, in which case *only* relevant partitions will be processed to answer your query.)

