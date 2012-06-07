# Querying your SnowPlow web analytics data using Hive

Many of the analyses we perform in SnowPlow use Hive. We tend to use Hive Interactive Sessions to develop queries and analyses. Once a set of queries has been developed in the interactive sessions, they can be stored as a text file in S3 and run as a batch process directly from the Elastic Mapreduce Command Line tools.

This part of the guide walks through the process of launching and running a Hive Interactive session. The steps involved are:

1. [Starting a job](#startingajob) i.e. firing up a set of instances to run Hive / Hadoop
2. [SSHing in to the instances and launching Hive](#sshin)
3. [Running Hive queries, including using the SnowPlow serde](#runninghivequeries)
4. [Terminating the session](#terminatingthesession)

Most of the analyses we perform are in Hive interactive sessions: because it is in these types of sessions that we can actively query data, explore results and develop more sophisticated analyses.

New sessions can either be initiated at the command-line, or via aws.amazon.com console. 

<a name="startingajob"/>
## Starting a job

There are 2 ways to start a job / fire up instances to run Hive: 

1. [Using the Ruby Client](#usingtherubyclient)
2. [From the Amazon Web UI](#fromtheamazonwebui)

<a name="usingtherubyclient"/>
### Starting a job using the Ruby Client

#### Starting a job using the Ruby Client on Mac / Linux

To initiative a new session on Mac / Linux, navigate to the `elastic-mapreduce-cli` folder (where you saved the command-line tools) and enter

	$ ./elastic-mapreduce --create --alive --name "Hive Job Flow" --hive-interactive --hive-version 0.7

You should see something like:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-1.jpg)

Note: The Ruby command line interface tools uses the security information you provided in the `credentials.json` file and takes a set of default values for e.g. the number of instances that are fired up. For more details, consult the [EMR command line tools documentation](http://aws.amazon.com/developertools/2264).

#### Starting a job using the Ruby Client on PC

TO WRITE: Add instructions to launch a session from the PC command-line, incl. setting up Putty and Pageant to SSH successfully

### Starting a job using the web UI

TO WRITE: Add instructions on creating jobs via the Amazon web UI. 

### Checking that the job has been setup using the Amazon web UI

Note: this will work however you initiated the job. (Whether you used the Ruby Client or the Web UI.)

Log into the [Amazon Web Console](https://console.aws.amazon.com/console/home) and click on [Elastic MapReduce] in the top menu bar. You should see the job you created listed. (In the screenshot below you'll see that we've initiated 2 Hive sessions.)

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-2.jpg)

Note: you can also check on the status of your current jobs via the command-line interface:

	./elastic-mapreduce --list

The above command will list all the current jobs including their statuses.

<a name="sshin"/>
## Establishing an SSH connection

### Establishing the SSH connection: Mac / Linux users

Return to the command-line, establish an SSH connection by entering the following

	$ ./elastic-mapreduce --ssh --jobflow {{JobFlowID}}

You can get your jobflowID either from the Amazon web UI or by listing all the jobs using the `./elastic-mapreduce --list` command line tool.

Substituting the JobFlowID generated when you created the session. You should see:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-3.jpg)

Now you can launch Hive by typing `Hive` at the command line:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-4.jpg)

### Establishing the SSH connection: PC users

TO WRITE

<a name="runninghivequeries"/>
## Running Hive queries and using the SnowPlow serde

SnowPlow data is stored in the log files generated every time the tags on your website call the SnowPlow pixel `ice.png`. The format of that data is proprietary: to access it in Hive you need to use the [SnowPlow Log Deserializer](https://github.com/snowplow/snowplow-log-deserializers), a JAR file that tells Hive how to take raw SnowPlow logs and parse them into fields that are directly accessible to the user in Hive.

The deserializer can be downloaded directly from the [downloads](https://github.com/snowplow/snowplow-log-deserializers/downloads) page on the [SnowPlow-log-deserializer](https://github.com/snowplow/snowplow-log-deserializers/downloads) repository.

Download the latest version (currently `snowplow-log-deserializers-0.4.4.jar`) and upload it to S3, via the web UI on [console.aws.amazon.com](https://console.aws.amazon.com/). Make a note of where you've saved it. (We save it in the same `static` folder we store `sp.js` and `ice.png`.)

We need to add the JAR to the Hive session, so Hive can access the deserializer. This is done by executing the following command, in the Hive interface:

	ADD JAR s3://{{JARS-BUCKET-NAME-HERE}}/snowplow-log-deserializers-0.4.4.jar

![Add the SnowPlow deserializer JAR to Hive](/snowplow/snowplow/raw/master/docs/images/hive-add-deserializer.png)

Now in Hive we need to define create a table with the data stored in the SnowPlow logs, using the deserializer. Do this by entering the following query:

	CREATE EXTERNAL TABLE snowplow_events_log
	ROW FORMAT SERDE 'com.snowplowanalytics.snowplow.hadoop.hive.SnowPlowEventDeserializer'
	LOCATION 's3://{{LOGS-BUCKET-NAME}}/'

![Create SnowPlow events table](/snowplow/snowplow/raw/master/docs/images/create-table-with-serde.png)

Don't forget to include the trailing slash in the location address. The above query creates a new table. The table is `EXTERNAL` because the data in it is not managed by Hive: it is stored in S3 (and only accessed by Hive). As a result, if you drop the table in Hive (`DROP TABLE snowplow_events_log`), the data will remain safely in S3, even if the table disappears from Hive. (You can re-enter the above query to _bring it back_):

The query instructs Hive to use the deserializer to translate the cloudfront logs into a format that Hive can read directly. It points Hive at the S3 bucket where the data is stored.

Now that the table has been created in Hive it is possible to query it:

	SHOW TABLES ;

Should return a list of tables: in this case our single `snowplow_events_logs` table.

The following query will return the number of unique visitors by day:

	SELECT 
	dt,
	COUNT(DISTINCT user_id) AS unique_visitors
	FROM snowplow_events_log 
	GROUP BY dt ;

To get the number of visits per day:

	SELECT 
	dt,
	COUNT(DISTINCT (CONCAT(user_id, visit_id))) AS visits
	FROM snowplow_events_log
	GROUP BY dt ;

Of course, in either of the above cases, we could aggregate our results by month (or any other time period) rather than by year:

	SELECT 
	YEAR(dt) AS yr,
	MONTH(dt) AS mnth,
	COUNT(DISTINCT user_id) AS unique_visitors,
	COUNT(DISTINCT (CONCAT(user_id, visit_id))) AS visits,
	FROM snowplow_events_log 
	GROUP BY yr, mnth ;


We can look at the number of 'transactions' (incl. page views, add-to-baskets, and other user actions) by each user to get a sense of whom are most engaged users are, just over May 2012:

	SELECT 
	user_id,
	COUNT(txn_id)
	FROM snowplow_log_events
	WHERE YEAR(dt) = 2012 AND MONTH(dt) = 5
	GROUP BY user_id ;

The results of the queries can be copied and pasted directly into Excel or a text-editor (from the command line terminal) for quick-and-dirty ad hoc analysis. More properly, it is trivial to write the results to S3, where they can be migrated into an analytics database for further analysis, used to feed a KPI dashboard or accessed on subsequent Hive analytics sessions. To do so, simply create a new table (making sure it is `EXTERNAL`), and output the contents of the `SELECT` statement into it as follows:

	CREATE EXTERNAL TABLE `{{YOUR-RESULTS-TABLE}}`
	LOCATION 's3://{{BUCKET-AND-FOLDER-WHERE-YOU-WISH-TO-SAVE-THE-DATA}}/'

	INSERT OVERWRITE `{{YOUR-RESULTS-TABLE}}`
	SELECT 
	user_id,
	COUNT(txn_id)
	FROM snowplow_log_events
	WHERE YEAR(dt) = 2012 AND MONTH(dt) = 5
	GROUP BY user_id ;

There are many ways of fetching the data from S3, either directly via the S3 web UI, or programmatically via the API. 


<a name="terminatingthesession"/>
## Terminating the session

Interactive sessions have to be terminated manually. (Or else you risk running up high Amazon fees...) Sessions can either be terminated at the command-line, using the Ruby Client, or via the web UI.



### Terminating the session using the web UI

TO WRITE

### Terminating the session at the command-line using the Ruby Client

To terminate the sessions via the command line, simply exit the session (by typing `exit ;` to escape Hive, then `exit` again at the EC2 command line to end the SSH session.) You then use the EMR command line tools to terminate the session:

	./elastic-mapreduce --terminate --jobflow {{JOBFLOW ID}}
