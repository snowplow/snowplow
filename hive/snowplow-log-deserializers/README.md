# SnowPlow Log Deserializers for Hive

## Introduction

snowplow-log-deserializers is a set of Deserializers which can import SnowPlow logs into [Apache Hive] [hive] ready for analysis.

The specific deserializers created to date are as follows:

1. **SnowPlowEventDeserializer** - for deserializing page views and web events. The standard SnowPlow use case
2. **SnowPlowAdImpDeserializer** - for deserializing ad impressions _(work-in-progress - see Roadmap section below)_. For [ad networks using SnowPlow] [snowplowads]

Both deserializers are based on our [cloudfront-log-deserializer] [cfserde], which is for general-purpose (i.e. non-SnowPlow-specific) analysis of CloudFront access log files.

snowplow-log-deserializers is a [Scala Build Tool] [sbt] project, written in Java, and is [available] [downloads] from GitHub as a downloadable jarfile. Tests are written in Specs2.

## The SnowPlow log format

Because SnowPlow uses Amazon Web Services' [CloudFront CDN] [cloudfront] for logging, the raw SnowPlow log format is identical to the [access log format] [cflogformat] for the CloudFront download distributions.

The SnowPlow-specific data is passed to CloudFront as a set of name-value pairs in the querystring attached to the request. The querystring name-value pairs (in **SnowPlow v0.4**) are as follows:

| **KEY**            | **FULL NAME**    | **ALWAYS SET?** | **DESCRIPTION**                                                                                                                        |
|-------------------:|:----------------:|:---------------:|:---------------------------------------------------------------------------------------------------------------------------------------|
| **Common**         |                  |                 | _Common to all SnowPlow querystrings_                                                                                                  |
| `tid`              | Transaction ID   | Yes             | Random number used to identify duplicates in the CloudFront logs                                                                       |
| `uid`              | User ID          | Yes             | Uniquely identifies the user i.e. web page visitor (strictly speaking, uniquely identifies the user's browser)                         |
| `vid`              | Visit ID         | Yes             | The visitor's current visit number. Increments each visit (i.e. is a direct counter). 30 minutes of inactivity ends a given visit      |
| `lang`             | Language         | Yes             | The visitor's web browser language (or Windows language for Internet Exporer)                                                          |
| `res`              | Screen Resolution| Yes             | The screen resolution as reported by the web browser. In the format `widthxheight` e.g. 1920x1080                                      |
| `cookie`           | Cookies Enabled? | Yes             | Whether or not the web browser has cookies enabled                                                                                     |
| `refr`             | Referrer         | No              | URL of the referrer to the page calling SnowPlow. Don't confuse with CloudFront's own `cs(Referer)` (URL of the page calling SnowPlow) | 
| `f_*`              | Browser_Features | Yes             | Whether the user's browser has specific features, e.g. `f_fla=1` means that the user's browser has Flash                               |
| `url`              | Page URL         | Yes             | The url of the page calling SnowPlow                                                                                                   | 
| **Page view**      |                  |                 | _In the SnowPlow querystring whenever a page view is logged_                                                                           |
| `page`             | Page Title       | Yes (for page views)| The title of the page calling SnowPlow                                                                                             |
| **Event**          |                  |                 | _In the SnowPlow querystring whenever an event is logged_                                                                              | 
| `ev_ca`            | Event Category   | Yes (for events)| The name you supply for the group of objects you are tracking                                                                          |
| `ev_ac`            | Event Action     | Yes (for events)| A string which defines the type of user interaction for the web object                                                                 |
| `ev_la`            | Event Label      | No              | An optional string which identifies the specific object being actioned                                                                 | 
| `ev_pr`            | Event Property   | No              | An optional string describing the object or the action performed on it                                                                 |
| `ev_va`            | Event Value      | No              | An optional float to quantify or further describe the user action                                                                      |
| **Ad imp**         |                  |                 | _In the SnowPlow querystring whenever an ad impression is logged_                                                                      |
| `ad_ba`            | Ad Banner ID     | Yes             | Adserver identifier for the ad banner (creative) being displayed                                                                       |
| `ad_ca`            | Ad Campaign ID   | No              | Adserver identifier for the ad campaign which the banner belongs to                                                                    |
| `ad_ad`            | Ad Advertiser ID | No              | Adserver identifier for the advertiser which the campaign belongs to                                                                   |
| `ad_uid`           | Ad User ID       | No              | Adserver identifier for the web user. Not to be confused with SnowPlow's own user identifier                                           |

## The Hive table format

Each SnowPlow log deserializer maps the SnowPlow log format onto an appropriate Hive table structure. There are two main transformations handled by each deserializer:

1. Extracting the user's browser, screen resolution, OS etc from the CloudFront `useragent` field 
2. Extracting the relevant name-value pairs from the CloudFront `cs-uri-query` aka querystring field

The Hive table definitions for each deserializer are shown below:

### 1. SnowPlowEventDeserializer Hive table

This is the Hive table for **Page views** and **Events**:

```hiveql
CREATE EXTERNAL TABLE events (
  dt STRING,
  tm STRING,
  txn_id STRING,
  user_id STRING,
  user_ipaddress STRING,
  visit_id INT,
  page_url STRING,
  page_title STRING,
  page_referrer STRING,  
  mkt_source STRING,
  mkt_medium STRING,
  mkt_term STRING,
  mkt_content STRING,
  mkt_name STRING,
  ev_category STRING,
  ev_action STRING,
  ev_label STRING,
  ev_property STRING,
  ev_value STRING,
  br_name STRING,
  br_family STRING,
  br_version STRING,
  br_type STRING,
  br_renderengine STRING,
  br_lang STRING,
  br_features ARRAY<STRING>,
  br_cookies BOOLEAN,
  os_name STRING,
  os_family STRING,
  os_manufacturer STRING,
  dvce_type STRING,
  dvce_ismobile BOOLEAN,
  dvce_type STRING,
  dvce_screenwidth INT,
  dvce_screenheight INT
)
```

A full description of each of these fields is out of scope of this documentation. Please see the [Events table definition] [eventstabledef] in the **Introduction to the SnowPlow Hive Tables** documentation for details.

### 2. SnowPlowAdImpDeserializer Hive table

This is the Hive table for **Ad impressions**:

```hiveql
CREATE EXTERNAL TABLE imps (
  dt STRING,
  tm STRING,
  txn_id STRING,
  user_id STRING,
  user_ipaddress STRING,
  visit_id INT,
  page_url STRING,
  page_referrer STRING,  
  mkt_source STRING,
  mkt_medium STRING,
  mkt_term STRING,
  mkt_content STRING,
  mkt_name STRING,
  ad_campaign_id STRING,
  ad_banner_id STRING,
  ad_advertiser_id STRING,
  ad_user_id STRING,
  br_name STRING,
  br_family STRING,
  br_version STRING,
  br_type STRING,
  br_renderengine STRING,
  br_lang STRING,
  br_features ARRAY<STRING>,
  br_cookies BOOLEAN,
  os_name STRING,
  os_family STRING,
  os_manufacturer STRING,
  dvce_type STRING,
  dvce_ismobile BOOLEAN,
  dvce_screenwidth INT,
  dvce_screenheight INT
)
```

A full description of each of these fields is out of scope of this documentation. Please see the [Ad impressions table definition] [adimpstabledef] in the **Introduction to the SnowPlow Hive Tables** documentation for details.

## Usage

First, download the latest jarfile for snowplow-log-deserializers from GitHub from the [Downloads] [downloads] menu.

Next, upload the jarfile into an S3 bucket accessible from your Hive console.

Now using these deserializers with Hive should be quite easy - here's an example using the SnowPlowEventDeserializer:

    ADD JAR s3://{{JARS-BUCKET-NAME}}/snowplow-log-deserializers-0.4.0.jar;

    CREATE EXTERNAL TABLE views_events 
    PARTITIONED BY (dt STRING)
    ROW FORMAT 
      SERDE 'com.snowplowanalytics.snowplow.hadoop.hive.SnowPlowEventDeserializer'
    LOCATION 's3://{{LOGS-BUCKET-NAME}}/';

Some points on this:

* Replace the `{{x-BUCKET-NAME}}` variables with your specific filepaths on Amazon S3
* Don't forget the trailing slash on your `LOCATION`, or you will get a cryptic "Can not create a Path from an empty string" exception
* In the `CREATE EXTERNAL TABLE` statement above, you do **not** have to manually specify all of the columns to create for this table. This is because Hive will query the SerDe to determine the _actual_ list of columns for this table.

Once you have created this table, you should be able to perform simple tests:

    TODO

## Roadmap

There are some outstanding tasks with these deserializers:

1. Add the functionality (and tests) around extracting the five marketing (`mkt_`) fields
2. Implement the `SnowPlowAdImpDeserializer` - this is lower priority, so please [vote for this GitHub issue] [adserdeticket] if you need this sooner

## Copyright and license

SnowPlow is copyright 2012 SnowPlow Analytics Ltd.

Contains the [Java Library for User-Agent Information] [useragentlib],
copyright 2008 Harald Walker (bitwalker.nl).

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[hive]: http://hive.apache.org/
[cloudfront]: http://aws.amazon.com/cloudfront/
[snowplowads]: http://www.keplarllp.com/blog/2012/05/warehousing-your-online-ad-data-with-snowplow
[sbt]: https://github.com/harrah/xsbt/wiki
[cflogformat]: https://github.com/snowplow/cloudfront-log-deserializer/blob/master/README.md#the-cloudfront-access-log-format
[eventstabledef]: https://github.com/snowplow/snowplow/blob/master/docs/07_snowplow_hive_tables_introduction.md#1-events
[adimpstabledef]: https://github.com/snowplow/snowplow/blob/master/docs/07_snowplow_hive_tables_introduction.md#2-ad-impressions
[useragentlib]: http://user-agent-utils.java.net/
[adserdeticket]: https://github.com/snowplow/snowplow-log-deserializers/issues/5
[cfserde]: https://github.com/snowplow/cloudfront-log-deserializer
[license]: http://www.apache.org/licenses/LICENSE-2.0
[downloads]: https://github.com/snowplow/snowplow-log-deserializers/downloads
