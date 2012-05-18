# SnowPlow log deserializers for Hive

## Introduction

snowplow-log-deserializers is a pair of Deserializers for importing SnowPlow logs into [Apache Hive] [hive] ready for analysis.

The specific deserializers are as follows:

1. **SnowPlowEventDeserializer** - for deserializing page views and web events. The standard SnowPlow use case
2. **SnowPlowAdImpDeserializer** - for deserializing ad impressions. Use this if you are an [ad network using SnowPlow] [snowplowads]

Both deserializers are based on our [cloudfront-log-deserializer] [cfserde], which is for general-purpose (i.e. non-SnowPlow-specific) analysis of CloudFront access log files.

cloudfront-log-deserializers is a [Scala Build Tool] [sbt] project written in Java, and is [available] [downloads] from GitHub as a downloadable jarfile.

## The SnowPlow log format

Because SnowPlow uses Amazon Web Services' CloudFront CDN for logging, the raw SnowPlow log format is identical to the [access log format] [cflogformat] for the CloudFront download distributions.

The SnowPlow-specific data is passed to CloudFront as a set of name-value pairs in the querystring attached to the request. The querystring name-value pairs are as follows:

| **KEY**           | **FULL NAME**  | **ALWAYS SET?** | **DESCRIPTION**                                                                                                                        |
|------------------:|:--------------:|:---------------:|:---------------------------------------------------------------------------------------------------------------------------------------|
| **Common**        |                |                 | _In all SnowPlow querystrings_                                                                                                         |
| `rdm`             | Random Number  | Yes             | For cachebusting - not used for analytics                                                                                              |
| `uid`             | User ID        | Yes             | Uniquely identifies the user i.e. web page visitor (strictly speaking, uniquely identifies the user's browser)                         |
| `vid`             | Visit ID       | Yes             | The visitor's current visit number. Increments each visit (i.e. is a direct counter). 30 minutes of inactivity ends a given visit      |
| `lang`            | Language       | Yes             | The visitor's web browser language (or Windows language for Internet Exporer)                                                          |
| `refr`            | Referrer       | No              | URL of the referrer to the page calling SnowPlow. Don't confuse with CloudFront's own `cs(Referer)` (URL of the page calling SnowPlow) | 
| **Page view**     |                |                 | _In the SnowPlow querystring when a page view is logged_                                                                               |
| `page`            | Page Title     | Yes             | The title of the page calling SnowPlow                                                                                                 |
| **Event**         |                |                 | _In the SnowPlow querystring when an event is logged_                                                                                  | 
| `ev_ca`           | Event Category | Yes             |     |
| `ev_ac`           | Event Action   | Yes             |     |
| `ev_la`           | Event Label    | No              |     |
| `ev_pr`           | Event Property | No              |     |
| `ev_va`           | Event Value    | No              |     |
| **Ad impression** |                |                 | _In the SnowPlow querystring when an ad impression is logged_                                                                          |
| `ad_ba`           | Ad Banner      | Yes             | |
| `ad_ca`           | Ad Campaign    | No              | |
| `ad_ad`           | Ad Advertiser  | No              | |
| `ad_uid`          | Ad User ID     | No              | |

## The Hive table format

Each SnowPlow log deserializer maps the SnowPlow log format onto an appropriate Hive table structure. The main transformation 

cloudfront-log-deserializer maps the access log format for a download distribution very directly onto an equivalent Hive table structure. The only transformation is that the querystring on the accessed URI is converted into a Hive `MAP<STRING, STRING>`.

Here is the Hive table definition in full:

    CREATE EXTERNAL TABLE impressions (
      dt STRING,
      tm STRING,
      edgelocation STRING,
      bytessent INT,
      ipaddress STRING,
      operation STRING,
      domain STRING,
      object STRING,
      httpstatus STRING,
      referrer STRING, 
      useragent STRING,
      querystring STRING
    )
    ...

## Usage

First, download the latest jarfile for cloudfront-log-deserializer from GitHub from the [Downloads] [downloads] menu.

Then upload the jarfile into an S3 bucket accessible from your Hive console.

Now using this Deserializer with Hive should be quite easy.

    ADD JAR s3://{{JARS-BUCKET-NAME}}/cf-log-deserializer-0.2.jar;

    CREATE EXTERNAL TABLE accesses 
    PARTITIONED BY (dt STRING)
    ROW FORMAT 
      SERDE 'com.snowplowanalytics.hive.serde.CfLogDeserializer'
    LOCATION 's3://{{LOGS-BUCKET-NAME}}/';

A couple of points on this:

* Don't forget the trailing slash on your `LOCATION`, or you will get a cryptic "Can not create a Path from an empty string" exception
* In the `CREATE EXTERNAL TABLE` statement above, you do **not** have to manually specify all of the columns to create for this table. This is because Hive will query the SerDe to determine the _actual_ list of columns for this table.

Once you have created this table, you should be able to perform simple tests:

    TODO

## Copyright and license

snowplow-log-deserializers is copyright 2012 Orderly Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[hive]: http://hive.apache.org/ 
[snowplowserdes]: https://github.com/snowplow/snowplow/tree/master/serdes
[awslogdocs]: http://docs.amazonwebservices.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#LogFileFormat
[license]: http://www.apache.org/licenses/LICENSE-2.0
[loganalyzer]: http://elasticmapreduce.s3.amazonaws.com/samples/cloudfront/code/cloudfront-loganalyzer.tgz
[w3cformat]: http://www.w3.org/TR/WD-logfile.html 
[s3logdeserializer]: http://javasourcecode.org/html/open-source/hive/hive-0.7.1/org/apache/hadoop/hive/contrib/serde2/s3/S3LogDeserializer.html
[downloads]: https://github.com/snowplow/cloudfront-log-deserializer/downloads