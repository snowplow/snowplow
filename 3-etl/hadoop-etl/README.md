# SnowPlow Hadoop ETL

## Introduction

This is the SnowPlow ETL process implemented for Hadoop using [Scalding] [scalding].

The SnowPlow Hadoop ETL process is an alternative to the SnowPlow Hive ETL process. 

## Technical approach

The Hadoop ETL parses raw CloudFront log files, extracts the SnowPlow events, enriches them (e.g. with geo-location information) and then writes them out to SnowPlow-format flatfiles.

Like the Hive ETL, the Hadoop ETL can be run on [Amazon Elastic MapReduce] [emr] using the EMR ETL Harness, a Rubygem.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-etl/hadoop-etl
    $ sbt assembly

The 'fat jar' is now available as:

    upload/snowplow-hadoop-etl-0.0.1.jar

## Unit testing

The `assembly` command above runs the test suite - but you can also run this manually with:

    $ sbt test
    <snip>
    [info] + A WordCount job should
	[info]   + count words correctly
	[info] Passed: : Total 2, Failed 0, Errors 0, Passed 2, Skipped 0

## Running on Amazon EMR

First, upload the jar to S3 - if you haven't yet built the project (see above), you can grab the latest copy of the jar from this repo's [Downloads] [downloads].

Next, upload the data file [`data/hello.txt`] [hello-txt] to S3.

Finally, you are ready to run this job using the [Amazon Ruby EMR client] [emr-client]:

    $ elastic-mapreduce --create --name "scalding-example-project" \
      --jar s3n://{{JAR_BUCKET}}/scalding-example-project-0.0.1.jar \
      --arg com.snowplowanalytics.hadoop.scalding.WordCountJob \
      --arg --hdfs \
      --arg --input --arg s3n://{{IN_BUCKET}}/hello.txt \
      --arg --output --arg s3n://{{OUT_BUCKET}}/results

## Checking your results

Once the output has completed, you should see a folder structure like this in your output bucket:

     results
     |
     +- _SUCCESS
     +- part-00000

Download the `part-00000` file and check that it contains:

	goodbye	1
	hello	1
	world	2

## Troubleshooting

If you are trying to run this on a non-Amazon EMR environment, you may need to edit:

    project/BuildSettings.scala

And comment out the Hadoop jar exclusions:

    // "hadoop-core-0.20.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
    // "hadoop-tools-0.20.2.jar" // "

## Next steps

Fork this project and adapt it into your own custom Scalding job.

Use the excellent [Elasticity] [elasticity] Ruby library to invoke/schedule your Scalding job on EMR.

## Roadmap

Nothing planned - although it would be nice to upgrade from Specs to Specs2 for the testing, and bump Scala to 2.9.1 at the same time. If you would like to do this, feel free to submit a pull request.

## Copyright and license

Copyright 2012 SnowPlow Analytics Ltd, with significant portions copyright 2012 Twitter, Inc.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[wordcount]: https://github.com/twitter/scalding/blob/master/README.md
[scalding]: https://github.com/twitter/scalding/
[snowplow]: http://snowplowanalytics.com
[emr]: http://aws.amazon.com/elasticmapreduce/
[downloads]: https://github.com/snowplow/scalding-example-project/downloads
[hello-txt]: https://github.com/snowplow/scalding-example-project/raw/master/data/hello.txt
[emr-client]: http://aws.amazon.com/developertools/2264
[elasticity]: https://github.com/rslifka/elasticity
[license]: http://www.apache.org/licenses/LICENSE-2.0