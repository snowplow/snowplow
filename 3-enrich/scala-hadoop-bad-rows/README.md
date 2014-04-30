# Scalding Example Project [![Build Status](https://travis-ci.org/snowplow/scalding-example-project.png)](https://travis-ci.org/snowplow/scalding-example-project)

## Introduction

To come.

### Run

You are ready to run this job using the [Amazon Ruby EMR client] [emr-client]:

    $ elastic-mapreduce --create --name "Extract raw events from Snowplow bad row JSONs" \
      --instance-type m1.xlarge --instance-count 3 \
      --jar s3://snowplow-hosted-assets/3-enrich/scala-bad-rows/snowplow-bad-rows-0.1.0.jar \
      --arg com.snowplowanalytics.hadoop.scalding.SnowplowBadRowsJob \
      --arg --hdfs \
      --arg --input --arg s3n://{{PATH_TO_YOUR_FIXABLE_BAD_ROWS}} \
      --arg --output --arg s3n://{{PATH_WILL_BE_STAGING_FOR_EMRETLRUNNER}}

Replace `{{JAR_BUCKET}}`, `{{IN_BUCKET}}` and `{{OUT_BUCKET}}` with the appropriate paths.

## Next steps

Fork this project and adapt it into your own custom Scalding job.

To invoke/schedule your Scalding job on EMR, check out:

* [Spark Plug] [spark-plug] for Scala
* [Elasticity] [elasticity] for Ruby
* [Boto] [boto] for Python
* [Lemur] [lemur] for Clojure

## Roadmap

Nothing planned currently.

## Copyright and license

Copyright 2012-2014 Snowplow Analytics Ltd, with significant portions copyright 2012 Twitter, Inc.

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
[snowplow-hadoop-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/scala-hadoop-enrich
[spark-example-project]: https://github.com/snowplow/spark-example-project
[emr]: http://aws.amazon.com/elasticmapreduce/
[hello-txt]: https://github.com/snowplow/scalding-example-project/raw/master/data/hello.txt
[emr-client]: http://aws.amazon.com/developertools/2264
[elasticity]: https://github.com/rslifka/elasticity
[spark-plug]: https://github.com/ogrodnek/spark-plug
[lemur]: https://github.com/TheClimateCorporation/lemur
[boto]: http://boto.readthedocs.org/en/latest/ref/emr.html
[license]: http://www.apache.org/licenses/LICENSE-2.0
