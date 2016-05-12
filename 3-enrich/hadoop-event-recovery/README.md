# Snowplow Hadoop Event Recovery

## Introduction

Use this Scalding job to extract raw Snowplow events from your Snowplow bad rows JSONs and fix any problems with them, making them ready for reprocessing.

## Usage

Run this job using the [Amazon Ruby EMR client] [emr-client]:

```
aws emr create-cluster --applications Name=Hadoop --ec2-attributes '{
    "InstanceProfile":"EMR_EC2_DefaultRole",
    "AvailabilityZone":"us-east-1d",
    "EmrManagedSlaveSecurityGroup":"sg-2f9aba4b",
    "EmrManagedMasterSecurityGroup":"sg-2e9aba4a"
}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://{{path to logs}}' --steps '[
{
    "Args":[
        "com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob",
        "--input",
        "s3://{{path to enriched}}/bad/run=2015-12-*,s3://{{path to enriched}}/bad/run=2016-01-*",
        "--output",
        "s3://{{path to output bucket}}",
        "--script",
        "ZnVuY3Rpb24gcHJvY2VzcyhldmVudCwgZXJyb3JzKSB7CiAgICAvLyBPbmx5IHJlcHJvY2VzcyBpZjoKICAgIC8vIDEuIHRoZXJlIGlzIG9ubHkgb25lIHZhbGlkYXRpb24gZXJyb3IgYW5kCiAgICAvLyAyLiB0aGUgZXJyb3IgcmVmZXJlbmNlcyBSRkMgMjM5Niwgd2hpY2ggc3BlY2lmaWVzIHdoYXQgbWFrZXMgYSBVUkwgdmFsaWQuCiAgICBpZiAoZXJyb3JzLmxlbmd0aCA8IDIgJiYgL1JGQyAyMzk2Ly50ZXN0KGVycm9yc1swXSkpIHsKICAgICAgICB2YXIgZmllbGRzID0gdHN2VG9BcnJheShldmVudCk7CiAgICAgICAgZmllbGRzWzldID0gJ2h0dHA6Ly93d3cucGxhY2Vob2xkZXIuY29tJ1w7CiAgICAgICAgcmV0dXJuIGFycmF5VG9Uc3YoZmllbGRzKTsKICAgIH0gZWxzZSB7CiAgICAgICAgcmV0dXJuIG51bGw7CiAgICB9Cn0K"
    ],
    "Type":"CUSTOM_JAR",
    "ActionOnFailure":"CONTINUE",
    "Jar":"s3://snowplow-hosted-assets/3-enrich/hadoop-event-recovery/snowplow-hadoop-event-recovery-0.1.0.jar",
    "Name":"Fix up bad rows"
}]' --name 'MyCluster' --instance-groups '[
    {
        "InstanceCount":1,
        "InstanceGroupType":"MASTER",
        "InstanceType":"m1.medium",
        "Name":"MASTER"
    },
    {
        "InstanceCount":2,
        "InstanceGroupType":"CORE",
        "InstanceType":"m1.medium",
        "Name":"CORE"
    }
]'
```

Replace the `{{...}}` placeholders above with the appropriate bucket paths.

## Copyright and license

Copyright 2014 Snowplow Analytics Ltd, with significant portions copyright 2012 Twitter, Inc.

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
