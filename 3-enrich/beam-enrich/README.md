# Beam Enrich

## Introduction

Beam Enrich processes raw Snowplow events from an input [GCP PubSub][pubsub] subscription,
enrich them and store them into an output PubSub topic.
Events are enriched using the [scala-common-enrich][common-enrich] library.

## Building

This project uses [sbt-native-packager][sbt-native-packager].

### Zip archive

To build the zip archive, run:

```bash
sbt universal:packageBin
```

### Docker image

To build a Docker image, run:

```bash
sbt docker:publishLocal
```

## Running

### Directly

Once unzipped the artifact can be run as follows:

```bash
./bin/beam-enrich \
  --runner=DataFlowRunner \
  --project=project-id \
  --streaming=true \
  --zone=europe-west2-a \
  --gcpTempLocation=gs://location/ \
  --raw=projects/project/subscriptions/raw-topic-subscription \
  --enriched=projects/project/topics/enriched-topic \
  --bad=projects/project/topics/bad-topic \
  --pii=projects/project/topics/pii-topic \ #OPTIONAL
  --resolver=iglu_resolver.json \
  --enrichments=enrichments/
```

To display the help message:

```bash
./bin/beam-enrich \
  --runner=DataFlowRunner \
  --help
```

### Through a docker container

A container can be run as follows:

```bash
docker run \
  -v $PWD/config:/snowplow/config \
  -e GOOGLE_APPLICATION_CREDENTIALS=/snowplow/config/credentials.json \ # if running outside GCP
  snowplow-docker-registry.bintray.io/snowplow/beam-enrich:0.1.0 \
  --runner=DataFlowRunner \
  --job-name=snowplow-enrich \
  --project=project-id \
  --streaming=true \
  --zone=europe-west2-a \
  --gcpTempLocation=gs://location/ \
  --raw=projects/project/subscriptions/raw-topic-subscription \
  --enriched=projects/project/topics/enriched-topic \
  --bad=projects/project/topics/bad-topic \
  --pii=projects/project/topics/pii-topic \ #OPTIONAL
  --resolver=/snowplow/config/iglu_resolver.json \
  --enrichments=/snowplow/config/enrichments/
```

To display the help message:

```bash
docker run snowplow-docker-registry.bintray.io/snowplow/beam-enrich:0.1.0 \
  --runner=DataFlowRunner \
  --help
```

### Additional information

Note that, for the enrichments relying on local files, the files need to be accessible from Google
Cloud Storage, e.g. for the IP lookups enrichment:

```json
{
  "schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
  "data": {
    "name": "ip_lookups",
    "vendor": "com.snowplowanalytics.snowplow",
    "enabled": true,
    "parameters": {
      "geo": {
        "database": "GeoLite2-City.mmdb",
        "uri": "gs://ben-sp-test/maxmind"
      }
    }
  }
}
```

A full list of all the Beam CLI options can be found at:
https://cloud.google.com/dataflow/pipelines/specifying-exec-params#setting-other-cloud-pipeline-options.

## Testing

To run the tests:

```
sbt test
```

## REPL

To experiment with the current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply run:

```
sbt repl/run
```

## Find out more

| Technical Docs              | Setup Guide           |
|-----------------------------|-----------------------|
| ![i1][techdocs-image]       | ![i2][setup-image]    |
| [Technical Docs][techdocs]  | [Setup Guide][setup]  |

## Copyright and license

Copyright 2018-2018 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[pubsub]: https://cloud.google.com/pubsub/
[sbt-native-packager]: https://github.com/sbt/sbt-native-packager
[common-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/scala-common-enrich

[license]: http://www.apache.org/licenses/LICENSE-2.0

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[techdocs]: https://github.com/snowplow/snowplow/wiki/Beam-Enrich
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-beam-enrich
