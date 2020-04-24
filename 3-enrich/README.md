# Enrich

![architecture][architecture-image]

## Overview

The **Enrich** process takes raw Snowplow events logged by a [Collector][collectors], cleans them up, enriches them and puts them into [Storage][storage].

## Available enrich

| ETL                             | Description                                                              | Status           |
|---------------------------------|--------------------------------------------------------------------------|------------------|
| [spark-enrich][e1] (1)  | The Snowplow Enrichment process built using Apache Spark   | Production-ready |
| [stream-enrich][e3] (3)        | The Snowplow Enrichment process built as an Amazon Kinesis application   | Production-ready |
| [scala-common-enrich][e4]      | A shared library for processing raw Snowplow events, used in (1) and (3) | Production-ready |
| [emr-etl-runner][e5]           | A Ruby app for running (1) and (2) on Amazon Elastic MapReduce           | Production-ready |

## How to add an enrichment?

5 things to update:
1. [iglu-central](https://github.com/snowplow/iglu-central): holds the JSON schema(s) required for the enrichments.
2. [scala-common-enrich](./scala-common-enrich/): library containing all the enrichments. This library is then used in enrichment jobs like [spark-enrich](./spark-enrich/), [stream-enrich](./stream-enrich/) or [beam-enrich](./beam-enrich/).
3. [spark-enrich](./spark-enrich/): holds the integration tests.
4. [config/](./config/enrichments/): holds one example of JSON configuration for each enrichment.
5. [Wiki](https://github.com/snowplow/snowplow/wiki/configurable-enrichments): contains documentation for each enrichment (how to configure it, the fields that it adds, etc).

### 1. Iglu

Files to create:
- If the new enrichment requires some configuration, JSON schema of this configuration. Examples can be found [here](https://github.com/snowplow/iglu-central/tree/master/schemas/com.snowplowanalytics.snowplow.enrichments/). `vendor`, `name`, `format`, `version` will be reused in `scala-common-enrich`, as well as the parameters' names when parsing the conf. 
- JSON schema of the the context added by the enrichment. An example can be found [here](https://github.com/snowplow/iglu-central/tree/master/schemas/com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0). `vendor`, `name`, `format`, `version` will be added to context data to create a self-describing JSON. The enrichment process will check that the context added by the enrichment is valid for this schema.
2 more files need to be generated for the context, with `igluctl static generate --with-json-paths <contextSchemaPath>` (`igluctl` can be found [here](https://docs.snowplowanalytics.com/open-source/iglu/igluctl/)): 
  - DDL ([examples](https://github.com/snowplow/iglu-central/tree/master/sql/)): used to create a table in Redshift to store the context of the enrichment.
  - JSON paths ([examples](https://github.com/snowplow/iglu-central/tree/master/jsonpaths/)): used to order the fields of the JSON in the same way that they are in the DDL (because a JSON is not ordered).

### 2. scala-common-enrich

#### 2.a. File with the new enrichment

This file should be created in [registry/](./scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/registry/).

It should contain 2 things:
1) Case class that extends `Enrichment` ([here](./scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/registry/enrichments.scala)) and that holds the logic of the enrichment.
This class has a function (e.g. `getContext`) that expects parameters from the raw event and returns the result of the enrichment, in a JSON holding the data as well as the name of the schema for these data ([self-describing JSON](https://snowplowanalytics.com/blog/2014/05/15/introducing-self-describing-jsons/)).
2) Companion object that extends `ParseableEnrichment` ([here](./scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/registry/enrichments.scala)) and has a function (e.g. `parse`) that can create an instance of the enrichment class from the configuration.

An example can be found [here](./scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/registry/YauaaEnrichment.scala).

The unit tests for this example can be found [here](./scala-common-enrich/src/test/scala/com.snowplowanalytics.snowplow.enrich.common/enrichments/registry/YauaaEnrichmentSpec.scala). The purpose of the unit tests is to make sure that:
- The functions used in the enrichment are working as expected.
- The enrichment can be correctly instanciated from the configuration.
- The self-describing JSON returned by the enrichment is correctly formatted and with the correct values.

#### 2.b. [EnrichmentRegistry](./scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/EnrichmentRegistry.scala)

This class instanciates the enrichments based on the configuration and holds a map `enrichment name -> enrichment instance`.

2 things to add:
1) In the method `buildEnrichmentConfig` of the companion object, add a case for the new enrichment and call the function previously created in the companion object of the enrichment.
```scala
  case "my_enrichment_config" =>
    MyEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
```
This instanciates the enrichment and puts it in the registry, if a configuration exists for the enrichment.

2) In `EnrichmentRegistry` case class, create function `getMyEnrichment`:
```scala
def getMyEnrichment: Option[MyEnrichment] =
  getEnrichment[MyEnrichment]("my_enrichment_config")
```
This function returns the instance of the enrichment from the registry.

`"my_enrichment_config"` should be the `name` field of the JSON schema for the configuration of the enrichment.

#### 2.c. [EnrichmentManager](./scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/EnrichmentManager.scala)

This is where the events are actually enriched, in the `enrichEvent` function.

3 things to do:
1) Get the result of the enrichment (if instanciated in the registry).
```scala
val myEnrichmentContext = registry.getMyEnrichment match {
  case Some(myEnrichment) =>
    myEnrichment
      .getContext(event.usergent, event.otherparams)
      .map(_.some)
  case None => None.success
}
```
2) Add the result (if any) to the derived contexts:
```scala
val preparedDerivedContexts =
  ...
  ++ List(myEnrichmentContext).collect {
    case Success(Some(context)) => context
  }
  ++ ...
```
3) Fail the event (will be sent to bad rows) if the enrichment didn't work:
```scala
val third =
  ...
  myEnrichmentContext.toValidationNel |@|
  ...
```

### 3. spark-enrich

The integration tests for the new enrichment need to be added in this project.
The purpose of these tests is to make sure that the results of the new enrichment are correctly added to the derived contexts of the enriched event.

An example can be found [here](./spark-enrich/src/test/scala/com.snowplowanalytics.snowplow.enrich.spark/good/YauaaEnrichmentCfLineSpec.scala).

The idea is to create lines as they would be written by a collector (e.g. [CloudFront](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#BasicDistributionFileFormat)),
and then to run an enrich job on these lines, with the new enrichment enabled.
It's then possible to check in the output of the job that the derived contexts of the enriched events contain the output of the new enrichment, with the correct values.

To enable the new enrichment in the test job, the function `getEnrichments`
in [EnrichJobSpec](./spark-enrich/src/test/scala/com.snowplowanalytics.snowplow.enrich.spark/EnrichJobSpec.scala)
needs to be updated with a new parameter saying if the enrichment should be enabled
and with the JSON configuration of the enrichment.

**What if the pull request on `iglu-central` with the schemas for the new enrichment has not been merged yet?**

It's still possible to use the new schemas in the integration tests.
When a new PR is created on `iglu-central`, a new registry is created at this address:
`http://iglucentral-dev.com.s3-website-us-east-1.amazonaws.com/<branch_of_PR>`,
with all the existing schemas and the new ones.

This registry needs to be added to the list of Iglu resolvers used in the tests.
In [EnrichJobSpec](./spark-enrich/src/test/scala/com.snowplowanalytics.snowplow.enrich.spark/EnrichJobSpec.scala) object,
add the registry in the value `igluCentralConfig`:
```scala
  |{
    |"name": "Iglu with MyEnrichment schemas",
    |"priority": 1,
    |"vendorPrefixes": [ "com.snowplowanalytics" ],
    |"connection": {
      |"http": {
        |"uri": "http://iglucentral-dev.com.s3-website-us-east-1.amazonaws.com/<branch_of_PR>"
      |}
    |}
  |}
```

### 4. config

Add example for the JSON configuration of the new enrichment.

### 5. Wiki

Add detailed page for the new enrichment.

--------------------------

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-3-enrichment.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[e1]: ./spark-enrich/
[e3]: ./stream-enrich/
[e4]: ./scala-common-enrich/
[e5]: ./emr-etl-runner/
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
[techdocs]: https://github.com/snowplow/snowplow/wiki/Enrichment
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
