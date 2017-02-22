/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.rdbloader

// cats
import cats.syntax.either._

// circe
import io.circe._
import io.circe.yaml.parser

// specs2
import org.specs2.Specification

// This project
import Config.Codecs._

class ConfigSpec extends Specification { def is = s2"""
  Configuration parse specification

    Parse storage without folder, but with comment, using auto decoder $e1
    Parse monitoring.snowplow with custom decoder $e2
    Parse monitoring with custom decoder $e3
    Parse enrich with custom decoder $e4
    Parse collectors with Cloudfront access format, using auto decoder $e5
    Parse emr with custom decoder $e6
    Parse s3 with custom decoder $e7
    Parse whole configuration using parse method $e8
    Get correct path in decoding failure $e9
  """

  def e1 = {
    import io.circe.generic.auto._

    val storageYaml =
      """
        |download:
        |  folder: # Postgres-only config option. Where to store the downloaded files. Leave blank for Redshift
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(storageYaml)
    val storage = ast.flatMap(_.as[Config.Storage])
    storage must beRight(Config.Storage(Config.Download(None)))
  }

  def e2 = {
    val monitoringYaml =
      """
        |method: get
        |app_id: ADD HERE # e.g. snowplow
        |collector: ADD HERE # e.g. d3rkrsqld9gmqf.cloudfront.net
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(monitoringYaml)
    val storage = ast.flatMap(_.as[Config.SnowplowMonitoring])
    storage must beRight(Config.SnowplowMonitoring(Config.GetMethod, "ADD HERE", "ADD HERE"))
  }

  def e3 = {
    val monitoringYaml =
      """
        |tags: {fromString: bar} # Name-value pairs describing this job
        |logging:
        |  level: DEBUG # You can optionally switch to INFO for production
        |snowplow:
        |  method: get
        |  app_id: ADD HERE # e.g. snowplow
        |  collector: ADD HERE # e.g. d3rkrsqld9gmqf.cloudfront.net
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(monitoringYaml)
    val storage = ast.flatMap(_.as[Config.Monitoring])

    val snowplow = Config.SnowplowMonitoring(Config.GetMethod, "ADD HERE", "ADD HERE")
    val logging = Config.Logging(Config.DebugLevel)
    val expected = Config.Monitoring(Map("fromString" -> "bar"), logging, snowplow)

    storage must beRight(expected)
  }

  def e4 = {
    val enrichYaml =
      """
        |job_name: Snowplow ETL # Give your job a name
        |versions:
        |  hadoop_enrich: 1.8.0 # Version of the Hadoop Enrichment process
        |  hadoop_shred: 0.10.0 # Version of the Hadoop Shredding process
        |  hadoop_elasticsearch: 0.1.0 # Version of the Hadoop to Elasticsearch copying process
        |continue_on_unexpected_error: false # Set to 'true' (and set :out_errors: above) if you don't want any exceptions thrown from ETL
        |output_compression: NONE # Compression only supported with Redshift, set to NONE if you have Postgres targets. Allowed formats: NONE, GZIP
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(enrichYaml)
    val storage = ast.flatMap(_.as[Config.Enrich])

    val versions = Config.EnrichVersions("1.8.0", "0.10.0", "0.1.0")
    val expected = Config.Enrich("Snowplow ETL", versions, false, Config.NoneCompression)

    storage must beRight(expected)
  }

  def e5 = {
    import io.circe.generic.auto._

    val collectorsYaml =
      """
        |format: tsv/com.amazon.aws.cloudfront/wd_access_log
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(collectorsYaml)
    val storage = ast.flatMap(_.as[Config.Collectors])

    val expected = Config.Collectors(Config.CfAccessLogFormat)

    storage must beRight(expected)
  }

  def e6 = {
    val emrYaml =
      """
        |ami_version: 4.5.0
        |region: ADD HERE        # Always set this
        |jobflow_role: EMR_EC2_DefaultRole # Created using $ aws emr create-default-roles
        |service_role: EMR_DefaultRole     # Created using $ aws emr create-default-roles
        |placement: ADD HERE     # Set this if not running in VPC. Leave blank otherwise
        |ec2_subnet_id: ADD HERE # Set this if running in VPC. Leave blank otherwise
        |ec2_key_name: ADD HERE
        |bootstrap: []           # Set this to specify custom boostrap actions. Leave empty otherwise
        |software:
        |  hbase:                # Optional. To launch on cluster, provide version, "0.92.0", keep quotes. Leave empty otherwise.
        |  lingual:              # Optional. To launch on cluster, provide version, "1.1", keep quotes. Leave empty otherwise.
        |# Adjust your Hadoop cluster below
        |jobflow:
        |  master_instance_type: m1.medium
        |  core_instance_count: 2
        |  core_instance_type: m1.medium
        |  task_instance_count: 0 # Increase to use spot instances
        |  task_instance_type: m1.medium
        |  task_instance_bid: 0.015 # In USD. Adjust bid, or leave blank for non-spot-priced (i.e. on-demand) task instances
        |bootstrap_failure_tries: 3 # Number of times to attempt the job in the event of bootstrap failures
        |additional_info: # Optional JSON string for selecting additional features
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(emrYaml)
    val emr = ast.flatMap(_.as[Config.SnowplowEmr])

    emr must beRight
  }

  def e7 = {
    val s3Yaml =
      """
        |region: ADD HERE
        |buckets:
        |  assets: s3://snowplow-hosted-assets # DO NOT CHANGE unless you are hosting the jarfiles etc yourself in your own bucket
        |  jsonpath_assets: s3://my-own-assets # If you have defined your own JSON Schemas, add the s3:// path to your own JSON Path files in your own bucket here
        |  log: s3://logs
        |  raw:
        |    in:                  # Multiple in buckets are permitted
        |      - s3://my-in-bucket
        |    processing: s3://processing
        |    archive: s3://archive          # e.g. s3://my-archive-bucket/raw
        |  enriched:
        |    good: s3://enriched-good       # e.g. s3://my-out-bucket/enriched/good
        |    bad: s3://enriched-bad         # e.g. s3://my-out-bucket/enriched/bad
        |    errors: s3://errors            # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: s3://path/to/arhcive  # Where to archive enriched events to, e.g. s3://my-archive-bucket/enriched
        |  shredded:
        |    good: s3://shredded       # e.g. s3://my-out-bucket/shredded/good
        |    bad: s3://shredded/bad    # e.g. s3://my-out-bucket/shredded/bad
        |    errors: s3://errors     # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: s3://archive   # Where to archive shredded events to, e.g. s3://my-archive-bucket/shredded
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(s3Yaml)
    val s3 = ast.flatMap(_.as[Config.SnowplowS3])

    s3 must beRight
  }

  def e8 = {
    val configYaml =
      """
        |aws:
        |  # Credentials can be hardcoded or set in environment variables
        |  access_key_id: <%= ENV['AWS_SNOWPLOW_ACCESS_KEY'] %>
        |  secret_access_key: <%= ENV['AWS_SNOWPLOW_SECRET_KEY'] %>
        |  s3:
        |    region: ADD HERE
        |    buckets:
        |      assets: s3://snowplow-hosted-assets # DO NOT CHANGE unless you are hosting the jarfiles etc yourself in your own bucket
        |      jsonpath_assets: # If you have defined your own JSON Schemas, add the s3:// path to your own JSON Path files in your own bucket here
        |      log: ADD HERE
        |      raw:
        |        in:                  # Multiple in buckets are permitted
        |          - s3://in-first/          # e.g. s3://my-in-bucket
        |          - s3://in-second/path/to/logs
        |        processing: s3://processing-logs/
        |        archive: s3://my-archive/    # e.g. s3://my-archive-bucket/raw
        |      enriched:
        |        good: s3://enriched-good/path/to/1       # e.g. s3://my-out-bucket/enriched/good
        |        bad: s3://snowplow-bad/PATH/to/1         # e.g. s3://my-out-bucket/enriched/bad
        |        errors:                    # Leave blank unless :continue_on_unexpected_error: set to true below
        |        archive: s3://enriched/    # Where to archive enriched events to, e.g. s3://my-archive-bucket/enriched
        |      shredded:
        |        good: s3://my-shredded-events/path/to/some/destination    # e.g. s3://my-out-bucket/shredded/good
        |        bad: s3://my_not_shredded-events/                         # e.g. s3://my-out-bucket/shredded/bad
        |        errors: s3://bucket/with/errors     # Leave blank unless :continue_on_unexpected_error: set to true below
        |        archive: s3://path                  # Where to archive shredded events to, e.g. s3://my-archive-bucket/shredded
        |  emr:
        |    ami_version: 4.5.0
        |    region: ADD HERE        # Always set this
        |    jobflow_role: EMR_EC2_DefaultRole # Created using $ aws emr create-default-roles
        |    service_role: EMR_DefaultRole     # Created using $ aws emr create-default-roles
        |    placement: ADD HERE     # Set this if not running in VPC. Leave blank otherwise
        |    ec2_subnet_id: ADD HERE # Set this if running in VPC. Leave blank otherwise
        |    ec2_key_name: ADD HERE
        |    bootstrap: []           # Set this to specify custom boostrap actions. Leave empty otherwise
        |    software:
        |      hbase:                # Optional. To launch on cluster, provide version, "0.92.0", keep quotes. Leave empty otherwise.
        |      lingual:              # Optional. To launch on cluster, provide version, "1.1", keep quotes. Leave empty otherwise.
        |    # Adjust your Hadoop cluster below
        |    jobflow:
        |      master_instance_type: m1.medium
        |      core_instance_count: 2
        |      core_instance_type: m1.medium
        |      task_instance_count: 0 # Increase to use spot instances
        |      task_instance_type: m1.medium
        |      task_instance_bid: 0.015 # In USD. Adjust bid, or leave blank for non-spot-priced (i.e. on-demand) task instances
        |    bootstrap_failure_tries: 3 # Number of times to attempt the job in the event of bootstrap failures
        |    additional_info:        # Optional JSON string for selecting additional features
        |collectors:
        |  format: cloudfront # For example: 'clj-tomcat' for the Clojure Collector, 'thrift' for Thrift records, 'tsv/com.amazon.aws.cloudfront/wd_access_log' for Cloudfront access logs or 'ndjson/urbanairship.connect/v1' for UrbanAirship Connect events
        |enrich:
        |  job_name: Snowplow ETL # Give your job a name
        |  versions:
        |    hadoop_enrich: 1.8.0 # Version of the Hadoop Enrichment process
        |    hadoop_shred: 0.10.0 # Version of the Hadoop Shredding process
        |    hadoop_elasticsearch: 0.1.0 # Version of the Hadoop to Elasticsearch copying process
        |  continue_on_unexpected_error: false # Set to 'true' (and set :out_errors: above) if you don't want any exceptions thrown from ETL
        |  output_compression: NONE # Compression only supported with Redshift, set to NONE if you have Postgres targets. Allowed formats: NONE, GZIP
        |storage:
        |  download:
        |    folder: # Postgres-only config option. Where to store the downloaded files. Leave blank for Redshift
        |monitoring:
        |  tags: {} # Name-value pairs describing this job
        |  logging:
        |    level: DEBUG # You can optionally switch to INFO for production
        |  snowplow:
        |    method: get
        |    app_id: ADD HERE # e.g. snowplow
        |    collector: ADD HERE # e.g. d3rkrsqld9gmqf.cloudfront.net
      """.stripMargin

    val result = Config.parse(configYaml)
    result must beRight
  }

  def e9 = {
    // buckets.enriched.good cannot be integer
    val s3Yaml =
      """
        |region: ADD HERE
        |buckets:
        |  assets: s3://snowplow-hosted-assets # DO NOT CHANGE unless you are hosting the jarfiles etc yourself in your own bucket
        |  jsonpath_assets: # If you have defined your own JSON Schemas, add the s3:// path to your own JSON Path files in your own bucket here
        |  log: ADD HERE
        |  raw:
        |    in:                  # Multiple in buckets are permitted
        |      - s3://in-first/          # e.g. s3://my-in-bucket
        |      - s3://in-second/path/to/logs
        |    processing: s3://processing-logs/
        |    archive: s3://my-archive/    # e.g. s3://my-archive-bucket/raw
        |  enriched:
        |    good: 0       # e.g. s3://my-out-bucket/enriched/good
        |    bad: ADD HERE        # e.g. s3://my-out-bucket/enriched/bad
        |    errors: ADD HERE     # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: ADD HERE    # Where to archive enriched events to, e.g. s3://my-archive-bucket/enriched
        |  shredded:
        |    good: ADD HERE       # e.g. s3://my-out-bucket/shredded/good
        |    bad: ADD HERE        # e.g. s3://my-out-bucket/shredded/bad
        |    errors: ADD HERE     # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: ADD HERE # Where to archive shredded events to, e.g. s3://my-archive-bucket/shredded
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(s3Yaml)
    val s3 = ast.flatMap(_.as[Config.SnowplowS3])

    val path = List(CursorOp.DownField("good"), CursorOp.DownField("enriched"), CursorOp.DownField("buckets"))
    s3.leftMap(_.asInstanceOf[DecodingFailure].history) must beLeft(path)
  }
}
