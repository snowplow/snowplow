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

class ConfigSpec extends Specification { def is = s2"""
  Configuration parse specification

    Parse storage without folder, but with comment $e1
    Parse monitoring.snowplow with custom decoder $e2
    Parse monitoring with custom decoder $e3
    Parse enrich with custom decoder $e4
    Parse collectors with Cloudfront access format $e5
    Parse emr with custom decoder $e6
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
    import ConfigParser._

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
    import ConfigParser._

    val monitoringYaml =
      """
        |tags: {foo: bar} # Name-value pairs describing this job
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
    val expected = Config.Monitoring(Map("foo" -> "bar"), logging, snowplow)

    storage must beRight(expected)
  }

  def e4 = {
    import ConfigParser._

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
    import ConfigParser._

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
    import ConfigParser._

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


}
