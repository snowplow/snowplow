/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader

import scala.io.Source.fromInputStream

import S3.Folder.{ coerce => s3 }
import config.Semver
import config.Semver._
import config.SnowplowConfig
import config.SnowplowConfig._
import config.StorageTarget

object SpecHelpers {

  val configYmlStream = getClass.getResourceAsStream("/valid-config.yml.base64")
  val configYml = fromInputStream(configYmlStream).getLines.mkString("\n")

  val resolverStream = getClass.getResourceAsStream("/resolver.json.base64")
  val resolver =  fromInputStream(resolverStream).getLines.mkString("\n")

  val targetStream = getClass.getResourceAsStream("/valid-redshift.json.base64")
  val target = fromInputStream(targetStream).getLines.mkString("\n")

  // config.yml with invalid raw.in S3 path
  val invalidConfigYmlStream = getClass.getResourceAsStream("/invalid-config.yml.base64")
  val invalidConfigYml = fromInputStream(invalidConfigYmlStream).getLines.mkString("\n")

  // target config with string as maxError
  val invalidTargetStream = getClass.getResourceAsStream("/invalid-redshift.json.base64")
  val invalidTarget = fromInputStream(invalidTargetStream).getLines.mkString("\n")

  val validConfig =
    SnowplowConfig(
      SnowplowAws(
        SnowplowS3(
          "us-east-1",
          SnowplowBuckets(
            s3("s3://snowplow-acme-storage/"),
            None,
            s3("s3://snowplow-acme-storage/logs"),
            ShreddedBucket(
              s3("s3://snowplow-acme-storage/shredded/good/"),
              s3("s3://snowplow-acme-storage/shredded/bad/"),
              None,
              s3("s3://snowplow-acme-storage/shredded-archive/")))
        )
      ),
      Enrich(EnrichVersions(Semver(1,9,0,None)),NoneCompression),
      Storage(Download(None),StorageVersions(Semver(0,12,0,Some(ReleaseCandidate(4))),Semver(0,1,0,None))),
      Monitoring(Map(),Logging(DebugLevel),Some(SnowplowMonitoring(Some(GetMethod),Some("batch-pipeline"),Some("snplow.acme.com")))))

  val validTarget = StorageTarget.RedshiftConfig(
    None,
    "AWS Redshift enriched events storage",
    "angkor-wat-final.ccxvdpz01xnr.us-east-1.redshift.amazonaws.com",
    "snowplow",
    5439,
    StorageTarget.Disable,
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "admin",
    "Supersecret1",
    1,
    20000)


}
