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
package loaders

import cats.{Id, ~>}
import cats.data.State

// specs2
import org.specs2.Specification

// This project
import S3.Folder.{coerce => s3}
import Common.SqlString.{unsafeCoerce => sql}
import LoaderError.JsonpathDiscoveryFailure
import config.Step
import config.SnowplowConfig._

class RedshiftLoaderSpec extends Specification { def is = s2"""
  Disover atomic events data $e1
  Disover atomic events data $e4
  """

  import SpecHelpers._


  def e1 = {
    val separator = "\t"
    val space = " "   // Because of missing COMPUPDATE

    val action = RedshiftLoader.discover(validConfig, validTarget, Set.empty)
    val result = action.foldMap(RedshiftLoaderSpec.findFirstInterpreter)

    val atomic =
      s"""
        |COPY atomic.events FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/atomic-events/'
        | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1'
        | DELIMITER '$separator' MAXERROR 1
        | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS$space
        | TIMEFORMAT 'auto' ACCEPTINVCHARS ;""".stripMargin

    val manifest =
      """
        |INSERT INTO atomic.manifest
        | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, 0 AS shredded_cardinality
        | FROM atomic.events
        | WHERE etl_tstamp IS NOT null
        | GROUP BY 1
        | ORDER BY etl_tstamp DESC
        | LIMIT 1;""".stripMargin

    val expected = RedshiftLoadStatements(sql(atomic),Nil,None,None,sql(manifest))

    result must beRight(expected)
  }

  def e4 = {
    val separator = "\t"

    val space = " "   // Because of missing COMPUPDATE

    val steps = Step.defaultSteps ++ Set(Step.Vacuum)
    val action = RedshiftLoader.discover(validConfig, validTarget, steps)
    val result: Either[LoaderError, RedshiftLoadStatements] = action.foldMap(RedshiftLoaderSpec.findFirstInterpreter2)

    val atomic = s"""
         |COPY atomic.events FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/atomic-events/'
         | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1'
         | DELIMITER '$separator' MAXERROR 1
         | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS$space
         | TIMEFORMAT 'auto' ACCEPTINVCHARS ;""".stripMargin

    val vacuum = List(
      sql("VACUUM SORT ONLY atomic.events;"),
      sql("VACUUM SORT ONLY atomic.com_snowplowanalytics_snowplow_submit_form_1;"))

    val analyze = List(
      sql("ANALYZE atomic.events;"),
      sql("ANALYZE atomic.com_snowplowanalytics_snowplow_submit_form_1;"))

    val shredded = List(sql("""
        |COPY atomic.com_snowplowanalytics_snowplow_submit_form_1 FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-'
        | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json'
        | REGION AS 'us-east-1'
        | MAXERROR 1 TRUNCATECOLUMNS TIMEFORMAT 'auto'
        | ACCEPTINVCHARS ;""".stripMargin))

    val manifest = """
        |INSERT INTO atomic.manifest
        | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, 1 AS shredded_cardinality
        | FROM atomic.events
        | WHERE etl_tstamp IS NOT null
        | GROUP BY 1
        | ORDER BY etl_tstamp DESC
        | LIMIT 1;""".stripMargin

    val expected = RedshiftLoadStatements(sql(atomic), shredded, Some(vacuum), Some(analyze), sql(manifest))

    result must beRight(expected)
  }
}

object RedshiftLoaderSpec {
  import LoaderA._

  type Requests[A] = State[List[String], A]

  def findFirstInterpreter2: LoaderA ~> Id = new (LoaderA ~> Id) {
    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(bucket, _) =>
          Stream(
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00001-dbb35260-7b12-494b-be87-e7a4b1f59906.txt"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00002-cba3a610-0b90-494b-be87-e7a4b1f59906.txt"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00003-fba35670-9b83-494b-be87-e7a4b1f59906.txt"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00004-fba3866a-8b90-494b-be87-e7a4b1fa9906.txt"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00005-aba3568f-7b96-494b-be87-e7a4b1fa9906.txt")
          )

        case KeyExists(k) =>
          if (k == "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json") {
            true
          } else false

        case action =>
          throw new RuntimeException(s"Unexpected Action [$action]")
      }
    }
  }

  def findFirstInterpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(bucket, _) =>
          Stream(
            S3.Key.coerce(bucket + "random-file"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57_$folder$"),
            S3.Key.coerce(bucket + "run=2017-0-22-12-20-57/atomic-events"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/random-file"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-events/part-01"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/_SUCCESS"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/$folder$"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-02")
          )

        case action =>
          throw new RuntimeException(s"Unexpected Action [$action]")
      }
    }
  }

  // Aggregate keyExists requests
  def aggregateInterpreter: LoaderA ~> Requests = new (LoaderA ~> Requests) {
    val appendRequests = (key: String, state: List[String]) => {
      val newState = key :: state
      (newState, false)
    }
    def apply[A](effect: LoaderA[A]): Requests[A] = {
      effect match {
        case KeyExists(key) => State(appendRequests(key, _))
        case action => throw new RuntimeException(s"Unknown action [$action]")
      }
    }
  }

  def listInterpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(bucket, _) =>
          Stream(
            // These "random-file" keys will fail process for discoverShreddedTypes
            S3.Key.coerce(bucket + "random-file"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57_$folder$"),
            S3.Key.coerce(bucket + "run=2017-0-22-12-20-57/atomic-events"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/random-file"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-events/part-01"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/_SUCCESS"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/$folder$"),
            S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-02")
          )
        case action => throw new RuntimeException(s"Unexpected Action [$action]")
      }
    }
  }
}
