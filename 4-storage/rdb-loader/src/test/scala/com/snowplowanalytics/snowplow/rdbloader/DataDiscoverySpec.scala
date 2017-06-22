/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

import cats.{Id, ~>}

import org.specs2.Specification

import DataDiscovery._
import ShreddedType._
import S3.Key.{coerce => s3key}
import S3.Folder.{coerce => dir}
import config.Semver


class DataDiscoverySpec extends Specification { def is = s2"""
  Disover two run folders at once $e1
  """

  def e1 = {
    def listInterpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(bucket) =>
            Right(List(
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001"),

              S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/atomic-events/part-0000"),
              S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/atomic-events/part-0001"),
              S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00000"),
              S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00001")
            ))

          case LoaderA.KeyExists(key) =>
            if (key == "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json" ||
              key == "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json" ||
              key == "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/add_to_cart_1.json")
              true
            else
              false

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val shreddedGood = S3.Folder.coerce("s3://runfolder-test/shredded/good/")

    val expected = List(
      FullDiscovery(
        List(
          s3key("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/atomic-events/part-0000"),
          s3key("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/atomic-events/part-0001")),
        Map(
          ShreddedType(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",2,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json")) ->
              List(
                s3key("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001")),
          ShreddedType(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json")) ->
              List(
                s3key("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
                s3key("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"))
        )
      ),

      FullDiscovery(
        List(
          s3key("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/atomic-events/part-0000"),
          s3key("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/atomic-events/part-0001")),
        Map(
          ShreddedType(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/"), "com.snowplowanalytics.snowplow","add_to_cart",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/add_to_cart_1.json")) ->
              List(
                s3key("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00000"),
                s3key("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00001"))
        )
      )
    )

    val result = DataDiscovery.discoverFull(shreddedGood, Semver(0,11,0), "us-east-1", None)
    val endResult = result.foldMap(listInterpreter)

    endResult must beRight(expected)
  }
}
