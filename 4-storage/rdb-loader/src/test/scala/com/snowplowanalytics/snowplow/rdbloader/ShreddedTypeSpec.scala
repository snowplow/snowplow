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

import org.scalacheck.Gen
import cats.{Id, ~>}
import cats.implicits._
import org.specs2.Specification
import org.specs2.ScalaCheck

// This project
import S3.Folder.{ coerce => s3 }
import S3.Key.{ coerce => s3key }
import ShreddedType._
import config.Semver

object ShreddedTypeSpec {

  /**
   * `Gen` instance for a vendor/name-like string
   */
  implicit val alphaNum: Gen[String] = for {
    n <- Gen.chooseNum(1, 5)
    d <- Gen.oneOf('_', '.', '-')
    s <- Gen.listOf(Gen.alphaNumChar)
      .map(_.mkString)
      .suchThat(_.nonEmpty)
    (a, b) = s.splitAt(n)
    r <- Gen.const(s"$a$d$b")
      .suchThat(x => !x.startsWith(d.toString))
      .suchThat(x => !x.endsWith(d.toString))
  } yield r

  implicit val subpath: Gen[String] = for {
    s <- Gen.listOf(Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(!_.isEmpty))
    path = s.mkString("/")
  } yield if (path.isEmpty) "" else path + "/"

  /**
   * Elements for shredded path
   */
  type ShreddedTypeElements = (String, String, String, String, Int, Int, Int)

  /**
   * Generator of `ShreddedTypeElements`
   * This generator doesn't guarantee that all elements are valid
   * (such as `name` without dots), it allows to test parse failures
   */
  val shreddedTypeElementsGen = for {
    subpath <- subpath
    vendor <- alphaNum
    name <- alphaNum
    format <- alphaNum
    model <- Gen.chooseNum(0, 10)
    revision <- Gen.chooseNum(0, 10)
    addition <- Gen.chooseNum(0, 10)
  } yield (subpath, vendor, name, format, model, revision, addition)


}


class ShreddedTypeSpec extends Specification with ScalaCheck { def is = s2"""
  Transform correct S3 path $e1
  Fail to transform path without valid vendor $e2
  Fail to transform path without file $e3
  Transform correct S3 path without prefix $e4
  Transform batch of paths $e5
  Omit atomic-events path $e6
  Transform correct S3 path for Shred job 1.6.0 format $e7
  Transform correct S3 path without root folder $e8
  Modern and legacy transformation always give same result $e9
  Fetch valid shredded types from S3 $e10
  """

  import ShreddedTypeSpec._

  def e1 = {
    val path = "cross-batch-test/shredded-archive/run=2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001"
    val expectedPrefix = S3.Folder.coerce("s3://rdb-test/cross-batch-test/shredded-archive/run=2017-04-27-14-39-42")
    val expected = ShreddedTypeInfo(expectedPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1)
    val key = S3.Key.coerce(s"s3://rdb-test/$path")

    val result = ShreddedType.transformPath(key, Semver(0,10,0))
    result must beRight(expected)
  }

  def e2 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/submit_form/jsonschema/1-0-0/part-00000-00001"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val result = ShreddedType.transformPath(key, Semver(0,10,0))
    result must beLeft
  }

  def e3 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val result = ShreddedType.transformPath(key, Semver(0,12,0))
    result must beLeft
  }

  def e4 = {
    val path = "com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val result = ShreddedType.transformPath(key, Semver(0,10,0))
    val expected = ShreddedTypeInfo(S3.Folder.coerce("s3://rdb-test"), "com.snowplowanalytics.snowplow", "submit_form", 1)
    result must beRight(expected)
  }

  def e5 = {
    def listInterpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(bucket, _) =>
            Stream(
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-1-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00001")
            )

          case LoaderA.KeyExists(key) =>
              true

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val paths = List(
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.acme/context/jsonschema/2-1-0/part-00000-00001",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.acme/context/jsonschema/2-2-0/random-file",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.acme/context/jsonschema/1-1-0/part-00000-00001",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0/part-00000-00001",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0/part-00000-00002",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-3/part-00000-00003"
    ).map(S3.Key.coerce)

    val commonPrefix = S3.Folder.coerce("s3://snowplow-events/shredded/run=2017-04-27-14-39-42")
    val expected = List(
      ShreddedType(commonPrefix, "com.acme", "context", 2,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.acme/context_2.json")),
      ShreddedType(commonPrefix, "com.acme", "context", 1,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.acme/context_1.json")),
      ShreddedType(commonPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")),
      ShreddedType(commonPrefix, "com.snowplowanalytics.snowplow", "geolocation_context", 1,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/geolocation_context_1.json"))
    ).map(Right.apply)

    val result = ShreddedType.getShreddedTypes(paths, Semver(0,10,0), "us-east-1", None)
    val endResult = result.foldMap(listInterpreter)

    endResult must beEqualTo(expected)
  }

  def e6 = {
    def listInterpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(bucket, _) =>
            Stream(
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-1-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00001")
            )

          case LoaderA.KeyExists(key) =>
            true

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val paths = List(
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/atomic-events/part-00001",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/atomic-events/part-00002",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/atomic-events/part-00003",
      "s3://snowplow-events/shredded/run=2017-04-27-14-39-42/com.acme/context/jsonschema/1-1-0/part-00000-00001"
    ).map(S3.Key.coerce)

    val commonPrefix = S3.Folder.coerce("s3://snowplow-events/shredded/run=2017-04-27-14-39-42")
    val expected = List(
      ShreddedType(commonPrefix, "com.acme", "context", 1,
      s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.acme/context_1.json"))
    ).map(Right.apply)

    val result = ShreddedType.getShreddedTypes(paths, Semver(0,10,0), "us-east-1", None).foldMap(listInterpreter)
    result must beEqualTo(expected)
  }

  def e7 = {
    val path = "vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00000-00001"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val result = ShreddedType.transformPath(key, Semver(0,13,0))
    val expected = ShreddedTypeInfo(S3.Folder.coerce("s3://rdb-test"), "com.snowplowanalytics.snowplow", "submit_form", 1)
    result must beRight(expected)
  }

  def e8 = {
    val path = "run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val expectedPrefix = S3.Folder.coerce("s3://rdb-test/run%3D2017-04-27-14-39-42")
    val expected = ShreddedTypeInfo(expectedPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1)
    val result = ShreddedType.transformPath(key, Semver(0,11,0))
    result must beRight(expected)
  }

  def e9 = {
    prop { (elements: ShreddedTypeElements) => elements match {
      case (subpath, vendor, name, format, model, revision, addition) =>
        val legacy = s"s3://some-bucket/$subpath$vendor/$name/$format/$model-$revision-$addition/part-1"
        val modern = s"s3://some-bucket/${subpath}vendor=$vendor/name=$name/format=$format/version=$model-$revision-$addition/part-2"
        val legacyResult = ShreddedType.transformPath(S3.Key.coerce(legacy), Semver(0,11,0))
        val modernResult = ShreddedType.transformPath(S3.Key.coerce(modern), Semver(0,12,0))
        val eitherMatch = legacyResult.void.leftMap(_ => ()) must beEqualTo(modernResult.void.leftMap(_ => ()))
        val valueMatch = (legacyResult, modernResult) match {
          case (l: Right[_, _], m: Right[_, _]) =>
            l.b must beEqualTo(m.b)
          case (Left(_), Left(_)) => ok
          case _ => ko
        }
        eitherMatch.and(valueMatch)

    } }.setGen(shreddedTypeElementsGen)
  }

  def e10 = {
    def listInterpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(bucket, _) =>
            Stream(
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-1-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00002"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00000"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00001")
            )

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

    val action = ShreddedType.discoverShreddedTypes(s3("s3://snowplow-events-acme.storage/"), Semver(0,11,0), "us-east-1", None)

    val result = action.foldMap(listInterpreter)

    val expected = List(
      Right(ShreddedType(
        s3("s3://snowplow-events-acme.storage/run=2017-05-22-12-20-57/"),
        "com.mailchimp","email_address_change",
        1,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json"))),
      Right(ShreddedType(
        s3("s3://snowplow-events-acme.storage/run=2017-05-22-12-20-57/"),
        "com.mailchimp",
        "email_address_change",
        2,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json"))),
      Right(ShreddedType(
        s3("s3://snowplow-events-acme.storage/run=2017-05-22-12-20-57/"),
        "com.snowplowanalytics.snowplow",
        "add_to_cart",
        1,
        s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/add_to_cart_1.json")
      ))
    )

    result must beEqualTo(expected)
  }
}
