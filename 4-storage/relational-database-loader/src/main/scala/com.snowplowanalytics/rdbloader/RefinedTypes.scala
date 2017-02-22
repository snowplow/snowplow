package com.snowplowanalytics.rdbloader

import shapeless.tag, tag._

import cats.syntax.either._

import io.circe.Decoder

/**
  * Module with auxiliary types to add type-safety into configuration parsing
  */
object RefinedTypes {

  sealed trait S3BucketTag

  /**
    * Refined type for AWS S3 bucket, allowing only valid S3 paths
    */
  type S3Bucket = String @@ S3BucketTag

  object S3Bucket extends tag.Tagger[S3BucketTag] {
    def parse(s: String): Either[String, S3Bucket] = s match {
      case _ if !s.startsWith("s3://") => "Bucket name must start with s3://".asLeft
      case _ if s.length > 1024        => "Key length cannot be more than 1024 symbols".asLeft
      case _                           => apply(s).asRight
    }
  }

  implicit val bucketDecoder =
    Decoder.decodeString.emap(S3Bucket.parse)

}
