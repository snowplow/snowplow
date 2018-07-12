/**
 * Copyright 2012-2018 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.snowplowanalytics.refererparser.scala

// Json
import io.circe._;
import io.circe.parser._;

// Cats Effect
import cats.syntax.either._

import scala.collection.mutable.HashMap
import scala.collection.JavaConverters
import scala.io.Source

import com.snowplowanalytics.refererparser.CorruptJsonException
import com.snowplowanalytics.refererparser.{RefererLookup => JRefererLookup}
import com.snowplowanalytics.refererparser.{Medium => JMedium}

protected case class RefererLookup(
  medium: JMedium,
  source: String,
  parameters: Option[List[String]]
)

object ParseReferersJson {
  private def parseDocument(rawJson: String): Either[CorruptJsonException, JsonObject] = {
    parse(rawJson) match {
      case Right(success) => success.asObject match {
        case Some(obj) => Right(obj)
        case None => Left(new CorruptJsonException("referers.json must be an object"))
      }
      case Left(failure) => Left(new CorruptJsonException("Unable to parse referers.json: " + failure.getMessage()))
    }
  }

  /**
   * Builds the map of hosts to referers from the
   * input JSON file.
   *
   * @param referersJson An InputStream containing the
   *                     referers database in JSON format.
   *
   * @return a Map where the key is the hostname of each
   *         referer and the value (RefererLookup)
   *         contains all known info about this referer
   */
  def parseReferersJson(rawJson: String): Either[CorruptJsonException, HashMap[String, RefererLookup]] = {
    val doc = parseDocument(rawJson) match {
      case Right(success) => success
      case Left(failure) => return Left(failure)
    }

    val referers: HashMap[String, RefererLookup] = new HashMap()

    for {
      (mediumName, referersJson) <- doc.toIterable
      medium = try {
        JMedium.fromString(mediumName)
      } catch {
        case e: IllegalArgumentException => return Left(new CorruptJsonException(s"Bad Medium $mediumName"))
      }
      referersObject = referersJson.asObject match {
        case Some(obj) => obj
        case None => return Left(new CorruptJsonException(s"Medium $mediumName is not an object"))
      }
      (sourceName, sourceJson) <- referersObject.toIterable
    } yield {
      val sourceObject = sourceJson.asObject match {
        case Some(obj) => obj
        case None => return Left(new CorruptJsonException(s"Source $sourceName is not an object"))
      }
      val parameters = sourceObject.toMap.get("parameters").map(_.asArray match {
        case Some(arr) => arr.map(_.asString match {
          case Some(str) => str
          case None => return Left(new CorruptJsonException(s"Parameter of $sourceName is not a string"))
        })
        case None => return Left(new CorruptJsonException(s"Parameters of $sourceName is not an array"))
      })
      val domains = sourceObject.toMap.get("domains").map(_.asArray) match {
        case Some(Some(arr)) => arr
        case _ => return Left(new CorruptJsonException(s"Domains of $sourceName is not an array"))
      }

      if (medium == JMedium.SEARCH && parameters.isEmpty) {
        return Left(new CorruptJsonException(s"No parameters found for search referer $sourceName"))
      } else if (medium != JMedium.SEARCH && !parameters.isEmpty) {
        return Left(new CorruptJsonException(s"Parameters not supported for non-search referer $sourceName"))
      }

      for (domainJson <- domains) {
        val domain = domainJson.asString match {
          case Some(str) => str
          case None => return Left(new CorruptJsonException(s"Domain of $sourceName is not a string"))
        }

        if (referers.contains(domain)) {
          //return Left(new CorruptJsonException(s"Duplicate of domain $domain found"))
        }

        referers.put(domain, new RefererLookup(medium, sourceName, parameters.map(_.toList)))
      }
    }

    Right(referers)
  }

  /**
   * Java-ifies the the result from parseReferersJson
   */
  @throws(classOf[CorruptJsonException])
  def loadReferers(referersStream: java.io.InputStream): java.util.Map[String, JRefererLookup] = {
    val rawJson = Source.fromInputStream(referersStream).mkString

    val map = parseReferersJson(rawJson) match {
      case Right(success) => success
      case Left(failure) => throw failure
    }

    // Convert case class to Java RefererLookup
    JavaConverters.mapAsJavaMapConverter(
      map.mapValues(r => new JRefererLookup(
        r.medium,
        r.source,
        r.parameters.map(
          l => JavaConverters.bufferAsJavaListConverter(l.toBuffer).asJava
        ).getOrElse(null)
      ))
    ).asJava
  }
}
