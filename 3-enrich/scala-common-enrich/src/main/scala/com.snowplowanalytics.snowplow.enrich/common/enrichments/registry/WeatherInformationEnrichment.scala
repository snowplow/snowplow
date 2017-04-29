 /*Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._


//Timestamp
import java.sql.Timestamp

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.SchemaKey
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.ScalazJson4sUtils

object WeatherInformationEnrichmentConfig extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "weather_information", "jsonschema", "1-0-0")

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[WeatherInformationEnrichment.type] ={
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        apiKey       <- ScalazJson4sUtils.extract[String](config, "parameters", "apiKey")
        latitute     <- ScalazJson4sUtils.extract[Double](config, "parameters", "latitute")
        longitute    <- ScalazJson4sUtils.extract[Double](config, "parameters", "longitute")
        timestamp    <- ScalazJson4sUtils.extract[Timestamp](config, "parameters", "timestamp")
        enrich        = WeatherInformationEnrichment(apiKey, latitute, longitute, timestamp)
      } yield enrich).toValidationNel
    })
  }
}


case class Information(

  temprature: Double,
  Condition: String)



case class WeatherInformationEnrichment(
  apiKey: String,
  latitute: Double,
  longitute: Double, 
  timestamp: Timestamp) extends Enrichment  {

  val version = new DefaultArtifactVersion("0.1.0")

// return type to be changes to : Validation[String, Information]
  def extractWeatherInformation() = {
  
  }
}
