/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.io.File
import java.net.URI
import java.net.URL
import java.net.URLClassLoader

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import iglu.client.validation.ProcessingMessageMethods._

// Snowplow referer-parser
import com.snowplowanalytics.refererparser.scala.{Parser => RefererParser}
import com.snowplowanalytics.refererparser.scala.Referer

// This project
import utils.{ConversionUtils => CU}
import utils.MapTransformer
import utils.MapTransformer._
import utils.ScalazJson4sUtils
import outputs.EnrichedEvent

/**
 * Companion object. Lets us create a
 * RefererParserEnrichment from a JValue
 */
object CustomEnrichment extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", 1, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[CustomEnrichment] = {
    isParseable(config, schemaKey).map( conf => {

      val setupObjects = for {
        JArray(cc) <- config \ "parameters" \ "classes"
        classConfiguration <- cc
        JString(qualifiedClassname) <- classConfiguration
        JString(classUrl) <- classConfiguration
        JBool(classEnabled) <- classConfiguration
      } yield UserEnrichmentSetup(qualifiedClassname, classUrl, classEnabled)

      CustomEnrichment(setupObjects)
    })
  }

}

case class UserEnrichmentSetup(
  qualifiedClassname: String,
  classUrl: String,
  classEnabled: Boolean)

case class CustomEnrichment(
  setups: List[UserEnrichmentSetup]
  ) extends Enrichment {

  def getFilesToCache: List[(URI, String)] = for {
    setup <- setups if setup.classEnabled
  } yield (new URI(setup.classUrl), setup.qualifiedClassname)

  lazy val instances: List[IUserEnrichment] = setups filter {_.classEnabled} map { setup =>
    val classFile = new File(setup.qualifiedClassname).toURI.toURL
    val classLoader = new URLClassLoader(Array(classFile))
    val klazz = classLoader.loadClass(setup.qualifiedClassname)
    klazz.newInstance.asInstanceOf[IUserEnrichment]
  }

  val version = new DefaultArtifactVersion("0.1.0")

  def performCustomEnrichments(input: EnrichedEvent) = instances.foreach(_.act(input))
}

trait IUserEnrichment {
  def act(input: EnrichedEvent)
}
