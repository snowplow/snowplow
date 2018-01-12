/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

// Scripting
import org.mozilla.javascript._

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Jackson
import org.codehaus.jackson.JsonParseException

// Scala
import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import outputs.EnrichedEvent
import utils.{
  ScalazJson4sUtils,
  ConversionUtils,
  JsonUtils => JU
}

/**
 * Lets us create a JavascriptScriptEnrichment from a JValue.
 */
object JavascriptScriptEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", 1, 0)

  /**
   * Creates a JavascriptScriptEnrichment instance from a JValue.
   *
   * @param config The JavaScript script enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured JavascriptScriptEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[JavascriptScriptEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        encoded  <- ScalazJson4sUtils.extract[String](config, "parameters", "script")
        raw      <- ConversionUtils.decodeBase64Url("script", encoded).toProcessingMessage // TODO: shouldn't be URL-safe
        compiled <- JavascriptScriptEnrichment.compile(raw).toProcessingMessage
        enrich    = JavascriptScriptEnrichment(compiled)
      } yield enrich).toValidationNel
    })
  }

}

/**
 * Companion object for working with JavaScript scripts.
 */
object JavascriptScriptEnrichment {

  object Variables {
    private val prefix = "$snowplow31337" // To avoid collisions
    val In  = s"${prefix}In"
    val Out = s"${prefix}Out"
  }

  /**
   * Appends an invocation to the script and
   * then attempts to compile it.
   *
   * @param script the JavaScript process()
   *        function as a String
   */
  private[registry] def compile(script: String): Validation[String, Script] = {

    // Script mustn't be null
    if (Option(script).isEmpty) {
      return "JavaScript script for evaluation is null".fail
    }

    val invoke =
      s"""|// User-supplied script
          |${script}
          |
          |// Immediately invoke using reserved args
          |var ${Variables.Out} = JSON.stringify(process(${Variables.In}));
          |
          |// Don't return anything
          |null;
          |""".stripMargin

    val cx = Context.enter()
    try {
      cx.compileString(invoke, "user-defined-script", 0, null).success
    } catch {
      case NonFatal(se) => s"Error compiling JavaScript script: [${se}]".fail
    }
  }

  /**
   * Run the process function as stored in the CompiledScript
   * against the supplied EnrichedEvent.
   *
   * @param script the JavaScript process()
   *        function as a CompiledScript
   * @param event The enriched event to
   *        pass into our process function
   * @return a Validation boxing either a
   *         JSON array of contexts on Success,
   *         or an error String on Failure
   */
  implicit val formats = DefaultFormats
  private[registry] def process(script: Script, event: EnrichedEvent): Validation[String, List[JObject]] = {

    val cx = Context.enter()
    val scope = cx.initStandardObjects

    try {
      scope.put(Variables.In, scope, Context.javaToJS(event, scope))
      val retVal = script.exec(cx, scope)
      if (Option(retVal).isDefined) {
        return s"Evaluated JavaScript script should not return a value; returned: [${retVal}]".fail
      }
    } catch {
      case NonFatal(nf) =>
        return s"Evaluating JavaScript script threw an exception: [${nf}]".fail
    } finally {
      Context.exit()
    }

    Option(scope.get(Variables.Out)) match {
      case None => Nil.success
      case Some(obj) => {
        try {
          JsonMethods.parse(obj.asInstanceOf[String]) match {
            case JArray(elements) => failFastCast(List[JObject](), elements).success
            case _ => s"JavaScript script must return an Array; got [${obj}]".fail
          }
        } catch {
          case NonFatal(nf) =>
            s"Could not convert object returned from JavaScript script to JValue AST: [${nf}]".fail
        }
      }
    }
  }

  /**
   * Åttempt to fail fast for our cast to List[].
   *
   * Taken from http://stackoverflow.com/a/6690611/255627
   */
  import scala.language.higherKinds
  private def failFastCast[A: Manifest, T[A] <: Traversable[A]](as: T[A], any: Any) = {
    val res = any.asInstanceOf[T[A]]
    if (res.isEmpty) res
    else {
      manifest[A].newArray(1).update(0, res.head) // force exception on wrong type
      res
    }
  }
}

/**
 * Config for an JavaScript script enrichment
 *
 * @param script The compiled script ready for
 */
case class JavascriptScriptEnrichment(
  script: Script
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * Run the process function as stored in the CompiledScript
   * against the supplied EnrichedEvent.
   *
   * @param event The enriched event to
   *        pass into our process function
   * @return a Validation boxing either a
   *         JSON array of contexts on Success,
   *         or an error String on Failure
   */
  def process(event: EnrichedEvent): Validation[String, List[JObject]] =
    JavascriptScriptEnrichment.process(script, event)

}
