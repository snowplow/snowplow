/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry

import scala.util.control.NonFatal

import cats.data.ValidatedNel
import cats.implicits._
import com.github.fge.jsonschema.core.report.ProcessingMessage
import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import org.mozilla.javascript._
import io.circe._
import io.circe.parser._

import outputs.EnrichedEvent
import utils.{CirceUtils, ConversionUtils}

/** Lets us create a JavascriptScriptEnrichment from a Json. */
object JavascriptScriptEnrichmentConfig extends ParseableEnrichment {
  val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow",
      "javascript_script_config",
      "jsonschema",
      1,
      0
    )

  /**
   * Creates a JavascriptScriptEnrichment instance from a JValue.
   * @param c The JavaScript script enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a configured JavascriptScriptEnrichment instance
   */
  def parse(
    c: Json,
    schemaKey: SchemaKey
  ): ValidatedNel[ProcessingMessage, JavascriptScriptEnrichment] =
    (for {
      _ <- isParseable(c, schemaKey)
      encoded <- CirceUtils.extract[String](c, "parameters", "script").toEither
      raw <- ConversionUtils.decodeBase64Url("script", encoded)
      compiled <- JavascriptScriptEnrichment.compile(raw)
    } yield JavascriptScriptEnrichment(compiled))
      .leftMap(_.toProcessingMessage)
      .toEitherNel
      .toValidated
}

object JavascriptScriptEnrichment {
  object Variables {
    private val prefix = "$snowplow31337" // To avoid collisions
    val In = s"${prefix}In"
    val Out = s"${prefix}Out"
  }

  /**
   * Appends an invocation to the script and then attempts to compile it.
   * @param script the JavaScript process() function as a String
   */
  private[registry] def compile(script: String): Either[String, Script] =
    Option(script) match {
      case Some(s) =>
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
        Either
          .catchNonFatal(cx.compileString(invoke, "user-defined-script", 0, null))
          .leftMap(e => s"Error compiling JavaScript script: [${e.getMessage}]")
      case None => "JavaScript script for evaluation is null".asLeft
    }

  /**
   * Run the process function as stored in the CompiledScript against the supplied EnrichedEvent.
   * @param script the JavaScript process() function as a CompiledScript
   * @param event The enriched event to pass into our process function
   * @return a Validation boxing either a JSON array of contexts, or an error String
   */
  private[registry] def process(
    script: Script,
    event: EnrichedEvent
  ): Either[String, List[Json]] = {
    val cx = Context.enter()
    val scope = cx.initStandardObjects

    try {
      scope.put(Variables.In, scope, Context.javaToJS(event, scope))
      val retVal = script.exec(cx, scope)
      if (Option(retVal).isDefined) {
        return s"Evaluated JavaScript script should not return a value; returned: [$retVal]".asLeft
      }
    } catch {
      case NonFatal(nf) =>
        return s"Evaluating JavaScript script threw an exception: [$nf]".asLeft
    } finally {
      Context.exit()
    }

    Option(scope.get(Variables.Out)) match {
      case None => Nil.asRight
      case Some(obj) =>
        parse(obj.asInstanceOf[String]) match {
          case Right(js) =>
            js.asArray match {
              case Some(array) => array.toList.asRight
              case None => s"JavaScript script must return an Array; got [$obj]".asLeft
            }
          case Left(e) =>
            ("Could not convert object returned from JavaScript script to Json: " +
              s"[${e.getMessage}]").asLeft
        }
    }
  }
}

/**
 * Config for an JavaScript script enrichment
 * @param script The compiled script ready for
 */
final case class JavascriptScriptEnrichment(script: Script) extends Enrichment {

  /**
   * Run the process function as stored in the CompiledScript against the supplied EnrichedEvent.
   * @param event The enriched event to pass into our process function
   * @return either a JSON array of contexts on Success, or an error String on Failure
   */
  def process(event: EnrichedEvent): Either[String, List[Json]] =
    JavascriptScriptEnrichment.process(script, event)

}
