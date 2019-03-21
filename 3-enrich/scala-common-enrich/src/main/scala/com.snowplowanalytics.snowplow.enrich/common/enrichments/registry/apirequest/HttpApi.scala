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
package enrichments.registry.apirequest

import java.net.URLEncoder

import cats.syntax.option._
import io.circe.syntax._

import utils.HttpClient

/**
 * API client able to make HTTP requests
 * @param method HTTP method
 * @param uri URI template
 * @param authentication auth preferences
 * @param timeout time in milliseconds after which request can be considered failed
 */
case class HttpApi(
  method: String,
  uri: String,
  timeout: Int,
  authentication: Authentication
) {
  import HttpApi._

  private val authUser = for {
    httpBasic <- authentication.httpBasic
    user <- httpBasic.username
  } yield user

  private val authPassword = for {
    httpBasic <- authentication.httpBasic
    password <- httpBasic.password
  } yield password

  /**
   * Primary API method, taking kv-context derived from event (POJO and contexts),
   * generating request and sending it
   * @param url URL to query
   * @param body optional request body
   * @return self-describing JSON ready to be attached to event contexts
   */
  def perform(url: String, body: Option[String] = None): Either[Throwable, String] = {
    val req =
      HttpClient.buildRequest(url, authUser = authUser, authPassword = authPassword, body, method, None, None)
    HttpClient.getBody(req)
  }

  /**
   * Build URL from URI templates (http://acme.com/{{key1}}/{{key2}}
   * Context values taken from event will be URL-encoded
   * @param context key-value context to substitute
   * @return Some request if everything is built correct,
   *         None if some placeholders weren't matched
   */
  private[apirequest] def buildUrl(context: Map[String, String]): Option[String] = {
    val encodedContext = context.map { case (k, v) => (k, URLEncoder.encode(v, "UTF-8")) }
    val url = encodedContext.toList.foldLeft(uri)(replace)
    if (everythingMatched(url)) url.some
    else none
  }

  /**
   * Build request data body when the method supports this
   * @param context key-value context
   * @return Some body data if method supports body, None if method does not support body
   */
  private[apirequest] def buildBody(context: Map[String, String]): Option[String] =
    method match {
      case "POST" | "PUT" => Some(context.asJson.noSpaces)
      case "GET" => None
    }
}

object HttpApi {

  /**
   * Check if URI still contain any braces (it's impossible for URL-encoded string)
   * @param uri URI generated out of template
   * @return true if uri contains no curly braces
   */
  private[apirequest] def everythingMatched(uri: String): Boolean =
    !(uri.contains('{') || uri.contains('}'))

  /**
   * Replace all keys (within curly braces) inside template `t` with corresponding value.
   * This function also double checks pair's key contains only allowed characters
   * (as specified in ALE config schema), otherwise regex can be injected
   * @param t string with placeholders
   * @param pair key-value pair
   * @return template with replaced placehoders for pair's key
   */
  private[apirequest] def replace(t: String, pair: (String, String)): String = pair match {
    case (key, value) =>
      if (!key.matches("^[a-zA-Z0-9_-]+$")) t
      else t.replaceAll(s"\\{\\{\\ *$key\\ *\\}\\}", value)
  }
}

/**
 * Helper class to configure authentication for HTTP API
 * @param httpBasic single possible auth type is http-basic
 */
case class Authentication(httpBasic: Option[HttpBasic])

/** Container for HTTP Basic auth credentials */
case class HttpBasic(username: Option[String], password: Option[String])
