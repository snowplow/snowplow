package com.snowplowanalytics.snowplow.storage.kinesis.redshift.handler

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.AbstractHandler



/**
 * Created by denismo on 14/10/15.
 */
object JettyConfigHandler extends AbstractHandler with DripfeedConfig {

  val variableList = scala.collection.mutable.MutableList[ConfigVariable]()

  def addVariable(v: ConfigVariable) = {
    variableList += v
  }

  def getVariable(name: String) : Option[ConfigVariable] = {
    variableList.find(_.name == name)
  }

  private def readVariable(name: String) : String = {
    variableList.find(_.name == name) match {
      case Some(v) => v.get
      case None => null
    }
  }

  private def setVariable(name: String, newValue: String): Boolean = {
    variableList.find(_.name == name) match {
      case Some(v) =>
        v.set(newValue)
        true
      case None => false
    }
  }

  override def handle(path: String, request: Request, httpRequest: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setStatus(HttpServletResponse.SC_OK)
    // GET/PUT /dripfeed/v1/config/appid2schema
    // GET/PUT /dripfeed/v1/config/maxCollectionTime
    // GET/PUT /dripfeed/v1/config/collectionTime
    if (request.getPathInfo.startsWith("/dripfeed/v1/config")) {
      if (request.getPathInfo.startsWith("/dripfeed/v1/config/")) { // has variable
        val variableName = request.getPathInfo.substring("/dripfeed/v1/config/".length)
        if (request.getMethod == "GET") {
          response.setContentType("text/plain")
          val value: String = readVariable(variableName)
          if (value != null) {
            response.setStatus(HttpServletResponse.SC_OK)
            response.getWriter.println(value)
          } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          }
        } else if (request.getMethod == "PUT") {
          response.setContentType("text/plain")
          response.setStatus(if (setVariable(variableName, request.getParameter("value"))) HttpServletResponse.SC_OK else HttpServletResponse.SC_NOT_FOUND)
        }
      } else {
        response.setContentType("application/json")
        response.setStatus(HttpServletResponse.SC_OK)
        val writer = response.getWriter
        writer.println("{")
        variableList.zipWithIndex foreach { case (v, index) =>
          if (index < variableList.size-1) {
            writer.println("  \"" + v.name + "\": \"" + v.get + "\",")
          } else {
            writer.println("  \"" + v.name + "\": \"" + v.get + "\"")
          }
        }
        writer.println("}")
        writer.flush()
      }
    }
    request.setHandled(true)
  }
}