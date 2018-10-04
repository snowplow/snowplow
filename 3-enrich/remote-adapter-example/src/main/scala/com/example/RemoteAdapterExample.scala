package com.example

import java.io._
import java.net.InetSocketAddress
import java.util.Base64

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

/**
 * This app is a simplified example of an Enrich Remote Adapter.
 *
 * It can also be used as-is to validate the External integration tests of the scala-common-enrich RemoteAdapterSpec test class:
 * if you set the externalEnrichmentUrl in that test class to Some("http://127.0.0.1:8995/myENrichment"),
 * then the External tests in that class should pass whenever this app is running.
 */
object RemoteAdapterExample extends App {
  val tcpPort  = 8995
  val pathName = "myEnrichment"

  val httpServer = localHttpServer(tcpPort, pathName)
  httpServer.start()

  val mockAdapter = new MockAdapter("igluResolver.conf")

  private def localHttpServer(tcpPort: Int, basePath: String): HttpServer = {
    val httpServer = HttpServer.create(new InetSocketAddress(tcpPort), 0)

    httpServer.createContext(
      s"/$basePath",
      new HttpHandler {
        def handle(exchange: HttpExchange): Unit = {

          val encodedBody      = getBodyAsString(exchange.getRequestBody)
          val deserializedBody = deserializeFromBase64(encodedBody)

          val response = mockAdapter.handle(deserializedBody)

          exchange.sendResponseHeaders(200, 0)
          exchange.getResponseBody.write(serializeToBase64(response).getBytes)
          exchange.getResponseBody.close()
        }
      }
    )
    httpServer
  }

  private def getBodyAsString(body: InputStream): String = {
    val s = new java.util.Scanner(body).useDelimiter("\\A")
    if (s.hasNext) s.next() else ""
  }

  def serializeToBase64(p: Object): String = {
    val baos = new ByteArrayOutputStream()
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(p)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  def deserializeFromBase64(s: String): Any = {
    val bytes = Base64.getDecoder.decode(s)
    val ois   = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val p     = ois.readObject()
    ois.close()
    p
  }

}
