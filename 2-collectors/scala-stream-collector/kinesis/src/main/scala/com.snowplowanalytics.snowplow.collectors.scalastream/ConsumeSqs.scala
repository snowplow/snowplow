package com.snowplowanalytics.snowplow.collectors.scalastream

// class Foo {}
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

object ConsumeSqs extends App {

  val queueUrl = "http://localhost:4576/queue/good-events-queue"

  val sqs = AmazonSQSClientBuilder
    .standard()
    .withCredentials(new DefaultAWSCredentialsProviderChain())
    .withRegion("eu-central-1")
    .build

  while (true) {
    val msgs = sqs.receiveMessage(queueUrl).getMessages
    msgs.forEach { m =>
      sqs.deleteMessage(queueUrl, m.getReceiptHandle())
      val decoded = new String(java.util.Base64.getDecoder().decode(m.getBody()))
      println(s"msg (${m.getMessageId()}):  $decoded, (original-body: ${m.getBody()})")
    }
  }

}
