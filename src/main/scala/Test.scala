import com.snowplowanalytics.snowplow.hive.serde.SnowPlowEventDeserializer

object Test {

  def main(args: Array[String]) {
    Console.println("Running tests...");
    SnowPlowEventDeserializer.runTest("2012-03-16	11:45:01	ARN1	3422	195.78.71.32	GET	detlpfvsg0d9v.cloudfront.net	/ice.png	200	http://delivery.ads-creativesyndicator.com/adserver/www/delivery/afr.php?zoneid=103&cb=INSERT_RANDOM_NUMBER_HERE&ct0=INSERT_CLICKURL_HERE	Mozilla/5.0%20(Windows%20NT%206.0)%20AppleWebKit/535.11%20(KHTML,%20like%20Gecko)%20Chrome/17.0.963.79%20Safari/535.11	&ad_ba=1884&ad_ca=547&ad_us=a1088f76c6931b0a26228dc3bde321d7&r=481413&urlref=http%253A%252F%252Fwww.fantasyfootballscout.co.uk%252F&_id=b41cf6859dccd8ce&_ref=http%253A%252F%252Fwww.fantasyfootballscout.co.uk%252F&pdf=1&qt=0&realp=0&wma=0&dir=1&fla=1&java=1&gears=0&ag=1&res=1920x1200&cookie=1");
  }
}
