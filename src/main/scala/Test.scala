import com.snowplowanalytics.snowplow.hive.serde.SnowPlowEventDeserializer

object Test {

  def main(args: Array[String]) {
    Console.println("Running tests...");
    SnowPlowEventDeserializer.deserializeLine("2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_pr=ERROR&r=236095&urlref=http%253A%252F%252Ftest.psybazaar.com%252F&_id=135f6b7536aff045&lang=en-US&visit=5&pdf=0&qt=1&realp=0&wma=1&dir=0&fla=1&java=1&gears=0&ag=0&res=1920x1080&cookie=1", true);
  }
}
