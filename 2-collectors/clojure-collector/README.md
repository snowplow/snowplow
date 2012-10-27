# clojure-collector

## Introduction

clojure-collector is a SnowPlow event collector for [SnowPlow] [snowplow], written in Clojure.

There is one major benefit of using clojure-collector over SnowPlow's [CloudFront-based collector] [cloudfront-collector]:

clojure-collector allows the use of a third-party cookie, making user tracking across domains possible for all browsers except mobile Safari; this is something even the Google Analytics JavaScript-set cookie approach struggles with.

## How clojure-collector works

In a nutshell: clojure-collector receives events from the [SnowPlow JavaScript tracker] [snowplow-js], sets/updates a third-party user tracking cookie, and returns the pixel to the client.

In pseudocode terms:

	if (request contains an "sp" cookie) {
	    Record that cookie as the user identifier
	    Set that cookie with a now+1 year cookie expiry
	    Add the headers and payload to the output array
	} else {
	    Set the "sp" cookie with a now+1 year cookie expiry
	    Add the headers and payload to the output array
	}

Note that clojure-collector does not contain any logging functionality of its own (unlike Simon Rumble's [SnowCannon] [snowcannon]); instead, you are expected to run clojure-collector in a servlet container like [Tomcat][4] or [Jetty][5], with access logging (including response headers) enabled.

## Deployment and configuration

Section to come (should be eventually moved to Setup guide).

Before rolling up your own solution, take a look at the valve component that Tomcat supports. In particular, the aptly named RequestDumperValve will log all the request and response headers in Tomcat. To enable the valve, just go to your server.xml file in the /conf directory and uncomment this line:

<Valve className="org.apache.catalina.valves.RequestDumperValve"/>

Then restart your server and the headers will appear in your log file.

http://tomcat.apache.org/tomcat-5.0-doc/config/valve.html

http://tomcat.apache.org/tomcat-5.0-doc/config/valve.html#Request Dumper Valve

## Technical architecture

clojure-collector This is an example web application that uses [Ring][1] and
[Compojure][2]. It demonstrates basic routing and embedded resources.

To play around with this example project, you will first need
[Leiningen][3] installed.

Download the project dependencies with:

    lein deps

Now you can start a development web server with:

    lein ring server

Or you can compile the project into a war-file ready for deployment to
a servlet container like [Tomcat][4] or [Jetty][5]:

    lein ring uberwar

## Copyright and license

clojure-collector is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[1]: https://github.com/mmcgrana/ring
[2]: https://github.com/weavejester/compojure
[3]: https://github.com/technomancy/leiningen
[4]: http://tomcat.apache.org
[5]: http://jetty.codehaus.org/jetty

[snowplow]: http://snowplowanalytics.com
[cloudfront-collector]: https://github.com/snowplow/snowplow/tree/master/2-collectors/cloudfront-collector
[snowcannon]: https://github.com/shermozle/SnowCannon
[snowplow-js]: https://github.com/snowplow/snowplow/tree/master/1-trackers/javascript

[license]: http://www.apache.org/licenses/LICENSE-2.0