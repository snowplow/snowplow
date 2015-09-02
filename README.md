# referer-parser Java/Scala library

This is the Java and Scala implementation of [referer-parser] [referer-parser], the library for extracting attribution data from referer _(sic)_ URLs.

The implementation uses the shared 'database' of known referers found in [`referers.yml`] [referers-yml].

The Scala implementation is a core component of [Snowplow] [snowplow], the open-source web-scale analytics platform powered by Hadoop, Hive and Redshift.

## Java

### Usage

Use referer-parser in Java like this:

```java
import com.snowplowanalytics.refererparser.Parser;

...

String refererUrl = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari";
String pageUrl    = "http:/www.psychicbazaar.com/shop" // Our current URL

Parser refererParser = new Parser();
Referer r = refererParser.parse(refererUrl, pageUrl);

System.out.println(r.medium);     // => "search"
System.out.println(r.source);     // => "Google"
System.out.println(r.term);       // => "gateway oracle cards denise linn"
```

### Installation

Add the following code into your `HOME/.m2/settings.xml` to be able to use this repository:

```xml
<settings>
  <profiles>
    <profile>
      <!-- ... -->
      <repositories>
        <repository>
          <id>com.snowplowanalytics</id>
          <name>SnowPlow Analytics</name>
          <url>http://maven.snplow.com/releases</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
    </profile>
  </profiles>
</settings>
```

Then add into your project's `pom.xml`:

```xml
<dependency>
    <groupId>com.snowplowanalytics</groupId>
    <artifactId>referer-parser_2.11</artifactId>
    <version>0.3.0</version>
</dependency>
```

## Scala

### Usage

Use referer-parser in Scala like this:

```scala
val refererUrl = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
val pageUrl    = "http:/www.psychicbazaar.com/shop" // Our current URL

import com.snowplowanalytics.refererparser.scala.Parser
for (r <- Parser.parse(refererUrl, pageUrl)) {
  println(r.medium)         // => "search"
  for (s <- r.source) {
    println(s)              // => "Google"
  }
  for (t <- r.term) {
    println(t)              // => "gateway oracle cards denise linn"
  }
}
```

You can also provide a list of domains which should be considered internal:

```scala
val refererUrl = "http://www.subdomain1.snowplowanalytics.com"
val pageUrl = "http://www.snowplowanalytics.com"
val internalDomains = List(
  "www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com"
)

import com.snowplowanalytics.refererparser.scala.Parser

for (r <- Parser.parse(refererUrl, pageUrl, internalDomains)) {
  println(r.medium)         // => "internal"
  for (s <- r.source) {
    println(s)              // => null
  }
  for (t <- r.term) {
    println(t)              // => null
  }
}
```

### Installation

Add this to your SBT config:

```scala
// Resolver
val snowplowRepo = "SnowPlow Repo" at "http://maven.snplow.com/releases/"

// Dependency
val refererParser = "com.snowplowanalytics" %% "referer-parser" % "0.3.0"
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Copyright and license

The referer-parser Java/Scala library is copyright 2012-2015 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: https://github.com/snowplow/snowplow

[referer-parser]: https://github.com/snowplow/referer-parser
[referers-yml]: https://github.com/snowplow/referer-parser/blob/master/referers.yml

[license]: http://www.apache.org/licenses/LICENSE-2.0
