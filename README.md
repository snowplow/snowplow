# referer-parser Java/Scala library

This is the Java and Scala implementation of [referer-parser] [referer-parser], the library for extracting search marketing data from referer _(sic)_ URLs.

The implementation uses the shared 'database' of known search engine referers found in [`search.yml`] [search-yml].

## Installation: Java

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
  <groupId>org.snowplowanalytics</groupId>
  <artifactId>refererparser</artifactId>
  <version>0.0.1</version>
</dependency>
```

## Installation: Scala

Add this to your SBT config:

```scala
// Resolver
val snowplowRepo = "SnowPlow Repo" at "http://maven.snplow.com/releases/"

// Dependency
val scalaUtil = "com.snowplowanalytics"   % "refererparser"   % "0.0.1" .
```

## Usage: Java

Use referer-parser in Java like this:

```java
import com.snowplowanalytics.refererparser.Parser;

...

  String refererUrl = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari";

  Parser refererParser = new Parser();
  Referal r = refererParser.parse(refererUrl);

  System.out.println(r.referer.name);       // => "Google"
  System.out.println(r.search.parameter);   // => "q"    
  System.out.println(r.search.term);        // => "gateway oracle cards denise linn"
```

## Usage: Scala

Use referer-parser in Scala like this:

```ruby
val refererUrl = "http://www.googlex.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"

import com.snowplowanalytics.refererparser.scala.Parser
for (r <- Parser.parse(refererUrl)) {
  println(r.referer.name)      // => "Google"
  for (s <- r.search) {
    println(s.term)            // => "gateway oracle cards denise linn"
    println(s.parameter)       // => "q"    
  }
}
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Copyright and license

The referer-parser Java/Scala library is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[referer-parser]: https://github.com/snowplow/referer-parser
[search-yml]: https://github.com/snowplow/referer-parser/blob/master/search.yml

[license]: http://www.apache.org/licenses/LICENSE-2.0