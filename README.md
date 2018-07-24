# referer-parser Java/Scala library

[![Build Status](https://travis-ci.org/snowplow-referer-parser/jvm-referer-parser.svg?branch=develop)](https://travis-ci.org/snowplow-referer-parser/jvm-referer-parser)
[![codecov](https://codecov.io/gh/snowplow-referer-parser/jvm-referer-parser/branch/master/graph/badge.svg)](https://codecov.io/gh/snowplow-referer-parser/jvm-referer-parser)

This is the Java and Scala implementation of [referer-parser][referer-parser], the library for extracting attribution data from referer _(sic)_ URLs.

The implementation uses a JSON version of the shared 'database' of known referers found in [`referers.yml`][referers-yml].

The Scala implementation is a core component of [Snowplow][snowplow], the open-source web-scale analytics platform.

### Usage

All effects within the Scala implementation are wrapped in `Sync` from [cats-effect][cats-effect]. In these examples we use `IO`, but anything that implements `Sync` can be used.

```scala
import com.snowplowanalytics.refererparser.Parser
import cats.effect.IO
import java.net.URI

val refererUrl = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
val pageUrl    = "http:/www.psychicbazaar.com/shop" // Our current URL

// We can instantiate a new Parser instance which will load referers.json
val parser = Parser.create[IO].unsafeRunSync()
val result = parser.parse(refererUrl, pageUrl)
result match {
  case Some(result) =>
    println(result.medium) // => "search"
    println(result.source) // => Some("Google")
    println(result.term)   // => Some("gateway oracle cards denise linn")
  case None =>
    println("Referer not in database")
}

// You can provide a list of domains which should be considered internal
Parser.parse(
    new URI("http://www.subdomain1.snowplowanalytics.com"),
    Some("http://www.snowplowanalytics.com"),
    List("www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com")
) match {
  case Some(result) =>
    println(result.medium) // => "internal"
    println(result.source) // => None
    println(result.term)   // => None
  case None =>
    println("Referer not in database")
}

// A custom referers.json can be passed in as a Source
val customParser = Parser.create[IO](
  Source.fromResource("custom-referers.json")
).unsafeRunSync()
```

### Installation

Add this to your SBT config:

```scala
val refererParser = "com.snowplowanalytics" %% "referer-parser" % "0.3.0"
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Copyright and license

The referer-parser Java/Scala library is copyright 2012-2018 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: https://github.com/snowplow/snowplow

[referer-parser]: https://github.com/snowplow-referer-parser/referer-parser
[referers-yml]: https://github.com/snowplow-referer-parser/referer-parser/blob/develop/resources/referers.yml

[cats-effect]: https://github.com/typelevel/cats-effect

[license]: http://www.apache.org/licenses/LICENSE-2.0
