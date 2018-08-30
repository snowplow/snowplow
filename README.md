# referer-parser Scala library

[![Build Status](https://travis-ci.org/snowplow-referer-parser/scala-referer-parser.svg?branch=develop)](https://travis-ci.org/snowplow-referer-parser/scala-referer-parser)
[![Join the chat at https://gitter.im/snowplow-referer-parser/referer-parser](https://badges.gitter.im/snowplow-referer-parser/referer-parser.svg)](https://gitter.im/snowplow-referer-parser/referer-parser?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![codecov](https://codecov.io/gh/snowplow-referer-parser/scala-referer-parser/branch/develop/graph/badge.svg)](https://codecov.io/gh/snowplow-referer-parser/scala-referer-parser)

This is the Scala implementation of [referer-parser][referer-parser], the library for extracting attribution data from referer _(sic)_ URLs.

The implementation uses a JSON version of the shared 'database' of known referers found in [`referers.yml`][referers-yml].

The Scala implementation is a core component of [Snowplow][snowplow], the open-source web-scale analytics platform.

### Usage

All effects within the Scala implementation are wrapped in `Sync` from [cats-effect][cats-effect]. In these examples we use `IO`, but anything that implements `Sync` can be used.

```scala
import com.snowplowanalytics.refererparser.Parser
import cats.effect.IO
import cats.data.EitherT
import java.net.URI

val refererUrl = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
val pageUrl    = "http:/www.psychicbazaar.com/shop" // Our current URL

val referersJsonPath = "/opt/referers/referers.json"

// We use EitherT to handle exceptions. The IO routine will short circuit if an exception is returned.
val io: EitherT[IO, Exception, Unit] = for {
  // We can instantiate a new Parse instance with Parse.create
  parser <- EitherT(Parser.create[IO](referersJsonPath))

  // Referer is a sealed hierarchy of different referer types
  referer1 <- EitherT.fromOption[IO](parser.parse(refererUrl, pageUrl),
    new Exception("No parseable referer"))
  _ <- EitherT.right(IO { println(referer1) })
    // => SearchReferer(Google, Some(gateway oracle cards denise linn))

  // You can provide a list of domains which should be considered internal
  referer2 <- EitherT.fromOption[IO](parser.parse(
      new URI("http://www.subdomain1.snowplowanalytics.com"),
      Some("http://www.snowplowanalytics.com"),
      List("www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com")
    ), new Exception("No parseable referer"))
  _ <- EitherT.right(IO { println(referer2) })
    // => InternalReferer


  // Various overloads are available for common cases, for instance
  maybeReferer1 = parser.parse("https://www.bing.com/search?q=snowplow")
  maybeReferer2 = parser.parse(new URI("https://www.bing.com/search?q=snowplow"), None, Nil)
  _ <- EitherT.right(IO { println( maybeReferer1 == maybeReferer2 ) }) // => true
} yield Unit

io.value.unsafeRunSync()
```

More examples can be seen in [ParseTest.scala][parsetest-scala]. See [Parser.scala][parser-scala] for all overloads.

[parsetest-scala]: src/test/scala/com/snowplowanalytics/refererparser/ParseTest.scala
[parser-scala]: src/main/scala/com/snowplowanalytics/refererparser/Parser.scala

### Installation

Add this to your SBT config:

```scala
val refererParser = "com.snowplowanalytics" %% "referer-parser" % "0.4.0"
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
