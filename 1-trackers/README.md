# Trackers

![architecture][architecture-image]

## Overview

**Trackers** are client- or server-side libraries which track customer behaviour by sending Snowplow events to a [Collector][collectors].

## Available trackers

| Tracker                           | Description                                                    | Status           |
|-----------------------------------|----------------------------------------------------------------|------------------|
| [actionscript3-tracker][as3]     | An event tracker for ActionScript 3.0 games, apps and widgets  | Beta             |
| [android-tracker][android]       | An event tracker for Android apps and games                    | Production-ready |
| [arduino-tracker][arduino]       | An event tracker for IP-connected Arduino boards               | Production-ready |
| [dotnet-tracker][dotnet]         | An event tracker for the .NET platform                         | Production-ready |
| [ios-tracker][ios]               | An event tracker for iOS apps and games                        | Production-ready |
| [java-tracker][java]             | An event tracker for Java-based desktop and server apps, servlets and games | Production-ready |
| [javascript-tracker][javascript] | A client-side JavaScript tracker for web browser use           | Production-ready |
| [lua-tracker][lua]               | An event tracker for Lua apps, games and plugins               | Production-ready |
| [no-js-tracker][no-js]           | A pixel-based tracker for no-JavaScript web environments       | Production-ready |
| [nodejs-tracker][nodejs]         | An event tracker for node.js apps and servers                  | Production-ready |
| [php-tracker][php]               | An event tracker for PHP apps and scripts                      | Production-ready |
| [python-tracker][python]         | An event tracker for Python and Django webapps, apps and games | Production-ready |
| [ruby-tracker][ruby]             | An event tracker for Ruby and Rails apps and gems              | Production-ready |
| [scala-tracker][scala]           | An event tracker for Scala, Akka and Play apps and servers     | Production-ready |
| [unity-tracker][unity]           | An event tracker for Unity games and apps                      | Beta             |

For other trackers (e.g. PHP, Go) and their approximate timelines, please see the [Product Roadmap][roadmap].

## Find out more

| Technical Docs               | Setup Guide           | Roadmap & Contributing               |         
|------------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]       | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][tech-docs] | [Setup Guide][setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-1a-trackers.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors

[as3]: https://github.com/snowplow/snowplow-actionscript3-tracker
[android]: https://github.com/snowplow/snowplow-android-tracker
[arduino]: https://github.com/snowplow/snowplow-arduino-tracker
[dotnet]: https://github.com/snowplow/snowplow-dotnet-tracker
[ios]: https://github.com/snowplow/snowplow-ios-tracker
[java]: https://github.com/snowplow/snowplow-java-tracker
[javascript]: https://github.com/snowplow/snowplow-javascript-tracker
[lua]: https://github.com/snowplow/snowplow-lua-tracker
[no-js]: ./no-js-tracker/
[nodejs]: https://github.com/snowplow/snowplow-nodejs-tracker
[php]: https://github.com/snowplow/snowplow-php-tracker
[python]: https://github.com/snowplow/snowplow-python-tracker
[ruby]: https://github.com/snowplow/snowplow-ruby-tracker
[scala]: https://github.com/snowplow/snowplow-scala-tracker
[unity]: https://github.com/snowplow/snowplow-unity-tracker
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-a-Tracker
[tech-docs]: https://github.com/snowplow/snowplow/wiki/trackers
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
