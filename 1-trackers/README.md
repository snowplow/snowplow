# Trackers

![architecture] [architecture-image]

## Overview

**Trackers** are client- or server-side libraries which track customer behaviour by sending Snowplow events to a [Collector] [collectors].

## Available trackers

| Tracker                   | Description                                                    | Status           |
|---------------------------|----------------------------------------------------------------|------------------|
| [android-tracker] [t0]    | An event tracker for Android apps and games                    | Beta             |
| [arduino-tracker] [t1]    | An event tracker for IP-connected Arduino boards               | Production-ready |
| [ios-tracker] [t2]        | An event tracker for iOS apps and games                        | Beta             |
| [java-tracker] [t3]       | An event tracker for Java-based desktop and server apps, servlets and games | Beta |
| [javascript-tracker] [t4] | A client-side JavaScript tracker for web browser use           | Production-ready |
| [lua-tracker] [t5]        | An event tracker for Lua apps, games and plugins               | Production-ready |
| [no-js-tracker] [t6]      | A pixel-based tracker for no-JavaScript web environments       | Production-ready |
| [nodejs-tracker] [t7]     | An event tracker for node.js apps and servers                  | Beta             |
| [python-tracker] [t8]     | An event tracker for Python and Django webapps, apps and games | Production-ready |
| [ruby-tracker] [t9]       | An event tracker for Ruby and Rails apps and gems              | Beta             |

For other trackers (e.g. PHP, Go) and their approximate timelines, please see the [Product Roadmap][roadmap].

## Find out more

| Technical Docs               | Setup Guide           | Roadmap & Contributing               |         
|------------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]       | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [tech-docs] | [Setup Guide] [setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/1-trackers.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[t0]: https://github.com/snowplow/snowplow-android-tracker
[t1]: https://github.com/snowplow/snowplow-arduino-tracker
[t2]: https://github.com/snowplow/snowplow-ios-tracker
[t3]: https://github.com/snowplow/snowplow-java-tracker
[t4]: https://github.com/snowplow/snowplow-javascript-tracker
[t5]: https://github.com/snowplow/snowplow-lua-tracker
[t6]: ./no-js-tracker/
[t7]: https://github.com/snowplow/snowplow-nodejs-tracker
[t8]: https://github.com/snowplow/snowplow-python-tracker
[t9]: https://github.com/snowplow/snowplow-ruby-tracker
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-a-Tracker
[tech-docs]: https://github.com/snowplow/snowplow/wiki/trackers
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
