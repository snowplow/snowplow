# Trackers

![architecture] [architecture-image]

## Overview

**Trackers** are client- or server-side libraries which track customer behaviour by sending Snowplow events to a [Collector] [collectors].

## Available trackers

| Tracker                   | Description                                                    | Status           |
|---------------------------|----------------------------------------------------------------|------------------|
| [javascript-tracker] [t1] | A client-side JavaScript tracker for web browser use           | Production-ready |
| [no-js-tracker] [t2]      | A pixel-based tracker for no-JavaScript web environments       | Production-ready |
| [python-tracker] [t3]     | An event tracker for Python and Django webapps, apps and games | Production-ready |
| [ruby-tracker] [t4]       | An event tracker for Ruby and Rails apps and gems              | Beta             |
| [java-tracker] [t5]       | An event tracker for Java-based desktop and server apps, servlets and games | Beta |
| [arduino-tracker] [t6]    | An event tracker for IP-connected Arduino boards               | Production-ready |
| [lua-tracker] [t7]        | An event tracker for Lua apps, games and plugins               | Production-ready |
| [nodejs-tracker] [t8]     | An event tracker for node.js apps and servers                  | Beta             |

For other trackers (e.g. iOS, Android) and their approximate timelines, please see the [Product Roadmap][roadmap].

## Find out more

| Technical Docs               | Setup Guide           | Roadmap & Contributing               |         
|------------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]       | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [tech-docs] | [Setup Guide] [setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/1-trackers.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[t1]: https://github.com/snowplow/javascript-tracker
[t2]: ./no-js-tracker/
[t3]: https://github.com/snowplow/python-tracker
[t4]: https://github.com/snowplow/ruby-tracker
[t5]: https://github.com/snowplow/java-tracker
[t6]: https://github.com/snowplow/arduino-tracker
[t7]: https://github.com/snowplow/lua-tracker
[t8]: https://github.com/snowplow/snowplow-nodejs-tracker
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-a-Tracker
[tech-docs]: https://github.com/snowplow/snowplow/wiki/trackers
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
