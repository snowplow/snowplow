# Trackers

![architecture] [architecture-image]

## Overview

**Trackers** are client- or server-side libraries which track customer behaviour by sending Snowplow events to a [Collector] [collectors].

## Available trackers

| Tracker                   | Description                                          | Status               |
|---------------------------|------------------------------------------------------|----------------------|
| [javascript-tracker] [t1] | A client-side JavaScript tracker for web browser use | Production-ready     |
| [no-js-tracker] [t2]      | A pixel-based tracker for no-JavaScript web environments | Production-ready |
| [arduino-tracker] [t3]    | An event tracker for IP-connected Arduino boards     | Production-ready     |

For other trackers (e.g. iOS, Android) and their approximate timelines, please see the [Product Roadmap][roadmap].

## Find out more

| Technical Docs               | Setup Guide           | Roadmap & Contributing               |         
|------------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]       | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [tech-docs] | [Setup Guide] [setup] | _coming soon_                        |

![Tracker](https://collector.snplow.com/i?&e=pv&page=1%20Trackers%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/1-trackers.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[t1]: ./1-trackers/javascript-tracker/
[t2]: ./1-trackers/no-js-tracker/
[t3]: https://github.com/snowplow/snowplow-arduino-tracker
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-a-Tracker
[tech-docs]: https://github.com/snowplow/snowplow/wiki/trackers
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
