# Collectors

![architecture] [architecture-image]

## Overview

A **Collector** receives Snowplow events from one or more [Trackers]
[trackers]. A Collector captures and logs these events in their raw form, ready to be processed by the Snowplow [Enrich] [enrich] phase.

## Available collectors

| Collector                       | Description                                           | Status           |
|---------------------------------|-------------------------------------------------------|------------------|
| [cloudfront-collector] [c1]     | An Amazon CloudFront-based collector. No moving parts | Production-ready |                         |
| [clojure-collector] [c2]     | A Clojure collector compatible with Amazon Elastic Beanstalk | Production-ready |
| [contrib-nodejs-collector] [c3] | SnowCannon is a node.js-based collector               | Alpha             |

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |


![Tracker](https://collector.snplow.com/i?&e=pv&page=2%20Collectors%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/2-collectors.png
[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich
[snowcannon-readme]: https://github.com/shermozle/SnowCannon/blob/master/README.md
[c1]: https://github.com/snowplow/snowplow/tree/master/2-collectors/cloudfront-collector
[c2]: https://github.com/snowplow/snowplow/tree/master/2-collectors/clojure-collector
[c3]: https://github.com/shermozle/SnowCannon
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-a-collector
[techdocs]: https://github.com/snowplow/snowplow/wiki/collectors
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
