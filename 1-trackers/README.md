# Trackers

![architecture] [architecture-image]

## Overview

**Trackers** are client- or server-side libraries which track customer behaviour by sending SnowPlow events to a [Collector] [collectors].

## Available trackers

| Tracker                   | Description                                          | Status           | Read more |
|---------------------------|------------------------------------------------------|------------------|-----------|
| [javascript-tracker] [t1] | A client-side JavaScript tracker for web browser use | Production-ready | Wiki      |
| contrib-ios-tracker       | Coming soon from a community member                  | Pre-alpha        | -         | 

## Useful resources

1. [Guide to choosing and setting up a tracker] [setup] on the [wiki] [wiki].
2. [Tracker technical documentation] [tech-docs], also on the [wiki] [wiki].

## Contributing

We welcome community contributions of additional Trackers! We have a guide to contributing Trackers coming soon on the Wiki. 

![Tracker](https://collector.snplow.com/i?&e=pv&page=1%20Trackers%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/1-trackers/1-trackers.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[t1]: ./1-trackers/javascript-tracker/
[setup]: https://github.com/snowplow/snowplow/wiki/choosing-a-tracker
[tech-docs]: https://github.com/snowplow/snowplow/wiki/trackers
[wiki]: https://github.com/snowplow/snowplow/wiki/trackers
