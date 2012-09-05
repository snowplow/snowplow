# Trackers

![architecture] [architecture-image]

## Overview

**Trackers** are client- or server-side libraries which track customer behaviour by sending SnowPlow events to a [Collector] [collectors].

## Available trackers

| Tracker                   | Description                                          | Status           | Read more |
|---------------------------|------------------------------------------------------|------------------|-----------|
| [javascript-tracker] [t1] | A client-side JavaScript tracker for web browser use | Production-ready | Wiki      |
| contrib-ios-tracker       | _Coming soon_ from a community member              | Pre-alpha        | -         | 

## Contributing

We welcome community contributions of additional trackers! We have a guide to contributing trackers coming soon on the Wiki. 

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/1-trackers/1-trackers.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[t1]: ./1-trackers/javascript-tracker/