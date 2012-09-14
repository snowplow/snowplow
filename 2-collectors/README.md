# Collectors

![architecture] [architecture-image]

## Overview

A **Collector** receives SnowPlow events from one or more [Trackers]
[trackers]. A Collector captures and logs these events in their raw form, ready to be processed by the SnowPlow [ETL] [etl] phase.

## Available collectors

| Collector                       | Description                                           | Status           | Read more                    |
|---------------------------------|-------------------------------------------------------|------------------|------------------------------|
| [cloudfront-collector] [c1]     | An Amazon CloudFront-based collector. No moving parts | Production-ready | Wiki                         |
| [contrib-nodejs-collector] [c2] | SnowCannon is a node.js-based collector               | Beta             | [README] [snowcannon-readme] | 

## Contributing

We welcome community contributions of additional Collectors! We have a guide to contributing Collectors coming soon on the Wiki. 

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/2-collectors/2-collectors.png
[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[etl]: https://github.com/snowplow/snowplow/tree/master/3-etl
[snowcannon-readme]: https://github.com/shermozle/SnowCannon/blob/master/README.md
[c1]: ./2-collectors/cloudfront-collector/
[c2]: ./2-collectors/contrib-nodejs-collector/
