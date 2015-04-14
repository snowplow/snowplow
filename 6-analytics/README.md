# Analytics

![architecture] [architecture-image]

## Overview

There is a wealth of analysis you can perform on Snowplow data, either once it's in [Storage] [storage], or even in-stream. (For users of the real-time pipeline.)

This repository contains some example [recipes] [recipes]. For more information on analysing Snowplow data, please see the [Analytics Cookbook] [cookbook].

## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Analytics Cookbook] [cookbook] | [Setup Guide] [setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-6-analytics.png
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[setup]: https://github.com/snowplow/snowplow/wiki/getting-started-analysing-SnowPlow-data
[cookbook]: http://snowplowanalytics.com/analytics/index.html
[recipes]: https://github.com/snowplow/snowplow/tree/master/6-analytics/recipes

[looker]: http://looker.com/

[e1]: ./looker-analytics/
[e2]: ./postgres-analytics/
[e3]: ./redshift-analytics/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
