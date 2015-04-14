# Looker Metadata Model for Snowplow

## Introduction

Looker is a next-generation BI tool that is particularly well suited to analyse Snowplow data.

The metadata model included in this repo is a starting point to enable Snowplow users to get up and running quickly with Looker. It includes common dimensions and metrics that are likely to be useful to a broad range of Snowplow users e.g.

* New vs returning visitor
* Date / time a user first touched the website
* Number of transactions, events, page views and page pings per visit and per visitor

It should be straightforward for Snowplow users to build on this basic model to build e.g.:

* Business-specific audience segments
* Recognise business-specific events on a user journey and join them together to form business-specific funnels
* Develop business-specific metrics

## Contents

The contents of this folder are as follows:

* This `README.md` and the `LICENSE-2.0.txt` Apache license file
* `looker-metadata-model` contains the YAML metadata model files

## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| _coming soon_               | [Setup Guide] [setup] | _coming soon_                        |

### Other useful resources

* Blog post on [why Looker works so well with Snowplow] [looker-intro-post]
* Recipes for analytics using Looker *(coming soon)*

## Copyright and license

Looker Metadata Model for Snowplow is copyright 2012-2013 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[looker]: http://www.looker.com/
[looker-intro-post]: http://snowplowanalytics.com/blog/2013/12/10/introducing-looker-a-fresh-approach-to-bi-on-snowplow-data/
[license]: http://www.apache.org/licenses/LICENSE-2.0
[setup]: /snowplow/snowplow/wiki/Getting-started-with-Looker
[techdocs]: https://github.com/snowplow/snowplow/wiki/amazon-redshift-storage
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
