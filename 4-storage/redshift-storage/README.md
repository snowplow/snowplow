# Amazon Redshift storage for SnowPlow

## Introduction

[Amazon Redshift][redshift] is a fully-managed, Petabyte scale datawarehouse
provided by Amazon. It is an excellent location for storing SnowPlow data, as it
enables you to plugin a wide variety of analytics tools directly onto SnowPlow data.

## Contents

The contents of this folder are as follows:

* In this folder is this `README.md` and the `LICENSE-20.txt` Apache license file
* `sql` contains Redshift-compatible SQL scripts to setup your database

## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

## Copyright and license

redshift-storage is copyright 2012-2013 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[redshift]: http://aws.amazon.com/redshift/
[license]: http://www.apache.org/licenses/LICENSE-2.0
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-redshift
[techdocs]: https://github.com/snowplow/snowplow/wiki/amazon-redshift-storage
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png