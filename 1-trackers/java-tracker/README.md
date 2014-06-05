# Java Analytics for Snowplow

## Overview

Add analytics to your Java programs and data pipelines with the [Snowplow] [snowplow] event tracker for [Java] [java].

## Current Build

Since production is still early and large changes are being made daily, I have not uploaded any jar files yet. Class files can be found in the out folder. At the moment the source files are set up as a package.

To get started, move into the src folder and follow the set up guide!

Note: The set up there is for the package "javaplow", this means you should leave all source files in the javaplow folder, and drag the folder into your project. 

### About the Tracker

All tracking methods have been tested successfully, but more bugs are discovered every day. If you find one, let me know by email or github.

###Ready to [view the documentation][documentation]?

Dont have the collector configured? [See Snowplow setup] [setup]

## Comments

- For a full list of available functions, look into the packages interfaces.
- Context is meant to be in JSON String format

    `String i = "{'Movie':'Shawshank Redemption', 'Time':'100 Minutes' }"`

## Find out more

| Technical Docs                  | Setup Guide               | Roadmap                 | Contributing                      |
|---------------------------------|---------------------------|-------------------------|-----------------------------------|
| ![i1] [techdocs-image]          | ![i2] [setup-image]       | ![i3] [roadmap-image]   | ![i4] [contributing-image]        |
| **[Technical Docs] [techdocs]** | **[Setup Guide] [setup]** | **[Roadmap] [roadmap]** | **[Contributing] [contributing]** |

## Copyright and license

The Snowplow Java Tracker is copyright 2013 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0] [license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[java]: http://www.java.com/en/

[snowplow]: http://snowplowanalytics.com

[dependencies]: https://drive.google.com/folderview?id=0B9v7AAtH8DSpWWZ1c3RUZjU3WlU&usp=sharing
[documentation]: https://gleasonk.github.io/Saggezza/JavaDoc/index.html

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow/snowplow/wiki/Snowplow-technical-documentation
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-Snowplow
[roadmap]: https://github.com/snowplow/snowplow/wiki
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

[license]: http://www.apache.org/licenses/LICENSE-2.0
