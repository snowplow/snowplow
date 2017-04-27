# PostgreSQL storage for SnowPlow

## Introduction

[PostgreSQL][postgres] ("Postgres") is an advanced open-source Object-Relational DBMS. Postgres is an excellent storage target for Snowplow data, supporting a variety of rich SQL querying techniques (e.g. [windowing][window-functions]) as well as connections to a variety of BI and dashboarding tools.

Hosted options for Postgres include [Heroku][heroku] and [Engine Yard][engine-yard], with a [full list of hosting providers][hosted-postgres] available from PostgreSQL themselves.

## Contents

The contents of this folder are as follows:

* In this folder is this `README.md` and the `LICENSE-20.txt` Apache license file
* `sql` contains Postgres-compatible SQL scripts to setup your database

## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

## Copyright and license

postgres-storage is copyright 2013 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[postgres]: http://www.postgresql.org/
[window-functions]: http://www.postgresql.org/docs/9.1/static/tutorial-window.html

[heroku]: https://postgres.heroku.com/
[engine-yard]: https://pages.engineyard.com/hosted-database.html
[hosted-postgres]: http://www.postgresql.org/support/professional_hosting/

[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-postgres
[techdocs]: https://github.com/snowplow/snowplow/wiki/postgres-storage

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[license]: http://www.apache.org/licenses/LICENSE-2.0