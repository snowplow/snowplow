# Infobright storage for SnowPlow

## Introduction

[Infobright Community Edition] [ice] (ICE) is an open-source columnar database.
For all but the largest SnowPlow users, columnar databases such as Infobright
should be an attractive alternative to doing all of your analysis in Hive.

This folder contains scripts to setup ICE so that you can start loading
SnowPlow events into it.

## Setup guide

A guide to setting up ICE, including creating a database for SnowPlow and the SnowPlow events table, can be found on the **[Wiki]** [ice-setup].

## Contents

The contents of this folder are as follows:

* In this folder is this `README.md` and the `setup.sh` Bash script
* `sql` contains Infobright-compatible SQL scripts to setup your database and
  verify the setup

## Copyright and license

infobright-storage is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[ice]: http://www.infobright.org
[license]: http://www.apache.org/licenses/LICENSE-2.0
[ice-setup]: https://github.com/snowplow/snowplow/wiki/infobright-storage-setup